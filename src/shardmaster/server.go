package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "sort"
import "time"
import "reflect"
import "math"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  log map[int]Op
  largestSeenInst int
  largestLogIdx int  // greatest log item that has been applied to the configs array
  testing bool
  configs []Config // indexed by config num
}

// sortable array of ints.  Used to sort operation log.
type InstList []int
func (l InstList) Swap (i, j int) {l[i], l[j] = l[j], l[i]}
func (l InstList) Len() int {return len(l)}
func (l InstList) Less(i, j int) bool {return l[i] > l[j]}

type GIDList []int64
func (l GIDList) Swap (i, j int) {l[i], l[j] = l[j], l[i]}
func (l GIDList) Len() int {return len(l)}
func (l GIDList) Less(i, j int) bool {return l[i] > l[j]}


type Op struct {
  Type string           // {'JOIN','LEAVE','MOVE','QUERY'}
  GID int64             // join, leave, move
  Servers []string      // join
  Shard int             // move
  Num int               // query
}

func (op Op) Equals (diffOp Op) bool {
  return reflect.DeepEqual(op, diffOp)
}

func (sm *ShardMaster) nextInstNum() int {
  sm.largestSeenInst++
  return sm.largestSeenInst
}

// TODO: are there problems with this code?
// reminders: this is protected by a lock!!!
func (sm *ShardMaster) insertIntoLog(op Op) bool {
  to := 10 * time.Millisecond
  for {
    instNum := sm.nextInstNum()
    sm.px.Start(instNum, op)
    for i := 0; i < 8; i++ {
      decided, decidedOp := sm.px.Status(instNum)
      if decided {
        sm.log[instNum] = decidedOp.(Op)
        if op.Equals(decidedOp.(Op)) {
          return true
        } else {
          break
        }
      } else {
        time.Sleep(to)
        if to < 10 * time.Second {
          to *= 2
        }
      }
    }
  }
  return false
}

// return how far you are from required load
// positive means you have more than allowed.
// zero means you're ok.
// negative means you need some more.

func (sm *ShardMaster) checkLoad(config Config, gid int64) int {
  // setup
  shardCnt := len(config.Shards)
  groupCnt := len(config.Groups)
  maxShards := int(math.Ceil(float64(shardCnt)/float64(groupCnt)))
  remainder := shardCnt % groupCnt

  // TODO: need to get GIDs in different order?
  // get gids in lexicographic order
  gids := sm.getGids(config)
  gidIdx := -1
  for idx, elem := range gids {
    if elem == gid {
      gidIdx = idx
      break
    }
  }

  allowed := maxShards
  if remainder > 0 {
    if gidIdx < remainder {
      allowed = maxShards
    } else {
      allowed = maxShards - 1
    }
  }

  load := 0
  for _, elem := range config.Shards {
    if elem == gid {
      load += 1
    }
  }

  return load - allowed
}

/*
  Give all 0'd shards a new home.
  Given all 0'd replica groups some shards.
  Rebalance further (if necessary)
*/

func (sm *ShardMaster) latestConfig() Config {
  return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) setLatestConfig(conf Config) {
  sm.configs[len(sm.configs)-1] = conf
}

func (sm *ShardMaster) rebalance(config Config) Config {
  gids := sm.getGids(config)
  idx := 0
  status := make([]int, len(gids))

  for {
    // are we there yet?
    numOk := 0
    for _,stat := range status {
      numOk += stat
    }
    if numOk == len(gids) {
      break
    }

    grpKey := gids[idx]
    loadDiff := sm.checkLoad(config, grpKey)

    if loadDiff > 0 {
      // revoke shard ownership
      numZeroed := 0
      for idx, elem := range config.Shards {
        if numZeroed == loadDiff {
          break
        } else if elem == grpKey {
          config.Shards[idx] = 0
          numZeroed++
        }
      }
    } else if loadDiff < 0 {
      // transfer shards
      numTransferred := 0
      for idx, elem := range config.Shards {
        if numTransferred == -loadDiff {
          break
        } else if elem == 0 {
          config.Shards[idx] = grpKey
          numTransferred++
        }
      }
    } else {
      status[idx] = 1  //DONE!
    }
    idx = (idx + 1) % len(gids)
  }
  return config
}


// the actual configuration changes happen here.

func (sm *ShardMaster) applyJoin(op Op) {
  sm.extendConfigs()
  newConfig := sm.latestConfig()
  // register new group
  newConfig.Groups[op.GID] = op.Servers
  sm.setLatestConfig(sm.rebalance(newConfig))
}

func (sm *ShardMaster) applyLeave(op Op) {
  sm.extendConfigs()
  newConfig := sm.latestConfig()
  delete(newConfig.Groups, op.GID)
  // revoke ownership of shards
  for idx, elem := range newConfig.Shards {
    if elem == op.GID {
      newConfig.Shards[idx] = 0
    }
  }
  sm.setLatestConfig(sm.rebalance(newConfig))
}

func (sm *ShardMaster) applyMove(op Op) {
  sm.extendConfigs()
  newConfig := sm.latestConfig()
  newConfig.Shards[op.Shard] = op.GID
  sm.setLatestConfig(newConfig)
}

func (sm *ShardMaster) interpretLog() {
  // discard old configs array
  // TODO: figure out a smarter way to do this.
  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  keyList := sm.getLogKeys()
  for instNum := range keyList {
    op, _ := sm.log[instNum]
    switch op.Type {
    case "JOIN":
      sm.applyJoin(op)
    case "LEAVE":
      sm.applyLeave(op)
    case "MOVE":
      sm.applyMove(op)
    }
  }
}

func (sm *ShardMaster) extendConfigs() {
  // save reference to old Configuration
  oldConfig := sm.configs[len(sm.configs)-1]

  // extend configs array by one
  newLen := len(sm.configs) + 1
  l := make([]Config, newLen, newLen)
  copy(l, sm.configs)
  sm.configs = l

  // copy old config into new.
  newConfig := Config{}
  newConfig.Num = oldConfig.Num + 1
  copy(newConfig.Shards[:], oldConfig.Shards[:])
  newConfig.Groups = map[int64][]string{}
  for k, v := range oldConfig.Groups {
    newConfig.Groups[k] = v
  }

  // okay, now we have a new config to work with.
  sm.configs[len(sm.configs)-1] = newConfig
}

func (sm *ShardMaster) getLogKeys() []int {
  keyList := make([]int, len(sm.log))
  i := 0
  for k, _ := range sm.log {
    keyList[i] = k
    i++
  }
  sort.Sort(InstList(keyList))
  return keyList
}

// returns group IDs sorted in lexicographic order
func (sm *ShardMaster) getGids(config Config) []int64 {
  keyList := make([]int64, len(config.Groups))
  i := 0
  for k, _ := range config.Groups {
    keyList[i] = k
    i++
  }
  sort.Sort(GIDList(keyList))
  return keyList
}

func (sm *ShardMaster) printOpLog () {
  keyList := sm.getLogKeys()
  //fmt.Printf("Printing out operation log for node %d\n", sm.me)
  for instNum := range keyList {
    op, _ := sm.log[instNum]
    fmt.Println(op.Type)
  }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{}
  op.Type = "JOIN"
  op.GID = args.GID
  op.Servers = args.Servers
  completed := sm.insertIntoLog(op)
  if completed {
    return nil
  }
  return fmt.Errorf("Timeout")
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{}
  op.Type = "LEAVE"
  op.GID = args.GID
  completed := sm.insertIntoLog(op)
  if completed {
    return nil
  }
  return fmt.Errorf("Timeout")
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{}
  op.Type = "MOVE"
  op.GID = args.GID
  op.Shard = args.Shard
  completed := sm.insertIntoLog(op)
  if completed {
    return nil
  }
  return fmt.Errorf("Timeout")
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{}
  op.Type = "QUERY"
  op.Num = args.Num
  completed := sm.insertIntoLog(op)
  if completed {
    sm.interpretLog()
    if op.Num < 0 || op.Num >= len(sm.configs) {
      reply.Config = sm.latestConfig()
    } else {
      reply.Config = sm.configs[op.Num]
    }
    return nil
  }
  return fmt.Errorf("Timeout")
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  // paxos reconfiguration log
  sm.log = map[int]Op{}

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
