package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strings"
import "strconv"

type Op struct {
  // Your definitions here.
}

type ShardKV struct {

  mu sync.Mutex
  tickMutex sync.Mutex

  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  configVals map[int]map[int]map[string]string
  configShards map[int][]int
  curConfig shardmaster.Config

  debug bool
  actionNum int
  uids map[int]bool
  groupConfig int

}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {

  if kv.debug { fmt.Printf("%d, %d wants lock for get\n", kv.me, kv.gid) }

  kv.mu.Lock()
  if kv.debug { fmt.Printf("%d, %d acquired lock for get\n", kv.me, kv.gid) }

  defer func() {
    kv.mu.Unlock()
    if kv.debug { fmt.Printf("%d, %d returned lock from get\n", kv.me, kv.gid) }
  }()


  behind := true
  for behind {

    // has the next action that I would propose already been decided?
    new, update := kv.px.Status(kv.actionNum + 1)

    if new {
      // TODO: opify this
      sendArgsString := strings.Split(update.(string), " ")

      if kv.debug { fmt.Printf("%d, %d updating with instruction: %s\n",kv.me, kv.gid,update) }

      for i :=0; i < len(sendArgsString); i++ {
        sendArgsString[i] = strings.Trim(sendArgsString[i],"&{[]}")
      }

      kv.actionNum += 1

      // FIXME: what if it is a get? need to record what to send as reply to a delayed get?

      if sendArgsString[0] == "Put" {

        sendArgs := &PutArgs{}
        sendArgs.Num,_ = strconv.Atoi(sendArgsString[1])
        sendArgs.Key = sendArgsString[2]
        sendArgs.Value = sendArgsString[3]
        var sendArgsReply PutReply

        if kv.debug { fmt.Printf("%d, %d applying update with instruction: %v; %v\n", kv.me, kv.gid, sendArgsString, sendArgs) }
        kv.applyPut(sendArgs, &sendArgsReply)

      }
    } else {
      behind = false
    }
  }

  myTurn := false
  var num int

  for ! myTurn {

    // Next paxos instance...
    num = kv.actionNum + 1

    // if kv.debug { fmt.Printf("%d starting paxos to get permission for %d, %v\n", kv.me, num, pxArgs) }
    args.Num = num

    // TODO: opify this
    kv.px.Start(num,fmt.Sprintf("Get %v",args))

    done := false

    for ! done { // === while paxos has not completed.
      decided, chosen := kv.px.Status(num)

      if ! decided {

        // TODO: use exponential backoff instead of static wait?
        time.Sleep(time.Millisecond*10)
        if kv.debug { fmt.Printf("%d waiting for paxos to get permission for %d, %v, %t, %v\n", kv.me, num, args, decided, chosen) }

      } else {
        done = true

        if kv.debug { fmt.Printf("%d done paxos to get permission for %d, %v\n",kv.me, num, args) }

        myTurn = (fmt.Sprintf("Get %v",args) == chosen)

        if kv.debug { fmt.Printf("chosen = %s, mine = Join %d %v, myTurn = %t\n",chosen, num, args, myTurn) }

        if ! myTurn {

          // if kv.debug { fmt.Printf("%d ending paxos to get permission for %d, %v, %t\n",kv.me, num, pxArgs, myTurn) }

          // TODO: make this part more modular.
          sendArgsString := strings.Split(chosen.(string), " ")

          for i :=0; i < len(sendArgsString); i++ {
            sendArgsString[i] = strings.Trim(sendArgsString[i],"&{[]}")
          }

          kv.actionNum += 1

          if sendArgsString[0] == "Put" {
            sendArgs := &PutArgs{}
            sendArgs.Num,_ = strconv.Atoi(sendArgsString[1])
            sendArgs.Key = sendArgsString[2]
            sendArgs.Value = sendArgsString[3]
            var sendArgsReply PutReply
            if kv.debug { fmt.Printf("%d, %d applying update with instruction: %v; %v\n",kv.me, kv.gid,sendArgsString,sendArgs) }
            kv.applyPut(sendArgs, &sendArgsReply)
          }

        }
      }
    }
  }

  // TODO: is this needed? try commenting out, see if still works.
  kv.actionNum += 1

  if kv.debug { fmt.Printf("get\n") }
  if kv.debug { fmt.Printf("%d, %d wants get %s, %v\n", kv.me, kv.gid, args.Key, kv.configVals[kv.curConfig.Num]) }

  kv.applyGet(args, reply)

  return nil
}


func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  if kv.debug { fmt.Printf("%d, %d wants lock for put\n", kv.me, kv.gid) }

  kv.mu.Lock()
  if kv.debug { fmt.Printf("%d, %d acquired lock for put\n", kv.me, kv.gid) }

  defer func() {
    kv.mu.Unlock()
    if kv.debug { fmt.Printf("%d, %d returned lock for put\n", kv.me, kv.gid) }
  }()

  if kv.debug { fmt.Printf("put\n") }

  behind := true
  for behind {
    new, update := kv.px.Status(kv.actionNum + 1)

    if new {
      sendArgsString := strings.Split(update.(string), " ")
      for i :=0; i < len(sendArgsString); i++ {
        sendArgsString[i] = strings.Trim(sendArgsString[i],"&{[]}")
      }

      kv.actionNum += 1

      if sendArgsString[0] == "Put" {
        sendArgs := &PutArgs{}
        sendArgs.Num,_ = strconv.Atoi(sendArgsString[1])
        sendArgs.Key = sendArgsString[2]
        sendArgs.Value = sendArgsString[3]
        sendArgs.Uid,_ = strconv.Atoi(sendArgsString[4])
        var sendArgsReply PutReply
        if kv.debug { fmt.Printf("%d, %d applying update with instruction: %v; %v\n",kv.me, kv.gid,sendArgsString,sendArgs) }
        kv.applyPut(sendArgs, &sendArgsReply)
      }
    } else {
      behind = false
    }
  }

  myTurn := false

  var num int
  for myTurn == false {
    num = kv.actionNum + 1

    // if kv.debug { fmt.Printf("%d starting paxos to get permission for %d, %v\n",kv.me, num, pxArgs) }
    args.Num = num
    kv.px.Start(num,fmt.Sprintf("Put %v",args))

    done := false
    for ! done {
      decided,chosen := kv.px.Status(num)

      if !(decided) {
        time.Sleep(time.Millisecond*100)
        if kv.debug { fmt.Printf("%d waiting for paxos to get permission for %d, %v, %t, %v\n",kv.me, num, args, decided, chosen) }
      } else {

        done = true
        if kv.debug { fmt.Printf("%d done paxos to get permission for %d, %v\n",kv.me, num, args) }

        myTurn = (fmt.Sprintf("Put %v",args) == chosen)
        if kv.debug { fmt.Printf("chosen = %s, mine = Join %d %v, myTurn = %t\n",chosen, num, args, myTurn) }

        if !myTurn {

          // if kv.debug { fmt.Printf("%d ending paxos to get permission for %d, %v, %t\n",kv.me, num, pxArgs, myTurn) }

          sendArgsString := strings.Split(chosen.(string), " ")
          for i :=0; i < len(sendArgsString); i++ {
            sendArgsString[i] = strings.Trim(sendArgsString[i],"&{[]}")
          }

          kv.actionNum += 1
          if sendArgsString[0] == "Put" {
            sendArgs := &PutArgs{}
            sendArgs.Num,_ = strconv.Atoi(sendArgsString[1])
            sendArgs.Key = sendArgsString[2]
            sendArgs.Value = sendArgsString[3]
            sendArgs.Uid,_ = strconv.Atoi(sendArgsString[4])
            var sendArgsReply PutReply
            if kv.debug { fmt.Printf("%d, %d applying update with instruction: %v; %v\n",kv.me, kv.gid,sendArgsString,sendArgs) }
            kv.applyPut(sendArgs, &sendArgsReply)
          }
        } else {
          kv.actionNum += 1
        }
      }
    }
  }

  kv.applyPut(args, reply)
  return nil
}

func (kv *ShardKV) applyGet(args *GetArgs, reply *GetReply) error {
  shard := key2shard(args.Key)

  // TODO: don't want to query the shardmaster on every op.  what happens when this is commented out?

  // instead of querying every time, interpret log up to point
//  config := kv.sm.Query(-1)

  config := kv.curConfig

  if kv.gid != config.Shards[shard]{
    reply.Err = ErrWrongGroup
    return nil
  }

  //reply.Value = kv.curVals[shard][args.Key]
  reply.Value = kv.configVals[kv.curConfig.Num][shard][args.Key]

  // FIXME: distinguish between empty string and no key.
  if kv.configVals[kv.curConfig.Num][shard][args.Key] == "" {
    reply.Err = ErrNoKey
    return nil
  } else {
    reply.Err = OK
  }

  if kv.debug { fmt.Printf("%d, %d get %s returning %s, %v\n", kv.me, kv.gid, args.Key, reply.Value, kv.configVals[kv.curConfig.Num][shard]) }

  return nil
}

func (kv *ShardKV) applyPut(args *PutArgs, reply *PutReply) error {
  shard := key2shard(args.Key)

  // TODO: don't query the shardmaster on every operation.
//  config := kv.sm.Query(-1)

  config := kv.curConfig

  if kv.uids[args.Uid]{
    reply.Err = OK
    return nil
  }
  kv.uids[args.Uid] = true

  if kv.gid != config.Shards[shard]{
    reply.Err = ErrWrongGroup
    return nil
  }

  reply.Err = OK
  if kv.configVals[kv.curConfig.Num][shard] == nil {
    kv.configVals[kv.curConfig.Num][shard] = make(map[string]string)
  }

  kv.configVals[kv.curConfig.Num][shard][args.Key] = args.Value

  if kv.debug { fmt.Printf("%d, %d put %s as %s, %v, shard %d\n", kv.me, kv.gid, args.Key, args.Value, kv.configVals[kv.curConfig.Num][shard], shard) }

  kv.px.Done(args.Num)
  return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {

  var vals map[int]map[string]string

  // if we've seen this configuration in the past
  if kv.curConfig.Num >= args.CurConfig - 1 {
    vals = kv.configVals[args.CurConfig - 1]
  } else {
    reply.Err = Wait
    if kv.debug { fmt.Printf("%d, %d rejecting getShard for %d, %d. got confignum %d, curconfig is %d, map is %v. Passing shard %d\n", kv.me, kv.gid, args.Me, args.GID, args.CurConfig, kv.curConfig.Num, vals, args.Shard) }
    return nil
  }

  if kv.debug { fmt.Printf("%d, %d running getShard for %d, %d. got confignum %d, curconfig is %d, map is %v. Passing shard %d\n", kv.me, kv.gid, args.Me, args.GID, args.CurConfig, kv.curConfig.Num, vals, args.Shard) }

  reply.Err = OK
  reply.Map = vals[args.Shard]

  return nil
}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//

func (kv *ShardKV) tick() {

  kv.tickMutex.Lock()
  if kv.debug { fmt.Printf("%d, %d started with tick\n", kv.me, kv.gid) }

  defer func() {
    kv.tickMutex.Unlock()
    if kv.debug { fmt.Printf("%d, %d done with tick\n", kv.me, kv.gid) }
  }()

  upToDate := false

  for ! upToDate {

    config := kv.sm.Query(kv.curConfig.Num + 1)
    if kv.curConfig.Num == config.Num {
      // invariant: once I process a configuration, I have recieved all shards.
      upToDate = true
      break
    }

    if kv.debug { fmt.Printf("%d, %d updating to config %d from %d\n", kv.me,kv.gid, config.Num, kv.curConfig.Num) }
    if kv.debug { fmt.Printf("%d, %d shards were %v at config %d, but now are %v at config %d\n",kv.me,kv.gid, kv.curConfig.Shards, kv.curConfig.Num, config.Shards, config.Num) }

    kv.cleanConfigVals()

    // create new version of the kvstore???
    kv.configShards[config.Num] = make([]int,0,shardmaster.NShards)
    kv.configVals[config.Num] = make(map[int]map[string]string)

    // shard, gid
    for s, g := range config.Shards {

      // should I have this shard in this configuration?
      if g == kv.gid {

        // I had this shard previously.
        if kv.curConfig.Shards[s] == kv.gid {

          // copy shard into appropriate kv map
          kv.configVals[config.Num][s] = kv.configVals[kv.curConfig.Num][s]

        } else {

          if kv.curConfig.Num == 0 {
            kv.configVals[config.Num][s] = make(map[string]string)
          }

          // oldGid <- the group that the shard previously belonged to.
          oldGid := kv.curConfig.Shards[s]
          servers, ok := kv.curConfig.Groups[oldGid]

          if ok {  // group actually exists

            updated := false

            for ! updated {

              for _, srv := range servers {

                args := &GetShardArgs{}
                args.Shard = s
                args.CurConfig = config.Num
                args.Me = kv.me
                args.GID = kv.gid
                var reply GetShardReply

                // if kv.debug { fmt.Printf("%d, %d getting shard %d from %d, %d\n", kv.me, kv.gid, s, gi, oldGid) }

                // TODO: why just retry once?
                // TODO: expand so that we we can retry arbitrary number of times.
                ok := call(srv, "ShardKV.GetShard", args, &reply)
                if !ok {
                  ok = call(srv, "ShardKV.GetShard", args, &reply)
                }

                if ok && reply.Err == OK{
                  kv.configVals[config.Num][s] = reply.Map
                  // if kv.debug { fmt.Printf("%d, %d got shard %d from %d, %d - %v\n", kv.me, kv.gid, s, gi, oldGid, reply.Map) }
                  updated = true
                  break
                }

              }

              if updated {
                break
              }

              // get shards from other members of my group
              // TODO: is this actually nescessary??
              for _, srv := range kv.curConfig.Groups[kv.gid] {  // for each member of my replication group

                args := &GetShardArgs{}
                args.Shard = s
                args.CurConfig = config.Num+1
                args.Me = kv.me
                args.GID = kv.gid
                var reply GetShardReply

                // if kv.debug { fmt.Printf("%d, %d getting shard %d from %d, %d\n", kv.me, kv.gid, s, gi, oldGid) }
                ok := call(srv, "ShardKV.GetShard", args, &reply)
                if !ok {
                  ok = call(srv, "ShardKV.GetShard", args, &reply)
                }

                if ok && reply.Err == OK {
                  kv.configVals[config.Num][s] = reply.Map
                  // if kv.debug { fmt.Printf("%d, %d got shard %d from %d, %d - %v\n", kv.me, kv.gid, s, gi, oldGid, reply.Map) }
                  updated = true
                  break
                }
              }

            /*
            TODO: what all is going on here...
              //kv.mu.Unlock()  // originally a chan
              if kv.debug { fmt.Printf("%d, %d waiting for shard %d config %d\n", kv.me, kv.gid, s, config.Num) }
              time.Sleep(time.Millisecond * 10)
              if kv.debug { fmt.Printf("%d, %d want lock to try again for shard %d config %d\n", kv.me, kv.gid, s, config.Num) }
              // will this block?
              //kv.mu.Unlock()  // originally a chan
              if kv.debug { fmt.Printf("%d, %d got lock to try again for shard %d config %d\n", kv.me, kv.gid, s, config.Num) }a
            */

            }  // haven't gotten update.

          }  // group exists in configuration (REDUNDANT?)

        }  // should have shard, but don't

      }  // should I have this shard?

    }  // iterate through shard map

    // if kv.debug { fmt.Printf("%d, %d my maps were %v, but now they are %v\n", kv.me,kv.gid, kv.lastVals, kv.curVals) }
    kv.curConfig = config
  }
}


func (kv *ShardKV) cleanConfigVals() {

  group := kv.curConfig.Num
  skip := false

  // poll all servers in replication group; try to figure out min configuration
  for _, srv := range kv.curConfig.Groups[kv.gid] {

    yourConfigArgs := &YourConfigArgs{}
    var yourConfigReply YourConfigReply

    // TODO: why just retry once?
    ok := call(srv, "ShardKV.YourConfig", yourConfigArgs, &yourConfigReply)
    if ! ok {
      ok = call(srv, "ShardKV.YourConfig", yourConfigArgs, &yourConfigReply)
    }

    if ok {
      if yourConfigReply.Num < group {
        group = yourConfigReply.Num
      }
    } else {
      skip = true
      break
    }
  }

  if !skip {
    kv.groupConfig = group
  }

  skip = false
  lowest := kv.groupConfig

  // for each replication group
  for _, srvs := range kv.curConfig.Groups {

    // for each server in replication group
    for _, srv := range srvs {

      groupConfigArgs := &GroupConfigArgs{}
      var groupConfigReply GroupConfigReply
      ok := call(srv, "ShardKV.GroupConfig", groupConfigArgs, &groupConfigReply)
      if !ok {
        ok = call(srv, "ShardKV.GroupConfig", groupConfigArgs, &groupConfigReply)
      }

      if ok && groupConfigReply.Num < lowest {
        lowest = groupConfigReply.Num
        break
      }

      // TODO: why return?
      if !ok {
        return
      }
    }
  }


  // clear out all sorted data for configs smaller than smalled config stored by group
  for c, _ := range kv.configVals{
    if c < lowest {
      delete(kv.configVals,c)
    }
  }

  for c, _ := range kv.configShards{
    if c < lowest {
      delete(kv.configShards,c)
    }
  }

}

func (kv *ShardKV) YourConfig(args *YourConfigArgs, reply *YourConfigReply) error {
  // TODO: should this be protected by mutex?
  reply.Num = kv.curConfig.Num
  return nil
}

func (kv *ShardKV) GroupConfig(args *GroupConfigArgs, reply *GroupConfigReply) error {
  // TODO: should this be protected by mutex?
  reply.Num = kv.groupConfig
  return nil
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// me is the index of this server in servers[].
//

func StartServer(gid int64, shardmasters []string, servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  kv.curConfig = kv.sm.Query(-1)
  kv.configVals = make(map[int]map[int]map[string]string)
  kv.configVals[kv.curConfig.Num] = make(map[int]map[string]string)
  kv.configShards = make(map[int][]int)
  kv.configShards[kv.curConfig.Num] = make([]int,0,shardmaster.NShards)
  kv.uids = make(map[int]bool)

  // Your initialization code here.

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
