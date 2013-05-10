package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "sync"
import "os"
import "syscall"
import "math/rand"
import "reflect"
import "unsafe"
import "path"
import "encoding/gob"
import "strconv"

// Operation types
const (
  GetOp = iota
  PutOp
)

// max number of bytes allowed for a segment.
const SegLimit = 8 * 1024 * 1024

// replication level for cluster
const RepLevel = 3

// number of times to retry
const Retries = 3

// absolute path to where log segments should be stored
const SegPath = "/tmp/segment/"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string

  // TODO: reference to shardmaster
  //Config shardmaster.Config

  log *Log

  // pointers to PUTs live here.
  store map[string]*Op

  // backup buffers: map each server to a Segment.
  buffers map[string]*Segment

  // which segs am I responsible for?
  backedUpSegs map[string]map[int64]bool

  // primary's backup map
  backups map[int64]BackupGroup

  // have we seen these puts?
  request map[Request]bool

  // shim of live hosts
  hosts []string

}

// REQUEST

type Request struct {
  Client int64
  Request int64
}


// OPERATION

type Op struct {
//  Version VersionID
  Client int64
  Request int64
  Type int // {GetOp, PutOp}
  Key string
  Value string
}

func (op Op) size() int {
  size := int(unsafe.Sizeof(&op))  // convert from uintptr
  size += len(op.Key)
  size += len(op.Value)
  return size
}

func (op Op) Equals (diffOp Op) bool {
  return reflect.DeepEqual(op, diffOp)
}

// LOG

type Log struct {
  // TODO: only need to use a map if the int64s are non-sequential, right?
  Segments map[int64]*Segment
  CurrSegID int64
}

func (l *Log) getCurrSegment() (seg *Segment, ok bool) {
  s, o := l.Segments[l.CurrSegID]
  return s, o
}

func (l *Log) newSegment() *Segment {
  //prevSegment, _ := l.getCurrSegment()
  seg := new(Segment)
  seg.Size = 0
  seg.Active = true
  seg.ID = l.CurrSegID + 1

  // TODO: how do we make the new digest?
  l.Segments[seg.ID] = seg

  return seg
}

// LOG SEGMENTS

type Segment struct {
  ID int64
  Active bool
  Size int  //in bytes
  Digest []int64  //the ids of all preceding segments in log
  Ops []Op
}

func (s *Segment) append (op Op) bool {
  opSize := op.size()
  if s.Size + opSize > SegLimit {
    return false
  }
  s.Ops = append(s.Ops, op)
  s.Size += opSize
  return true
}

func (s *Segment) burp (toFile string) {
  fo, err := os.Create(toFile)
  if err != nil { panic(err) }
  defer fo.Close()
  enc := gob.NewEncoder(fo)
  err = enc.Encode(s)
  if err != nil {
    panic(err)
  }
  fo.Sync()
}

func (s *Segment) slurp (fromFile string) {
  fo, err := os.Open(fromFile)
  if err != nil { panic(err) }
  defer fo.Close()
  dec := gob.NewDecoder(fo)
  err = dec.Decode(s)
  if err != nil {
    panic(err)
  }
}

// BACKUP GROUP INFO
type BackupGroup struct {
  Backups  [RepLevel]string
  Liveness [RepLevel]bool
}


func (pb *PBServer) PullSegments(args *PullSegmentsArgs, reply *PullSegmentsReply) error {
  segments := make([]Segment, len(args.Segments))
  var wg sync.WaitGroup
  for i, segId := range args.Segments {
    wg.Add(1)
    go func(i int, segId int64) {
      segment := Segment{}
      fname := strconv.Itoa(int(segId))
      segment.slurp(path.Join(SegPath, fname))
      segments[i] = segment
      wg.Done()
    }(i, segId)
  }
  wg.Wait()
  fmt.Println("xfer", args)
  reply.Segments = segments
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  shard := key2shard(args.Key)
  // CHECK: am I the primary for this key
  if shard > 0 && false {
    reply.Err = ErrWrongServer
    return nil
  }

  op, ok := pb.store[args.Key]

  if ! ok {
    reply.Err = ErrNoKey
    return nil
  }

  reply.Value = op.Value
  reply.Err = OK
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  seg, _ := pb.log.getCurrSegment()
  group, ok := pb.backups[seg.ID]

  if ! ok {
    // pick some new replicas
  }

  // create operation
  putOp := new(Op)
  putOp.Client = args.Client
  putOp.Request = args.Request
  putOp.Type = PutOp
  putOp.Key = args.Key
  putOp.Value = args.Value

  shard := key2shard(putOp.Key)
  // CHECK: am I the primary for this key
  if shard > 0 && false {
    reply.Err = ErrWrongServer
    return nil
  }

  if seg.append(*putOp) == false {
    delete(pb.backups, seg.ID)
    if pb.broadcastFlush(seg.ID, group) {
      seg = pb.log.newSegment()
      seg.append(*putOp)
      // pick some new backups
    } else {
      panic("backup failure on flush")
    }
  }

  if pb.broadcastForward(*putOp, seg.ID, group) {
    pb.store[args.Key] = putOp
  } else {
    panic("backup failure on fwd")
  }

  reply.Err = OK
  return nil

}

func (pb *PBServer) checkPrimary(server string, segment int64, key string) Err {

  if key != "" {
    shard := key2shard(key)
    if shard > 0 && false {
      return ErrNotPrimary
    }
  }

  if segment != int64(0) {
    segs, _ := pb.backedUpSegs[server]
    resp, _ := segs[segment]
    if resp == false {
      return ErrNotResponsible
    }
  }

  return OK
}

// func (pb *PBServer) getReplicatedSegments(failedBackup string) ([]int64, []BackupGroup) {
//   // search through backups and return replication groups of which this server was member
// }


//TODO: fixme


func (pb *PBServer) enlistReplicas(segment Segment) bool {

  port := strconv.Itoa(SrvPort)

  hostsNeeded := RepLevel
  numHosts    := len(pb.hosts)
  numEnlisted := 0

  availHosts := map[string]bool{}
  enlisted   := map[string]bool{}

  for i := 0; i < numHosts; i++ {
    availHosts[pb.hosts[i]] = true
  }

  enlistArgs := new(EnlistReplicaArgs)
  enlistArgs.Origin = pb.me
  enlistArgs.Segment = segment

  for {

    idxs       := rand.Perm(len(availHosts))
    candidates := make([]string, numHosts - numEnlisted)

    // copy over available hosts
    i := 0
    for key, _ := range availHosts {
      candidates[i] = key
      i++
    }

    replicaIdxs := idxs[:hostsNeeded]

    var wg sync.WaitGroup
    replies   := make([]*EnlistReplicaReply, hostsNeeded)
    acks      := make([]bool, hostsNeeded)

    to := 10 * time.Millisecond

    // for each guy who hasn't acked
    for i := 0 ; i < hostsNeeded; i++ {
      host := candidates[replicaIdxs[i]-1]
      if (acks[i] == false) {
        wg.Add(1)
        go func(i int, backup string) {
          enlistReply := new(EnlistReplicaReply)
          enlistAck   := call(backup + ":" + port, "PBServer.EnlistReplica", enlistArgs, enlistReply)
          replies[i] = enlistReply
          acks[i]    = enlistAck
          wg.Done()
        }(i, host)
      }
    }
    wg.Wait()

    for idx, ack := range acks {
      host := candidates[replicaIdxs[idx]-1]
      if ack == false {
        // what the hell do we do here?
      } else {
        reply := replies[idx]
        if (reply.Err != OK) {
          // process error
        } else {
          hostsNeeded--
          enlisted[host] = true
        }
        delete(availHosts, host)
      }
    }

    if hostsNeeded != 0 {
      bg := new(BackupGroup)
      i := 0
      for k, _ := range enlisted {
        bg.Backups[i]  = k
        bg.Liveness[i] = true
        i++
      }
      return true
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }

  }

  return false

}


func (pb *PBServer) EnlistReplica(args *EnlistReplicaArgs, reply *EnlistReplicaReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  segs, ok := pb.backedUpSegs[args.Origin]
  if ! ok {
    segs = map[int64]bool{}
  }

  if segs[args.Segment.ID] == false {

    segs[args.Segment.ID] = true
    pb.backedUpSegs[args.Origin] = segs

  } else {
    panic("replica already enlisted")
  }

  reply.Err = OK
  return nil

}

func (pb *PBServer) FlushSeg(args *FlushSegArgs, reply *FlushSegReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  err := pb.checkPrimary(args.Origin, args.OldSegment, "")
  if err != OK {
    reply.Err = err
    return nil
  }

  seg, _ := pb.buffers[args.Origin]

  // write segment to disk in the background
  go func() {
    dirpath := path.Join(SegPath, args.Origin)
    os.Mkdir(dirpath, 0777)
    seg.burp(path.Join(dirpath, strconv.Itoa(int(seg.ID))))
  }()

  delete(pb.buffers, args.Origin)

  reply.Err = OK
  return nil
}

func (pb *PBServer) ForwardOp(args *ForwardOpArgs, reply *ForwardOpReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  err := pb.checkPrimary(args.Origin, args.Segment, args.Op.Key)
  if err != OK {
    reply.Err = err
    return nil
  }

  seg, _ := pb.buffers[args.Origin]
  res := seg.append(args.Op)

  if res == false {
    panic("buffer size exceeded in replica. should never happen.")
  }

  reply.Err = OK
  return nil
}


func (pb *PBServer) broadcastForward(op Op, segment int64, group BackupGroup) bool {

  port := strconv.Itoa(SrvPort)

  numOfBackups := len(group.Backups)

  replies := make([]*ForwardOpReply, numOfBackups)
  acks    := make([]bool, numOfBackups)

  var wg sync.WaitGroup

  // set the args
  fwdArgs  := new(ForwardOpArgs)
  fwdArgs.Origin = pb.me
  fwdArgs.Op = op
  fwdArgs.Segment = segment

  for i:= 0; i < Retries; i++ {

    to := 10 * time.Millisecond

    // for each guy who hasn't acked
    for idx, backup := range group.Backups {
      if (acks[idx] == false ) {
        wg.Add(1)
        go func(idx int, backup string) {
          fwdReply := new(ForwardOpReply)
          ack := call(backup + ":" + port, "PBServer.ForwardOp", fwdArgs, fwdReply)
          replies[idx] = fwdReply
          acks[idx]    = ack
          wg.Done()
        }(idx, backup)
      }
    }
    wg.Wait()

    numAcked := 0

    // process the responses.
    for idx, ack := range acks {
      if ack == false {
        // TODO: what the hell do we do here?
      } else {
        reply := replies[idx]
        if (reply.Err != OK) {
          return false
        } else {
          numAcked += 1
        }
      }
    }

    if numAcked == len(group.Backups) {
      return true
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }

  }

  //wasnt able to ack everyone
  return false
}


func (pb *PBServer) broadcastFlush(segment int64, group BackupGroup) bool {

  port := strconv.Itoa(SrvPort)

  replies := make([]*FlushSegReply, len(group.Backups))
  acks    := make([]bool, len(group.Backups))

  // set the args
  flshArgs  := new(FlushSegArgs)
  flshArgs.Origin = pb.me
  flshArgs.OldSegment = segment

  for i:= 0; i < Retries; i++ {

    to := 10 * time.Millisecond
    count := 0

    // for each guy who hasn't acked
    for idx, backup := range group.Backups {
      if (acks[idx] == false ) {
        count += 1
        go func(idx int, backup string) {
          flshReply := new(FlushSegReply)
          ack := call(backup + ":" + port, "PBServer.FlushSeg", flshArgs, flshReply)
          replies[idx] = flshReply
          acks[idx] = ack
        }(idx, backup)
      }
    }

    numAcked := 0

    // process the responses.
    for idx, ack := range acks {
      if ack == false {

      } else {
        reply := replies[idx]
        if (reply.Err != OK) {
          return false
        } else {
          numAcked += 1
        }
      }
    }

    if numAcked == len(group.Backups) {
      return true
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }

  //wasnt able to ack everyone
  return false
}



func (pb *PBServer) tick() {

}


// for performance testing. Isolate particular ops.
func (pb *PBServer) TestWriteSegment(args *TestWriteSegmentArgs, reply *TestWriteSegmentReply) error {
  var wg sync.WaitGroup
  os.Mkdir(SegPath, 0777)
  for i := 0; i < args.NumOfSegs; i++ {
    wg.Add(1)
    go func(i int) {
      segment := Segment{}
      segment.ID = int64(i)
      for {
        op := Op{}
        op.Client = int64(0)
        op.Request = int64(0)
        op.Type = PutOp
        op.Key = "foo foo foo foo foo foo foo foo foo foo"
        op.Value = "bar bar bar bar bar bar bar bar bar bar"
        if segment.append(op) == false {
          break
        }
      }
      fname := strconv.Itoa(i)
      segment.burp(path.Join(SegPath, fname))
      wg.Done()
    }(i)
  }
  wg.Wait()
  return nil
}

func (pb *PBServer) TestReadSegment(args *TestReadSegmentArgs, reply *TestReadSegmentReply) error {
  var wg sync.WaitGroup
  for i := 0; i < args.NumOfSegs; i++ {
    wg.Add(1)
    go func(i int) {
      segment := Segment{}
      fname := strconv.Itoa(i)
      segment.slurp(path.Join(SegPath, fname))
      wg.Done()
    }(i)
  }
  wg.Wait()
  return nil
}

func (pb *PBServer) TestPullSegments(args *TestPullSegmentsArgs, reply *TestPullSegmentsReply) error {
  var wg sync.WaitGroup
  port := strconv.Itoa(SrvPort)
  t1 := time.Now().UnixNano()

  for _, host := range args.Hosts {
    if host == "" {
      continue
    }
    for cnt:=0; cnt < 300; cnt++ {
      wg.Add(1)
      go func(host string) {
        sendargs  := new(PullSegmentsArgs)
        sendreply := new(PullSegmentsReply)
        sendargs.Segments = make([]int64, 1)
        sendargs.Segments[0] = int64(rand.Int63() % 30)
        ok := call(host + ":" + port, "PBServer.PullSegments", sendargs, sendreply)
        if ok {
          fmt.Println("segment from", host)
        }
        wg.Done()
      }(host)
    }
  }
  wg.Wait()
  t2 := time.Now().UnixNano()
  fmt.Println(t2-t1)
  return nil
}






// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}

func StartServer(me string) *PBServer {

  pb := new(PBServer)
  pb.me = me

  // initialize main data structures
  pb.log = new(Log)

  pb.store = map[string]*Op{}

  pb.buffers = map[string]*Segment{}

  pb.backedUpSegs = map[string]map[int64]bool{}

  pb.backups = map[int64]BackupGroup{}

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)

  port := strconv.Itoa(SrvPort)

  l, e := net.Listen("tcp", ":" + port);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {

        // deaf!
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()

        // mute!
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)

        // healthy!
        } else {
          go rpcs.ServeConn(conn)
        }

      } else if err == nil {
        conn.Close()
      }

      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(100 * time.Millisecond)
      //time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}


/*
type QuerySegmentsArgs struct {

	ShardsToRecover map[int] bool							--> set of shards to recover; all bools should be true, it's just a set

}

type QuerySegmentsReply struct {

	ServerName string
	ShardsToSegments map[int] (map[LogSegmentID] bool)		-->	map of shards to sets of corresponding segments

}
*/
// tell the viewserver which shards you have segments for and which segments you have
func (pb *PBServer) QuerySegments(args *QuerySegmentsArgs, reply *QuerySegmentsReply) error {

	reply.ServerName = pb.me

	return nil

}


/*
type ElectRecoveryMasterArgs struct {

	ShardsToSegmentsToServers map[int] (map[LogSegmentID] (map[string] bool))
															--> map of shards to recover, corresponding segments to retrieve, and set of servers containing each of those segments

}

type ElectRecoveryMasterReply struct {

}
*/
// you have been elected a recovery master
// recover the shards in ShardsToSegmentsToServers using the information in that data structure
func (pb *PBServer) ElectRecoveryMaster(args *ElectRecoveryMasterArgs, reply *ElectRecoveryMasterReply) error {

	reply.ServerName = pb.me
	
	

	return nil

}


// when done with recovery for a shard or group of shards (or all shards, it's flexible),
// announce it to the viewserver by calling ViewServer.RecoveryCompleted
//
/*
type RecoveryCompletedArgs struct {

	ServerName string
	ShardsRecovered []int

}

type RecoveryCompletedReply struct {

}
*/
