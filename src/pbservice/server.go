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
  buffers map[ServerID]*Segment

  // which segs am I responsible for?
  backedUpSegs map[ServerID]map[SegmentID]bool

  // primary's backup map
  backups map[SegmentID]BackupGroup

  // have we seen these puts?
  request map[Request]bool

  // shim of live hosts
  hosts []ServerID

}

// REQUEST

type Request struct {
  Client ClientID
  Request RequestID
}


// OPERATION

type Op struct {
//  Version VersionID
  Client ClientID
  Request RequestID
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
  // TODO: only need to use a map if the segmentIDs are non-sequential, right?
  Segments map[SegmentID]*Segment
  CurrSegID SegmentID
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
  ID SegmentID
  Active bool
  Size int  //in bytes
  Digest []SegmentID  //the ids of all preceding segments in log
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
  Backups [RepLevel]string
}


func (pb *PBServer) PullSegments(args *PullSegmentsArgs, reply *PullSegmentsReply) error {
  segments := make([]Segment, len(args.Segments))
  var wg sync.WaitGroup
  for i, segId := range args.Segments {
    wg.Add(1)
    go func(i int, segId SegmentID) {
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

  shard := key2Shard(op.Key)
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

  shard := key2Shard(op.Key)
  // CHECK: am I the primary for this key
  if shard > 0 && false {
    reply.Err = ErrWrongServer
    return nil
  }

  group, ok := pb.backups[seg.ID]

  // create operation
  putOp := new(Op)
  putOp.ClientID = args.ClientID
  putOp.RequestID = args.RequestID
  putOp.Type = PutOp
  putOp.Key = args.Key
  putOp.value = args.Value

  seg, _ := pb.log.getCurrSegment()

  if seg.append(putOp) == false {
    delete(pb.backups, seg.ID)
    if pb.broadcastFlush(seg.ID, group) {
      seg = l.newSegment()
      seg.append(putOp)
      // pick some new backups
    } else {
      panic("backup failure on flush")
    }
  }

  if pb.broadcastForward(*putOp, group) {
    pb.store[args.Key] = putOp
  } else {
    panic("backup failure on fwd")
  }

  reply.Err = OK
  return nil

}

func (pb *PBServer) checkPrimary(server ServerID, segment SegmentID, key string) Err {

  if key != "" {
    shard := key2Shard(op.Key)
    if shard > 0 && false {
      return ErrNotPrimary
    }
  }

  if segment != SegmentID(0) {
    segs, _ := pb.backedUpSegs[server]
    resp, _ := segs[segment]

    if resp == false {
      return ErrNotResponsible
    }
  }

  return OK
}

// func (pb *PBServer) getReplicatedSegments(failedBackup ServerID) ([]SegmentID, []BackupGroup) {
//   // search through backups and return replication groups of which this server was member
// }


//TODO: fixme

func (pb *PBServer) enlistReplicas() bool {

  port := strconv.Itoa(SrvPort)

  hostsNeeded := RepLevel
  numHosts    := len(pb.hosts)
  numEnlisted := 0

  availHosts := map[ServerID]bool{}
  enlisted   := map[ServerID]bool{}

  for i := 0; i < numHosts; i++ {
    availHosts[pb.hosts[i]] = true
  }

  enlistArgs := new(EnlistReplicaArgs)

  for {

    idxs       := rand.Perm(len(availHosts))
    candidates := make([]ServerID, numHosts - numEnlisted)

    i := 0
    for key, _ := range availHosts {
      candidates[i] = key
      i++
    }

    replicaIdxs := idxs[:hostsNeeded]

    replychan := make(chan data)
    ackchan   := make(chan data)

    replies   := make([]*EnlistReplicaReply, hostsNeeded)
    acks      := make([]bool, hostsNeeded)

    to := 10 * time.Millisecond
    count := 0

    // for each guy who hasn't acked
    for i := 0 ; i < hostsNeeded; i++ {

      host := candidates[replicaIdxs[i]-1]

      if (acks[idx] == false) {
        count += 1
        go func(replychan chan, ackchan chan, idx int, ServerID backup) {
          enlistReply := make(EnlistReplicaReply)
          enlistAck   := call(backup + ":" + port, 'PBServer.EnlistReplica', enlistArgs, enlistReplica)
          replychan <- data{idx, enlistReply}
          ackchan   <- data{idx, enlistAck}
        }(replychan, ackchan, idx, host)
      }

    }

    for i:= 0; i < count; i++ {
      reply := <- replychan
      replies[reply.i] = reply.val
      ack := <- ackchan
      acks[ack.i] = ack.val
    }

    for idx, ack := range acks {

      host := candidates[replicaIdxs[idx]-1]

      if ack == false {

      } else {
        reply := replies[idx]
        if (reply.Err) {

        } else {
          enlisted
        }
      }

      delete(availHosts, host)

    }

    if hostsNeeded != 0 {
      return true
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }

  }

  // select subset of live hosts
}


func (pb *PBServer) EnlistReplica(args *EnlistReplicaArgs, reply *EnlistReplicaReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  segs, ok := pb.backedUpSegs[args.Origin]
  if ! ok {
    segs = map[SegmentID]bool{}
  }

  if segs[args.SegmentID] == false {
    segs[args.SegmentID] = true
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
    dirpath := path.Join(SegPath, ServerID)
    os.Mkdir(dirpath, 0777)
    seg.burp(path.Join(dirpath, strconv.Itoa(seg.ID)))
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


func (pb *PBServer) broadcastForward(op Op, segment SegmentID, group BackupGroup) bool {

  port := strconv.Itoa(SrvPort)

  // setup
  replychan := make(chan data)
  ackchan   := make(chan data)

  replies := make([]*ForwardOpReply, len(group))
  acks    := make([]bool, len(group))

  // set the args
  fwdArgs  := make(ForwardOpArgs)
  fwdArgs.Origin = pb.me
  fwdArgs.Op = op
  fwdArgs.Segment = segment

  for i:= 0; i < Retries; i++ {

    to := 10 * time.Millisecond
    count := 0

    // for each guy who hasn't acked
    for idx, backup := range group {
      if (acks[idx] == false ) {
        count += 1
        go func(replychan chan, ackchan chan, idx int, ServerID backup) {
          fwdReply := make(ForwardOpReply)
          ack := call(backup + ":" + port, 'PBServer.ForwardOp', fwdArgs, fwdReply)
          replychan <- data{idx, fwdReply}
          ackchan   <- data{idx, fwdAck}
        }(replychan, ackchan, idx, backup)
      }
    }

    // collect up responses and acks
    for i:= 0; i < count; i++ {
      reply := <- replychan
      replies[reply.i] = reply.val

      ack := <- ackchan
      acks[ack.i] = ack.val
    }

    numAcked := 0

    // process the responses.
    for idx, ack := range acks {
      if ack == false {

      } else {
        reply := replies[idx]
        if (reply.Err) {
          return false
        } else {
          numAcked += 1
        }
      }
    }

    if numAcked == len(group) {
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


func (pb *PBServer) broadcastFlush(segment SegmentID, group BackupGroup) bool {

  port := strconv.Itoa(SrvPort)

  // setup
  replychan := make(chan data)
  ackchan   := make(chan data)

  replies := make([]*FlushSegReply, len(group))
  acks    := make([]bool, len(group))

  // set the args
  flshArgs  := make(ForwardOpArgs)
  flshArgs.Origin = pb.me
  flshArgs.Op = op

  for i:= 0; i < Retries; i++ {

    to := 10 * time.Millisecond
    count := 0

    // for each guy who hasn't acked
    for idx, backup := range group {
      if (acks[idx] == false ) {
        count += 1
        go func(replychan chan, ackchan chan, idx int, ServerID backup) {
          flshReply := new(FlushSegReply)
          ack := call(backup + ":" + port, 'PBServer.FlushSeg', flshArgs, flshReply)
          replychan <- data{idx, flshReply}
          ackchan   <- data{idx, flshAck}
        }(replychan, ackchan, idx, backup)
      }
    }

    // collect up responses and acks
    for i:= 0; i < count; i++ {

      reply := <- replychan
      replies[reply.i] = reply.val

      ack := <- ackchan
      acks[ack.i] = ack.val

    }

    numAcked := 0

    // process the responses.
    for idx, ack := range acks {
      if ack == false {

      } else {
        reply := replies[idx]
        if (reply.Err) {
          return false
        } else {
          numAcked += 1
        }
      }
    }

    if numAcked == len(group) {
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
      segment.ID = SegmentID(i)
      for {
        op := Op{}
        op.Client = ClientID(0)
        op.Request = RequestID(0)
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
        sendargs.Segments = make([]SegmentID, 1)
        sendargs.Segments[0] = SegmentID(rand.Int63() % 30)
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

  pb.buffers = map[ServerID]*Segment{}

  pb.backedUpShards = map[ServerID]map[SegmentID]bool{}

  pb.backups = map[SegmentID]BackupGroup{}

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
