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

  // keep track of operations that are pending
  pending map[ServerID]Op

  // which shards am I responsible for?
  backedUpShards map[ServerID]map[SegmentID]bool

  // primary's backup map
  backups map[SegmentID]BackupGroup
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

func (l *Log) newSegment(server ServerID) {
  //prevSegment, _ := l.getCurrSegment()

  seg := new(Segment)
  seg.Size = 0
  seg.Active = true
  seg.ID = l.CurrSegID + 1

  // TODO: how do we make the new digest?
  l.Segments[seg.ID] = seg
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

// for performance testing.
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



func (pb *PBServer) PullSegments(args *PullSegmentsArgs, reply *PullSegmentsReply) error {

  // TODO: what if segments don't exist on disk?
  // TODO: limit on number of segments that can be pulled

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

  // get shardnumber for key
  // CHECK: am I the primary for this key

  getOp := Op{}

  res := pb.broadcast(getOp)

  op, ok := pb.store[args.Key]

  if ok == false {
    reply.Err = ErrNoKey
    return nil
  }

  reply.Err = OK
  reply.Value = op.value
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // get shardnumber for key

  // CHECK: am I the primary for this key

  // create a PUT op

  pb.store[args.Key] = args.Value

  reply.Err = OK

  return nil
}

func (pb *PBServer) ForwardOp(args *ForwardOpArgs, reply *ForwardOpReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // TODO: am i responsible for this segment?
  // TODO: this this server responsible for the corresponding shard?
  if true {
    pending, ok := pb.pending[args.Origin]
    if ok {
      // Write previous pending Op to segment
    }
    pb.pending[args.Origin] = args.Op
  }

  reply.Err = OK
  return nil
}


func (pb *PBServer) broadcast(op Op, group BackupGroup) bool {

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

  for i:= 0; i < 8; i++ {

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


func (pb *PBServer) tick() {

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
