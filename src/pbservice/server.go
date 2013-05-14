package pbservice

import (
  "net"
  "fmt"
  "net/rpc"
  "log"
  "time"
  "sync"
  "os"
  "syscall"
  "math/rand"
  "reflect"
  "unsafe"
  "path"
  "encoding/gob"
  "strconv"
  "viewservice"
  "crypto/md5"
  "io"
  "strings"
)


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
const Retries = 5

// absolute path to where log segments should be stored
const SegPath = "/tmp/segment/"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  meHash string

  // TODO: reference to shardmaster
  clerk *viewservice.Clerk

  view viewservice.View
  //Config shardmaster.Config

  log *Log

  // pointers to PUTs live here.
  store map[string]*Op

  // backup buffers: map each server to a Segment.
  backupMu sync.Mutex
  buffers map[string]*Segment

  // which segs am I responsible for?
  backedUpSegs map[string]map[int64]map[int]bool

  // primary's backup map
  backups map[int64]BackupGroup

  // which segments have we already banished to disk
  flushed map[int64]bool

  // have we seen these puts?
  request map[Request]bool

  // who's around?
  serversAlive map[string]bool

  networkMode string

}

// REQUEST

type Request struct {
  Client int64
  Request int64
}


// OPERATION

type Op struct {
  Version int64
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
  Segments map[int64]*Segment
  CurrSegID int64
  CurrOpID int64
}

func (l *Log) init() {
  l.Segments = make(map[int64]*Segment)

  l.CurrOpID = int64(0)

  seg := new(Segment)
  seg.Size = 0
  seg.Active = true
  seg.ID = rand.Int63()
  seg.Digest = make([]int64, 0)

  l.Segments[seg.ID] = seg
  l.CurrSegID = seg.ID
}

func (l *Log) getCurrSegment() (seg *Segment, ok bool) {
  s, o := l.Segments[l.CurrSegID]
  return s, o
}

func (l *Log) newSegment() *Segment {
  prevSegment, _ := l.getCurrSegment()

  seg := new(Segment)
  seg.Size = 0
  seg.Active = true
  seg.ID = rand.Int63()
  seg.Ops = make([]Op, 0)

  seg.Digest = append(prevSegment.Digest, prevSegment.ID)

  l.Segments[seg.ID] = seg
  l.CurrSegID = seg.ID

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
  Backups  []string
}


// records that the segment that we're backing contains ops from a certain shard
func (pb *PBServer) recordShardBackup (origin string, segID int64, op Op) {

  _, ok1 := pb.backedUpSegs[origin]
  if ! ok1 {
    pb.backedUpSegs[origin] = make(map[int64] map[int]bool)
  }

  _, ok2 := pb.backedUpSegs[origin][segID]
  if ! ok2 {
    pb.backedUpSegs[origin][segID] = make(map[int]bool)
  }

  pb.backedUpSegs[origin][segID][key2shard(op.Key)] = true

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
  newGroup := make([]string, 0)

  for _, srv := range group.Backups {
    if pb.serversAlive[srv] {
      newGroup = append(newGroup, srv)
    }
  }

  group.Backups = newGroup
  pb.backups[seg.ID] = group

  if ! ok {
    if pb.enlistReplicas(*seg) == false {
      fmt.Println("couldn't enlist enough replicas")
      reply.Err = ErrBackupFailure
      return nil
    } else {
      group = pb.backups[seg.ID]
    }
  }

  // advance opcount.
  pb.log.CurrOpID++

  // create operation
  putOp := new(Op)
  putOp.Client = args.Client
  putOp.Request = args.Request
  putOp.Type = PutOp
  putOp.Key = args.Key
  putOp.Value = args.Value
  putOp.Version = pb.log.CurrOpID

  shard := key2shard(putOp.Key)
  if shard > 0 && false {
    reply.Err = ErrWrongServer
    return nil
  }

  flushed := false

  if seg.append(*putOp) == false {
    // fmt.Println("Flushing ", pb.me, seg.ID, group)
    flushed = true
    if pb.broadcastFlush(seg.ID, group) {
      seg = pb.log.newSegment()
      seg.append(*putOp)
      if pb.enlistReplicas(*seg) == false {
        fmt.Println("couldn't enlist enough replicas")
        reply.Err = ErrBackupFailure
        return nil
      } else {
        group = pb.backups[seg.ID]
      }
    } else {
      fmt.Println("backup failure on flush")
      reply.Err = ErrBackupFailure
      return nil
    }
  }

  if flushed || pb.broadcastForward(*putOp, seg.ID, group) {
    pb.store[args.Key] = putOp
  } else {
    fmt.Println("backup failure on fwd")
    reply.Err = ErrBackupFailure
    return nil
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
    _, ok := segs[segment]
    if ok == false {
      fmt.Println("Not responsible!")
      return ErrNotResponsible
    }
  }

  return OK
}

func (pb *PBServer) enlistReplicas(segment Segment) bool {

  hostsNeeded := RepLevel

  availHosts := map[string]bool{}
  enlisted   := map[string]bool{}

  for srv, alive := range pb.serversAlive {
    if alive {
      availHosts[srv] = alive
    }
  }

  delete(availHosts, pb.me)

  numHosts  := len(availHosts)

  enlistArgs := new(EnlistReplicaArgs)
  enlistArgs.Origin = pb.me
  enlistArgs.Segment = segment

  if numHosts <= hostsNeeded {
    fmt.Println("Not enough hosts", numHosts, hostsNeeded, pb.serversAlive)
    return false
  }

  for {

    idxs       := rand.Perm(len(availHosts))
    candidates := make([]string, numHosts - len(enlisted))

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
      host := candidates[replicaIdxs[i]]
      if (acks[i] == false) {
        wg.Add(1)
        go func(i int, backup string) {
          enlistReply := new(EnlistReplicaReply)
          enlistAck   := call(backup, "PBServer.EnlistReplica", pb.networkMode, enlistArgs, enlistReply)
          replies[i] = enlistReply
          acks[i]    = enlistAck
          wg.Done()
        }(i, host)
      }
    }
    wg.Wait()

    for idx, ack := range acks {
      host := candidates[replicaIdxs[idx]]
      if ack {
        reply := replies[idx]
        if (reply.Err != OK) {
          fmt.Println("ERROR: ", reply.Err)
        } else {
          hostsNeeded--
          enlisted[host] = true
        }
      }
      delete(availHosts, host)
    }

    if hostsNeeded == 0 {

      bg := new(BackupGroup)
      bg.Backups = make([]string, RepLevel)

      i := 0
      for k, _ := range enlisted {
        bg.Backups[i]  = k
        i++
      }
      pb.backups[segment.ID] = *bg

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
  pb.backupMu.Lock()
  defer pb.backupMu.Unlock()

  segID  := args.Segment.ID
  origin := args.Origin

  segs, segok := pb.backedUpSegs[origin]

  if ! segok {
    // segment uuids -> set of shard ids
    segs = make(map[int64] map[int]bool)
  }

  _, shardok := segs[segID]

  if shardok == false {

    segs[segID] = make(map[int]bool)

    newSeg := args.Segment

    pb.buffers[origin] = &newSeg

    // fmt.Println("Enlisted", args.Origin, pb.me, newSeg.ID)

    // record shardnum for op in buffer
    for _, op := range args.Segment.Ops {
      pb.recordShardBackup(origin, segID, op)
    }

    pb.backedUpSegs[origin] = segs

  } else {
    panic("replica already enlisted")
  }

  reply.Err = OK
  return nil

}

func (pb *PBServer) FlushSeg(args *FlushSegArgs, reply *FlushSegReply) error {
  pb.backupMu.Lock()
  defer pb.backupMu.Unlock()

  err := pb.checkPrimary(args.Origin, args.OldSegment, "")

  if err != OK {
    reply.Err = err
    return nil
  }

  segPtr, ok := pb.buffers[args.Origin]

  if ok {

    // copy over segment
    seg := *segPtr

    // free buffer
    delete(pb.buffers, args.Origin)

     // write segment to disk in the background
    go func() {

      dirpath := path.Join(SegPath, pb.meHash)
      os.Mkdir(dirpath, 0777)

      dirpath = path.Join(dirpath, pb.md5Digest(args.Origin))
      os.Mkdir(dirpath, 0777)

      seg.burp(path.Join(dirpath, strconv.Itoa(int(seg.ID))))

    }()

  }

  reply.Err = OK
  return nil
}

func (pb *PBServer) ForwardOp(args *ForwardOpArgs, reply *ForwardOpReply) error {
  pb.backupMu.Lock()
  defer pb.backupMu.Unlock()

  seg    := args.Segment
  origin := args.Origin
  op     := args.Op

  err := pb.checkPrimary(origin, seg, op.Key)
  if err != OK {
    reply.Err = err
    return nil
  }

  buf, _ := pb.buffers[origin]
  res := buf.append(op)
  pb.recordShardBackup(origin, seg, op)

  if res == false {
    panic("buffer size exceeded in replica. should never happen.")
  }

  reply.Err = OK
  return nil
}


func (pb *PBServer) broadcastForward(op Op, segment int64, group BackupGroup) bool {

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
          ack := call(backup, "PBServer.ForwardOp", pb.networkMode, fwdArgs, fwdReply)
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
      if ack {
        reply := replies[idx]
        if (reply.Err != OK) {
          fmt.Println("ERROR ", reply.Err)
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
          ack := call(backup, "PBServer.FlushSeg", pb.networkMode, flshArgs, flshReply)
          replies[idx] = flshReply
          acks[idx] = ack
        }(idx, backup)
      }
    }

    numAcked := 0

    // process the responses.
    for idx, ack := range acks {
      if ack {
        reply := replies[idx]
        if (reply.Err != OK) {
          fmt.Println("ERROR ", reply.Err)
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
	view, serversAlive, err := pb.clerk.Ping(pb.view.ViewNumber)
  if err == nil {
    // don't bother waiting for the lock.
    go func() {
      // fmt.Println("tick lock", pb.me)
      pb.mu.Lock()
      // fmt.Println("tick acq", pb.me)
      if pb.view.ViewNumber < view.ViewNumber {
        pb.view = view
        pb.serversAlive = serversAlive
      }
      pb.mu.Unlock()
    }()
  }
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
        ok := call(host, "PBServer.PullSegments", pb.networkMode, sendargs, sendreply)
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

// tell the viewserver which shards you have segments for and which segments you have
func (pb *PBServer) QuerySegments(args *QuerySegmentsArgs, reply *QuerySegmentsReply) error {

  // subset of backedUpSegs relevant to query
	relevant := make(map[string]map[int64]map[int]bool)

	for dead, _ := range args.DeadPrimaries {
    segMap, ok := pb.backedUpSegs[dead]
    if ok {
      relevant[dead] = segMap
    }
	}

  reply.ServerName = pb.me
  reply.BackedUpSegments = relevant

	return nil
}


func (pb *PBServer) PullSegmentsByShards(args *PullSegmentsByShardsArgs, reply *PullSegmentsByShardsReply) error {
  segments := make([]Segment, len(args.Segments))
  var wg sync.WaitGroup
  for i, segId := range args.Segments {
    wg.Add(1)
    go func(i int, segId int64) {
      pb.backupMu.Lock()
      buf, ok := pb.buffers[args.Owner]
      pb.backupMu.Unlock()

      oldSeg := &Segment{}

      if ok && buf.ID == segId {
        oldSeg = buf
      } else {
        fname := strconv.Itoa(int(segId))
        oldSeg.slurp(path.Join(SegPath, pb.meHash, pb.md5Digest(args.Owner), fname))
        fmt.Println("Pulled from disk", segId, oldSeg.Size)
      }

      newSeg := Segment{}
      // filter out operations from irrelevant shards
      for _, op := range oldSeg.Ops {
        if args.Shards[key2shard(op.Key)] {
          newSeg.append(op)
        }
      }

      segments[i] = newSeg
      wg.Done()
    }(i, segId)
  }
  wg.Wait()
  reply.Segments = segments
  return nil
}


// recover the shards in args.ShardsToSegmentsToServers
func (pb *PBServer) ElectRecoveryMaster(args *ElectRecoveryMasterArgs, reply *ElectRecoveryMasterReply) error {

  recoveryData       := args.RecoveryData
  segmentsRecovered  := make(map[int64]*Segment)
  segmentsInProcess  := make(map[int64]time.Time)

  var recoveryMu sync.Mutex

  // which shards are we interested in for this recovery
  shards := make(map[int]bool)
  for shard, _ := range recoveryData {
    shards[shard] = true
  }

  recoveredData := make(map[int]int)

  for {

    // fmt.Println(recoveryData)

    for shard, segsToBackups := range recoveryData {  // for each shard that we're tasked with recovering

      for seg, backups := range segsToBackups {  // and each log segment needed to recover that shard

        recoveryMu.Lock()
        _, recovered := segmentsRecovered[seg]
        recoveryTime, inProcess := segmentsInProcess[seg]
        recoveryMu.Unlock()

        // ten second timeout on single shard.
        if (!inProcess || time.Since(recoveryTime) >= 10 * time.Second) && !recovered {

          recoveryMu.Lock()
          segmentsInProcess[seg] = time.Now()
          recoveryMu.Unlock()

          backup := backups[rand.Int() % len(backups)]

          go func(seg int64, backup string, shard int) {

            var mainPrimary string

            OUTER:
            for primary, shards := range args.DeadPrimaries {
              for _, s := range shards {
                if s == shard {
                  mainPrimary = primary
                  break OUTER
                }
              }
            }

            pullSegmentsArgs  := new(PullSegmentsByShardsArgs)
            pullSegmentsArgs.Segments = []int64{seg}
            pullSegmentsArgs.Shards   = shards
            pullSegmentsArgs.Owner    = mainPrimary

            pullSegmentsReply := new(PullSegmentsByShardsReply)

            // fmt.Printf("Attempting: %s recovering segment %d from %s for %s:%d \n", pb.me, seg, backup, mainPrimary, shard)
            ok1 := call(backup, "PBServer.PullSegmentsByShards", pb.networkMode, pullSegmentsArgs, pullSegmentsReply)

            if ok1 {

              recoveryMu.Lock()
              recovered := pullSegmentsReply.Segments[0]
              segmentsRecovered[seg] = &recovered
              delete(segmentsInProcess, seg)
              recoveryMu.Unlock()

              for _, newOp := range recovered.Ops {

                recoveryMu.Lock()
                recoveredData[shard] = recoveredData[shard] + newOp.size()
                recoveryMu.Unlock()

                pb.mu.Lock()

                op := newOp

                maxOpID := pb.log.CurrOpID
                currOp, ok := pb.store[op.Key]

                if ok {
                  // if the version of the key in the data store is more up-to-date,
                  // don't bother processing the recovered operation.
                  if currOp.Version > op.Version {
                  	pb.mu.Unlock()
                    continue
                  }
                }

                // update primary's clock
                if maxOpID < op.Version {
                  pb.log.CurrOpID = op.Version
                } else {
                  pb.log.CurrOpID++
                  op.Version = pb.log.CurrOpID
                }

                seg, _ := pb.log.getCurrSegment()
                group, ok := pb.backups[seg.ID]

                newGroup := make([]string, 0)
                for _, srv := range group.Backups {
                  _, dead := args.DeadPrimaries[srv]
                  if dead == false && pb.serversAlive[srv] {
                    newGroup = append(newGroup, srv)
                  }
                }

                group.Backups = newGroup
                pb.backups[seg.ID] = group

                // normal put codepath
                flushed := false
                if seg.append(op) == false {
                  flushed = true
                  if pb.broadcastFlush(seg.ID, group) {
                    seg = pb.log.newSegment()
                    seg.append(op)
                    if pb.enlistReplicas(*seg) == false {
                      fmt.Println("couldn't enlist enough replicas")
                    } else {
                      group = pb.backups[seg.ID]
                    }
                  } else {
                    fmt.Println("backup failure on flush")
                  }
                }

                if flushed || pb.broadcastForward(op, seg.ID, group) {
                  pb.store[op.Key] = &op
                } else {
                  fmt.Println("backup failure on fwd")
                }

                pb.mu.Unlock()
              }

            }

          }(seg, backup, shard)

        }

      }

      // have we seen all the segments that we need for a given shard?
      seenAll := true
      for seg, _ := range segsToBackups {
        _, seen := segmentsRecovered[seg]
        if ! seen {
          seenAll = false
          break
        }
      }

      if seenAll {
        delete(recoveryData, shard)
        pb.clerk.RecoveryCompleted(pb.me, shard, recoveredData[shard])
      }

    }

    if len(recoveryData) == 0 {
      return nil
    } else {
      time.Sleep(50 * time.Millisecond)
    }

  }


	return nil
}


func (pb *PBServer) md5Digest(name string) string {
  // hash host to make directory name
  h1 := md5.New()
  io.WriteString(h1, name)
  return fmt.Sprintf("%x", h1.Sum([]byte{}))
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  fmt.Println("kill ", pb.me)
  pb.dead = true
  pb.l.Close()
}

func StartServer(me string, viewServer string) *PBServer {
  return StartMe(me, viewServer, "unix")
}

func StartMe(me string, viewServer string, networkMode string) *PBServer {

  pb := new(PBServer)

  pb.me = me

  pb.meHash = pb.md5Digest(me)

  os.RemoveAll(path.Join(SegPath, pb.meHash))
  os.Mkdir(SegPath, 0777)

  pb.view = viewservice.View{}

  pb.clerk = viewservice.MakeClerk(me, viewServer, networkMode)

  // initialize main data structures
  pb.log = new(Log)
  pb.log.init()

  pb.store = map[string]*Op{}

  pb.buffers = map[string]*Segment{}

  pb.backedUpSegs = map[string]map[int64]map[int]bool{}

  pb.backups = map[int64]BackupGroup{}

  pb.serversAlive = map[string]bool{}

  pb.networkMode = networkMode

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  hostname := pb.me

  if networkMode == "unix" {
    os.Remove(hostname)
  } else if networkMode == "tcp" {
    arr := strings.Split(hostname, ":")
    hostname = ":" + arr[1]
  }

  l, e := net.Listen(networkMode, hostname);

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
        //pb.kill()
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
