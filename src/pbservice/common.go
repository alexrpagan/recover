package pbservice

const (

  OK = "OK"

  ErrNoKey = "ErrNoKey"

  ErrWrongServer = "ErrWrongServer"

  ErrNotPrimary = "ErrNotPrimary"

  ErrNotResponsible = "ErrNotResponsible"

  ErrBackupFailure = "ErrBackupFailure"

  ErrNotPending = "ErrNotPending"

)

type Err string

type PutOrder struct {
  SegmentID int64
  OpIndex int
}


// Put

type PutArgs struct {
  Key string
  Value string
  Client int64
  Request int64
}

type PutReply struct {
  Err Err
}

// Get

type GetArgs struct {
  Key string
  Client int64
  Request int64
}

type GetReply struct {
  Err Err
  Value string
}


// Forward Op

type ForwardOpArgs struct {
  Origin string
  Op Op
  Segment int64
}

type ForwardOpReply struct {
  Err Err
}


// Flush Seg

type FlushSegArgs struct {
  Origin string
  OldSegment int64
}

type FlushSegReply struct {
  Err Err
}

// EnlistReplica

type EnlistReplicaArgs struct {
  Origin string
  Segment Segment
}

type EnlistReplicaReply struct {
  Err Err
}


// PullSegments

type PullSegmentsArgs struct {
  Segments []int64
}

type PullSegmentsReply struct {
  Segments []Segment
}


// PullSegmentsByShard

type PullSegmentsByShardsArgs struct {
  Owner string
  Shards map[int] bool
  Segments []int64
}

type PullSegmentsByShardsReply struct {
  Segments []Segment
}


// QuerySegments

type QuerySegmentsArgs struct {
  DeadPrimaries map[string][]int
}

type QuerySegmentsReply struct {
  ServerName string
  BackedUpSegments map[string]map[int64]map[int]bool
}


// ElectRecoveryMaster

type ElectRecoveryMasterArgs struct {
  RecoveryData map[int]map[int64][]string
  DeadPrimaries map[string][]int
}

type ElectRecoveryMasterReply struct {
  ServerName string
  ShardRecovered int
}


type KillArgs struct {

}

type KillReply struct {

}

// TESTING----------------------------

// TestSendSegment

type TestPullSegmentsArgs struct {
  Size int
  Hosts []string
}

type TestPullSegmentsReply struct {}

// TestWriteSegment

type TestWriteSegmentArgs struct {
  NumOfSegs int
}

type TestWriteSegmentReply struct {}

type TestReadSegmentArgs struct {
  NumOfSegs int
}

type TestReadSegmentReply struct {}

// \TESTING---------------------------

