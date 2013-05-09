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

const SrvPort = 5001

type Err string

// > 0
type SegmentID int64
type ClientID int64
type RequestID int64
type VersionID int64
type ServerID string

// Put

type PutArgs struct {
  Key string
  Value string
  Client ClientID
  Request RequestID
}

type PutReply struct {
  Err Err
}

// Get

type GetArgs struct {
  Key string
  Client ClientID
  Request RequestID
}

type GetReply struct {

}


// Forward Op

type ForwardOpArgs struct {
  Origin ServerID
  Op Op
  Segment SegmentID
}

type ForwardOpReply struct {
  Err Err
}


type FlushSegArgs struct {
  Origin ServerID
  OldSegment SegmentID
}

type FlushSegReply struct {
  Err Err
}

// EnlistReplica

type EnlistReplicaArgs struct {
  Origin ServerID
  Segment Segment
}

type EnlistReplicaReply struct {}


// PullSegments

type PullSegmentsArgs struct {
  Segments []SegmentID
}

type PullSegmentsReply struct {
  Segments []Segment
}


//TESTING STUFF

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