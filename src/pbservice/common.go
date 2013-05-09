package pbservice

const (

  OK = "OK"

  ErrNoKey = "ErrNoKey"

  ErrWrongServer = "ErrWrongServer"

  // sent by backup when delusional server tries to forward or commit an operation
  ErrNotPrimary = "ErrNotPrimary"

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
}

type ForwardOpReply struct {
  Err Err
}


// Commit OP

type CommitOpArgs struct {
  Origin ServerID
  Op Op
  Commit bool
}

type CommitOpReply struct {
  Err Err
}


// PullSegments

type PullSegmentsArgs struct {
  Segments []SegmentID
}

type PullSegmentsReply struct {
  Segments []Segment
}

// TestSendSegment

type TestPullSegmentsArgs struct {
  Size int
  Hosts []string
}

type TestPullSegmentsReply struct {
}

// TestWriteSegment

type TestWriteSegmentArgs struct {
  NumOfSegs int
}

type TestWriteSegmentReply struct {
}

type TestReadSegmentArgs struct {
  NumOfSegs int
}

type TestReadSegmentReply struct {
}