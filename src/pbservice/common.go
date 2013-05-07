package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
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