package pbservice

const (

  OK = "OK"

  ErrNoKey = "ErrNoKey"

  ErrWrongServer = "ErrWrongServer"

  ErrNotPrimary = "ErrNotPrimary"

  ErrNotResponsible = "ErrNotResponsible"

  ErrBackupFailure = "ErrBackupFailure"

  ErrNotPending = "ErrNotPending"
  
  PULL_SEGMENTS_SLEEP_INTERVAL = time.Millisecond * 300

)

const SrvPort = 5001

type Err string


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

type TestReadSegmentReply struct {}i


type PutOrder struct {

	SegmentID int64
	OpIndex int

}

type PullSegmentsByShardsArgs struct {

	Shards map[int] bool
	Segments []int64

}

type PullSegmentsByShardsReply struct {

	Segments []Segment

}
