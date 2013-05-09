package viewservice


import "time"


const PING_INTERVAL = time.Millisecond * 100
const DEAD_PINGS = 5
const REPLICATION_LEVEL = 3
const CRITICAL_MASS = 8
const NUMBER_OF_SHARDS = 100
const QUERY_RANGES_SLEEP_INTERVAL = time.Millisecond * 300


type View struct {

	viewNumber uint
	shardsToPrimaries []string			// shard #{shard index} -> primary

}


type PingArgs struct {

	ServerName string

}

type PingReply struct {

	ViewNumber uint
	ServersAlive []string				// array of servers primaries can choose as backups

}


type GetArgs struct {

}

type GetReply struct {

	View View

}


type QuerySegmentsArgs struct {

	ShardsToRecover map[int] bool

}

type QuerySegmentsReply struct {

	ServerName string
	ShardsToSegments map[int] LogSegmentID

}


type ElectRecoveryMasterArgs struct {

	ShardsToSegmentsToServers map[int] (map[LogSegmentID] (map[string] bool))

}

type ElectRecoveryMasterReply struct {

}


type RecoveryCompletedArgs struct {

	ServerName string
	ShardsRecovered []int

}

type RecoveryCompletedReply struct {

}
