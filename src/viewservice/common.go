package viewservice


import "time"


const PING_INTERVAL = time.Millisecond * 100
const DEAD_PINGS = 5
const REPLICATION_LEVEL = 3
const CRITICAL_MASS = 8
const NUMBER_OF_SHARDS = 100
const QUERY_SEGMENTS_SLEEP_INTERVAL = time.Millisecond * 300


type View struct {

	ViewNumber uint
	ShardsToPrimaries map[int] string		// shard #{shard index} -> primary

}


type PingArgs struct {

	ServerName string
	ViewNumber uint

}

type PingReply struct {

	View View
	ServersAlive map[string] bool			// set of servers primaries can choose as backups

}


type GetArgs struct {

}

type GetReply struct {

	View View

}


type QuerySegmentsArgs struct {

	DeadPrimaries string
	ShardsToRecover map[int] bool

}

type QuerySegmentsReply struct {

	ServerName string
	ShardsToSegments map[int] (map[int64] bool)

}


type ElectRecoveryMasterArgs struct {

	ShardsToSegmentsToServers map[int] (map[int64] (map[string] bool))	

}

type ElectRecoveryMasterReply struct {

}


type RecoveryCompletedArgs struct {

	ServerName string
	ShardRecovered int

}

type RecoveryCompletedReply struct {

}
