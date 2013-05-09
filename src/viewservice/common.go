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
	ViewNumber uint

}

type PingReply struct {

	CurrentViewNumber uint
	ServersAlive []string				// array of servers primaries can choose as backups

}


type GetArgs struct {

}

type GetReply struct {

	CurrentView View

}


type QueryRangesArgs {

	ShardsToRecover map[int] bool

}

type QueryRangesReply {

	ServerName string
	Ranges []Range

}

type Range struct {

	low int
	high int

}
