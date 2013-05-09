package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  Wait = "Wait"  //TODO: what does this mean?
)
type Err string

type PutArgs struct {
  Num int
  Key string
  Value string
  Uid int
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Num int
  Key string
}

type GetReply struct {
  Err Err
  Value string
}

type YourConfigArgs struct {
}

type YourConfigReply struct {
  Num int
}

type GroupConfigArgs struct {
}

type GroupConfigReply struct {
  Num int
}

type GetShardArgs struct {
  Shard int
  CurConfig int
  Me int
  GID int64
}

type GetShardReply struct {
  Map map[string]string
  Err string
}