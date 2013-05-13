/*
*********************
Package and Imports
*********************
*/


package pbservice

import (
  "viewservice"
  "net/rpc"
  "fmt"
  "time"
)

// clerk for the pbservice which encapsulates a viewservice clerk
type Clerk struct {
	vs *viewservice.Clerk
  view viewservice.View
  ClientID int64
  RequestID int64
  networkMode string
}


// makes a new clerk for the pbservice which encapsulates a viewservice clerk
func MakeClerk(me string, vshost string, networkMode string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost, networkMode)
  ck.networkMode = networkMode
  return ck
}


// sends an RPC
func call(srv string, rpcname string, networkMode string, args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial(networkMode, srv)
  if errx != nil {
    fmt.Println(rpcname, errx)
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  return false
}



// get a value for the key from the pbservice
func (ck *Clerk) Get(key string) string {

  if ck.viewIsInvalid() {
    ck.updateView()
  }

  args := GetArgs{}
  args.Key = key
  var reply GetReply

	// retry Get until succesful, updating view each attempt
  for {
    shard := key2shard(args.Key)
    primary, ok := ck.view.ShardsToPrimaries[shard]
    if ok {
      ack := call(primary, "PBServer.Get", ck.networkMode, args, &reply)
      if ack { break }
    }
    ck.updateView()
    time.Sleep(viewservice.PING_INTERVAL)
  }

  switch reply.Err {
  case ErrNoKey:
    fmt.Println("errnokey")
  case ErrWrongServer:
    fmt.Println("errwrongserver")
  }

  if reply.Err == ErrNoKey {
    return ""
  }

  return reply.Value
}


// put a value for the key from the pbservice
func (ck *Clerk) Put(key string, value string) {

  if ck.viewIsInvalid() {
    ck.updateView()
  }

  ck.RequestID += 1

  args := PutArgs{}
  args.Key = key
  args.Value = value
  args.Client = ck.ClientID
  args.Request = ck.RequestID

  var reply PutReply

  for {

    shard := key2shard(args.Key)
    primary, ok := ck.view.ShardsToPrimaries[shard]

    if ok {
      ack := call(primary, "PBServer.Put", ck.networkMode, args, &reply)
      if ack { break }
    }

    ck.updateView()
    time.Sleep(viewservice.PING_INTERVAL)
  }

  switch reply.Err {
  case ErrWrongServer:
    fmt.Println("errwrongserver")
  }

}

func (ck *Clerk) updateView() {
  view,_ := ck.vs.Get()
  ck.view = view
}

func (ck *Clerk) viewIsInvalid() bool {
  return ck.view.ViewNumber == 0
}

func key2shard(key string) int {
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= 100
  return shard
}
