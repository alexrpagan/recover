/*
*********************
Package and Imports
*********************
*/


package viewservice


import "net/rpc"
import "fmt"


/*
*********************
Unmodified Structures
*********************
*/


//
// the viewservice Clerk lives in the client
// and maintains a little state.
//
type Clerk struct {
  me string      // client's name (host:port)
  server string  // viewservice's host:port
  view View
  networkMode string
}


func MakeClerk(me string, server string, networkMode string) *Clerk {
  ck := new(Clerk)
  ck.me = me
  ck.server = server
  ck.view = View{}
  ck.networkMode = networkMode
  return ck
}


func call(srv string, rpcname string, networkMode string, args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial(networkMode, srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  return false
}


func (ck *Clerk) GetServerName() string {
  return ck.server
}


func (ck *Clerk) Ping(viewnum uint) (View, map[string]bool, error) {
  // prepare the arguments.
  args := &PingArgs{}
  args.ServerName = ck.me
  var reply PingReply

  // send an RPC request, wait for the reply.
  ok := call(ck.server, "ViewServer.Ping", ck.networkMode, args, &reply)
  if ok == false {
    ck.view = View{}
    return View{}, make(map[string]bool), fmt.Errorf("Ping(%v) failed", viewnum)
  }

  ck.view = reply.View
  return reply.View, reply.ServersAlive, nil
}


func (ck *Clerk) Get() (View, bool) {
  args := &GetArgs{}
  var reply GetReply
  ok := call(ck.server, "ViewServer.Get", ck.networkMode, args, &reply)

  if ok == false {
    return View{}, false
  }
  return reply.View, true
}

func (ck *Clerk) RecoveryCompleted(me string, shard int) bool {
  args  := RecoveryCompletedArgs{}
  args.ServerName = me
  args.ShardRecovered = shard
  reply := &RecoveryCompletedReply{}

  ok := call(ck.server, "ViewServer.RecoveryCompleted", ck.networkMode, args, &reply)

  // TODO: make sure that recovered primary is correct primary
  if ok == false {
    return false
  }

  return true
}


/*
********************
Utility Functions
********************
*/
