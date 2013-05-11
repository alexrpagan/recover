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
}


func MakeClerk(me string, server string) *Clerk {
  ck := new(Clerk)
  ck.me = me
  ck.server = server
  ck.view = View{}
  return ck
}


func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
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


func (ck *Clerk) Ping(viewnum uint) (View, error) {
  // prepare the arguments.
  args := &PingArgs{}
  args.ServerName = ck.me
  var reply PingReply

  // send an RPC request, wait for the reply.
  ok := call(ck.server, "ViewServer.Ping", args, &reply)
  if ok == false {
    ck.view = View{}
    return View{}, fmt.Errorf("Ping(%v) failed", viewnum)
  }

  ck.view = reply.View
  return reply.View, nil
}


func (ck *Clerk) Get() (View, bool) {
  args := &GetArgs{}
  var reply GetReply
  ok := call(ck.server, "ViewServer.Get", args, &reply)
  if ok == false {
    return View{}, false
  }
  return reply.View, true
}


/*
********************
Utility Functions
********************
*/
