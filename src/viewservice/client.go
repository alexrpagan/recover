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
}


/*
*********************
Unmodified Code
*********************
*/


func MakeClerk(me string, server string) *Clerk {
  ck := new(Clerk)
  ck.me = me
  ck.server = server
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


/*
*********************
Modified Structures
*********************
*/


/*
*********************
Modified Code
*********************
*/


func (ck *Clerk) Ping(viewnum uint) (View, error) {
  // prepare the arguments.
  args := &PingArgs{}
  args.Me = ck.me
  args.Viewnum = viewnum
  var reply PingReply

  // send an RPC request, wait for the reply.
  ok := call(ck.server, "ViewServer.Ping", args, &reply)
  if ok == false {
    return View{}, fmt.Errorf("Ping(%v) failed", viewnum)
  }

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


func (ck *Clerk) Primary() string {
  v, ok := ck.Get()
  if ok {
    return v.Primary
  }
  return ""
}
