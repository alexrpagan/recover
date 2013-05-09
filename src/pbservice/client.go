/*
*********************
Package and Imports
*********************
*/


package pbservice


import "viewservice"
import "net/rpc"
import "time"


/*
*********************
Unmodified Structures
*********************
*/


// clerk for the pbservice which encapsulates a viewservice clerk
type Clerk struct {
	vs *viewservice.Clerk
}


/*
*********************
Unmodified Code
*********************
*/


// makes a new clerk for the pbservice which encapsulates a viewservice clerk
func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  return ck
}


// sends an RPC
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("tcp", srv)
  if errx != nil {
    fmt.Println(errx)
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


// get a value for the key from the pbservice
func (ck *Clerk) Get(key string) string {
  // what if no view has been chosen by the time this has been called?
  if ck.viewIsInvalid() {
    ck.updateView()
  }

  args := GetArgs{}
  args.Key = key
  var reply GetReply

	// retry Get until succesful, updating view each attempt
  for {
    ok := call(ck.view.Primary, "PBServer.Get", args, &reply)
    if ok == false {
      ck.updateView()
      time.Sleep(viewservice.PingInterval)
    } else {
      break
    }
  }
  if reply.Err == ErrNoKey {
    return "errnokey"
  }

  return reply.Value
}


// put a value for the key from the pbservice
func (ck *Clerk) Put(key string, value string) {
  if ck.viewIsInvalid() {
    ck.updateView()
  }

  args := PutArgs{}
  args.Key = key
  args.Value = value
  var reply PutReply

  for {
    ok := call(ck.view.Primary, "PBServer.Put", args, &reply)
    if ok == false {
      ck.updateView()
      time.Sleep(viewservice.PingInterval)
    } else {
      break
    }
  }

}


/*
********************
Utility Functions
********************
*/


// test if a clerk's view is invalid
func (ck *Clerk) viewIsInvalid() bool {
  return ck.view.Viewnum == 0
}


// update the clerk's view
func (ck *Clerk) updateView() {
  currview, _ := ck.vs.Get()
  ck.view = currview
}
