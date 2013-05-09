package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "math"
import "reflect"

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  testing bool
  i_am_proposer bool
  am_proposer map[int]bool
  instances map[int]PaxosInstance // which instances do I know about?
  done []int // latest done value seen
}

type PaxosInstance struct {
  is_decided bool
  n_p int64
  n_p_peer int
  n_a int64
  n_a_peer int
  v_a interface{}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particula
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }
  return false
}


// arguments for RPCs
type PrepareArgs struct {
  Seqnum int
  N int64
  StartProposer bool
}

type PrepareReply struct {
  Ok bool
  N_p int64  //highest prepare seen
  N_a int64  // highest accept seen
  V_a interface{}
  Done int
}

type AcceptArgs struct {
  Seqnum int
  N int64
  V interface{}
}

type AcceptReply struct {
  N_p int64  // highest accept seen
  Ok bool
  Done int
}

type DecidedArgs struct {
  Seqnum int
  N int64
  V interface{}
}

type DecidedReply struct {
  Ok bool
  Done int
}

type ProposeArgs struct {
  Seqnum int
  V interface{}
}

type ProposeReply struct {
  Ok bool
}


// RPCs for paxos messages
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {

  px.mu.Lock()
  defer px.mu.Unlock()

  reply.Done = px.done[px.me]

  inst, _ := px.instances[args.Seqnum]

  reply.Ok = false

  if px.testing {
    fmt.Printf("PREPARE: peer %d, seqnum %d, n=%d, n_p=%d, n_a=%d, v_a=%d\n", px.me, args.Seqnum, args.N, inst.n_p, inst.n_a, inst.v_a)
  }

  if args.N > inst.n_p {
    reply.Ok = true
    inst.n_p = args.N
    reply.N_a = inst.n_a
    reply.V_a = inst.v_a
    px.instances[args.Seqnum] = inst
  }

  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {

  px.mu.Lock()
  defer px.mu.Unlock()

  reply.Done = px.done[px.me]

  inst, _ := px.instances[args.Seqnum]

  reply.Ok = false

  if px.testing {
    fmt.Printf("ACCEPT: peer %d, seqnum %d, n=%d, n_p=%d, n_a=%d, v_a=%d\n", px.me, args.Seqnum, args.N, inst.n_p, inst.n_a, inst.v_a)
  }

  if args.N >= inst.n_p {
    if (inst.v_a != nil && (reflect.DeepEqual(inst.v_a, args.V) == false)) {
//      fmt.Printf("MISMATCH at peer %d -> seqnum %d, Prop: %d, v0: %d (%d), v1: %d, n_p: %d\n", px.me, args.Seqnum, args.N, inst.v_a, inst.n_a, args.V, inst.n_p)
      return nil
    }

    reply.Ok = true
    inst.n_p = args.N
    inst.n_a = args.N
    inst.v_a = args.V
    px.instances[args.Seqnum] = inst

  }

  return nil

}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  inst, _ := px.instances[args.Seqnum]
  inst.is_decided = true
  inst.n_p = args.N
  inst.n_a = args.N
  inst.v_a = args.V
  px.instances[args.Seqnum] = inst
  reply.Done = px.done[px.me]
  return nil
}

func (px *Paxos) majoritySize() int {
  return int(math.Ceil(float64(len(px.peers)) / 2.0))
}

func (px *Paxos) randomSleep() {
  time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
}

func (px *Paxos) nextPropnum() int64 {
  t := time.Now()
  return t.UnixNano() + int64(px.me)
}

func (px *Paxos) Propose(args *ProposeArgs, reply *ProposeReply) error {

  var n int64
  bail_out := false
  px.mu.Lock()
  inst, _ := px.instances[args.Seqnum]
  if inst.is_decided {
    bail_out = true
  }
  px.mu.Unlock()

  if bail_out {
    return nil
  }

  go func () {

    for {

      n = px.nextPropnum()

      px.mu.Lock()
      inst, _ := px.instances[args.Seqnum]
      done := inst.is_decided || px.dead
      px.mu.Unlock()

      if (done) {
        return
      }

      max_n_a := int64(0)
      var max_v_a interface{}

      prep_ack := 0  // number of prepare acknowledgements
      accept_ack := 0  // number of accept acknowledgements

      prep_args := PrepareArgs{}
      prep_args.Seqnum = args.Seqnum
      prep_args.N = n

      for idx := 0; idx < len(px.peers); idx++ {

        prep_reply := PrepareReply{}

        peer := px.peers[idx]

        ok := true
        if idx == px.me {
          px.Prepare(&prep_args, &prep_reply)
        } else {
          ok = call(peer, "Paxos.Prepare", prep_args, &prep_reply)
        }

        if ok {
          if prep_reply.Ok {
            if px.testing {
              fmt.Printf("PREPARE OK: Peer %d, seqnum %d, proposal %d, n_a %d, v_a %d\n", idx, args.Seqnum, n, prep_reply.N_a, prep_reply.V_a)
            }
            prep_ack++
            if prep_reply.N_a > max_n_a {
              if px.testing {
                fmt.Printf("Proposer %d saw peer %d claim n_a=%d,v_a=%d for seqnum %d at proposal %d\n", px.me, idx, prep_reply.N_a, prep_reply.V_a, args.Seqnum, n)
              }
              max_n_a = prep_reply.N_a
              max_v_a = prep_reply.V_a
            }
          } else {
            if px.testing {
              fmt.Printf("PREPARE REJECT: Peer %d, seqnum %d, proposal %d\n", idx, args.Seqnum, n)
            }
          }

          // update done vector
          px.mu.Lock()
          px.done[idx] = prep_reply.Done
          px.mu.Unlock()

        } else {
          if px.testing{
            fmt.Printf("PREPARE FAILED: Peer %d, seqnum %d, proposal %d\n", idx, args.Seqnum, n)
          }
        }

      }

      // do we have a majority?
      if prep_ack >= px.majoritySize() {
        if px.testing {
          fmt.Printf("PREPARE: Proposer %d sees majority (%d/%d), value %d (n_a=%d), for seqnum %d at proposal %d\n", px.me, prep_ack, len(px.peers), max_v_a, max_n_a, args.Seqnum, n)
        }

        accept_args := AcceptArgs{}
        accept_args.Seqnum = args.Seqnum
        accept_args.N = n

        if max_n_a == int64(0) { // proposers have not accepted any values
          if px.testing{
            fmt.Printf("picking proposer %d's value %d for seqnum %d at proposal %d\n", px.me, args.V, args.Seqnum, n)
          }
          max_v_a = args.V
        }

        accept_args.V = max_v_a

        //fmt.Println("Sending out accepts" , px.me)

        for idx := 0; idx < len(px.peers); idx++ {

          accept_reply := AcceptReply{}

          peer := px.peers[idx]

          ok := true
          if idx == px.me {
            px.Accept(&accept_args, &accept_reply)
          } else {
            ok = call(peer, "Paxos.Accept", accept_args, &accept_reply)
          }

          if ok {
            if accept_reply.Ok {
              if px.testing {
                fmt.Printf("ACCEPT OK: Peer %d, seqnum %d, proposal %d, value %d\n", idx, args.Seqnum, n, args.V)
              }
              accept_ack++
            } else {
              if px.testing {
                fmt.Printf("ACCEPT REJECT: Peer %d, seqnum %d, proposal %d, value %d\n", idx, args.Seqnum, n, args.V)
              }
            }

            // update done vector
            px.mu.Lock()
            px.done[idx] = accept_reply.Done
            px.mu.Unlock()

          } else {
            if px.testing {
              fmt.Printf("ACCEPT FAILED: Peer %d, seqnum %d, proposal %d, value %d\n", idx, args.Seqnum, n, args.V)
            }
          }

        }

        if accept_ack >= px.majoritySize() {
          if px.testing {
            fmt.Printf("Majority (%d/%d) accepted proposer %d's value %d (n_a=%d), for seqnum %d at proposal %d\n", accept_ack, len(px.peers), px.me, max_v_a, max_n_a, args.Seqnum, n)
          }

          decided_args := DecidedArgs{}
          decided_args.Seqnum = args.Seqnum
          decided_args.V = max_v_a
          decided_args.N = n

          decided_reply := DecidedReply{}

           for idx := 0; idx < len(px.peers); idx++ {

            peer := px.peers[idx]

            ok := true
            if idx == px.me {
              px.Decided(&decided_args, &decided_reply)
            } else {
              ok = call(peer, "Paxos.Decided", decided_args, &decided_reply)
            }
            if ok {
              px.mu.Lock()
              px.done[idx] = decided_reply.Done
              px.mu.Unlock()
            }
          }

          // clean out old paxos instances.
          min := px.Min()
          px.mu.Lock()
          for k := range px.instances {
            if k < min {
              delete(px.instances, k)
            }
          }
          px.mu.Unlock()

          return
        }
      }
      px.randomSleep()
    }
  }()
  return nil
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  if seq < px.Min() {
    return
  }
  go func () {
    args := ProposeArgs{}
    args.Seqnum = seq
    args.V = v
    reply := ProposeReply{}
    px.Propose(&args, &reply)
  }()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  px.done[px.me] = seq
  px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  largest := -1
  for k, _ := range px.instances {
    if k > largest {
      largest = k
    }
  }
  return largest
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  maxInt := int(^uint(0) >> 1)
  min := maxInt
  for _, val := range px.done {
    if val < min {
      min = val
    }
  }
  if min == maxInt {
    return 0
  }
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  inst, _ := px.instances[seq]
  px.mu.Unlock()
  if seq < px.Min() || inst.is_decided == false {
    return false, nil
  }
  return true, inst.v_a
}

func (px *Paxos) GetPeers() []string {
  return px.peers
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {

  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.testing = false
  px.instances = map[int]PaxosInstance{}
  px.am_proposer = map[int]bool{}
  px.done = make([]int, len(peers))

  for idx, _ := range px.done {
    px.done[idx] = -1  // maximum int
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
