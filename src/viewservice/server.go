/*
*********************
Package and Imports
*********************
*/


package viewservice


import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"


/*
*********************
Unmodified Structures
*********************
*/


/*
*********************
Unmodified Code
*********************
*/


func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}


/*
*********************
Modified Structures
*********************
*/


type ViewServer struct {

	mu sync.Mutex
	l net.Listener
	dead bool
	me string

	// view state
	currentView View
	viewPending bool
	pendingView View
	criticalMassReached bool				// minimum number of servers/primaries reached
	
	// server states
	serverPings map[string] time.Time		// all servers including primaries, backups, and unused
	serversAlive map[string] alive			// all servers which can currently communicate with the viewservice
	primaryServers map[string] bool			// tracks which servers are primaries
	viewsAcknowledged map[string] uint		// tracks latest views acknowledged by primaries

}


/*
*********************
Modified Code
*********************
*/


// replies with current view
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// grab the vs lock for the duration of the method
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// reply with the current view
	reply.CurrentView = vs.currentView

	return nil

}


// updates server states
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// grab the vs lock for the duration of the method
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// update the last ping, highest acknowledged view, and liveness for the sender
	vs.serverPings[args.ServerName] = time.Now()
	vs.viewsAcknowledged[args.ServerName] = args.ViewNumber
	vs.serversAlive[args.ServerName] = true
	
	// reply with the current view and serversAlive
	reply.CurrentView = vs.currentView
	reply.ServersAlive = vs.serversAlive

	return nil

}


// tick cleans data structures, manages critical mass, updates views, and launches recovery
func (vs *ViewServer) tick() {

	// grab the vs lock for the duration of the method
	vs.mu.Lock()
	defer vs.mu.Unlock()
	
	// keeps track of servers which have died for use in launching recovery
	deadServers := make(map[string] bool)
	
	// remove dead servers from vs.serversAlive and vs.serverPings, and remember dead servers
	// we have to clean these data structures even before reaching critical mass
	for server, lastPingTime := range vs.serverPings {
	
		// don't have to worry about registering live servers because Ping() does it
		if time.Since(lastPingTime) >= PING_INTERVAL * DEAD_PINGS {
		
			deadServers[server] = true
			delete(vs.serversAlive, server)
			delete(vs.serverPings, server)
		
		}
	
	}

	// if we haven't reached the critical mass...
	if !vs.criticalMassReached {
	
		// ...check if we now have enough servers
		if len(vs.serversAlive >= CRITICAL_MASS) {
			
			// if so, iterate through the live servers and make them primaries
			for server, _ := range vs.serversAlive {
			
				vs.primaryServers[server] = true
			
			}
			
			// create an initial view with shards distributed round-robin
			vs.currentView = View{viewNumber: 1, shardsToPrimaries: make([]string, NUMBER_OF_SHARDS), serversAlive: vs.serversAlive}
			
			for (c = 0; c < NUMBER_OF_SHARDS; ++c) {
			
				vs.currentView.shardsToPrimaries[c] = vs.primaryServers[c % len(vs.primaryServers)]
			
			}
			
			// set vs.criticalMassReached to true so we never enter this block of code again
			vs.criticalMassReached = true
		
		}
		
		// return out of this regardless of outcome
		// this just simplifies the logic so we can consider this block separately from the code below
		return
	
	}
	
	// note that even if we dip below critical mass, we will keep skipping the previous block
	// it's just there to prevent the system from flooding the first live server with shards prematurely
	
	// check if a view is pending and switch to it
	if vs.viewPending {
	
		vs.viewPending = false
		vs.currentView = vs.pendingView
	
	}
	
	// launch recovery for all dead primaries
	for deadServer, _ := range deadServers {
	
		if vs.currentView.shardsToPrimaries[deadServer] {
		
			go vs.Recover(deadServer)
		
		}
	
	}

}


// runs recovery for deadServer's shards
func (vs *ViewServer) Recover(deadServer string) {

	// grab the vs lock for the duration of the method
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// figures out which shards are owned by deadServer
	shardsToRecover := make(map[int] bool)
	
	for shard, server := range vs.currentView.shardsToPrimaries {
	
		if server == deadServer {
		
			shardsToRecover[shard] = true
		
		}
	
	}
	
	// ask every live server which shards they have
	args := QueryRangesArgs{ShardsToRecover: shardsToRecover}
	replies := make([]QueryRangesReply, len(vs.serversAlive))
	// the earlier we stop using len(vs.*), the earlier we can release the lock
	repliesSuccesful := make([]bool, len(replies))
	
	i := 0
	repliesFinished := 0
	repliesFinishedLock sync.Mutex
	
	for server := range vs.serversAlive {
	
		go func() {
		
			repliesFinishedLock.Lock()
			defer repliesFinishedLock.Unlock()
		
			repliesSuccesful[i] = call(server, "PBService.QueryRanges", args, &replies[i])			
			repliesFinished++
		
		}
		
		i++
	
	}
	
	// wait until all of the threads have returned
	for {
	
		time.Sleep(QUERY_RANGES_SLEEP_INTERVAL)
		
		repliesFinishedLock.Lock()
		
		if repliesFinished >= len(replies) {
		
			repliesFinished.Unlock()
			
			break
		
		}
		
		repliesFinishedLock.Unlock()
	
	}
	
	// keep track of which servers have which ranges
	serverRanges := make(map[string] []Range)
	
	for i = 0; i < len(replies); i++ {
	
		if repliesSuccesful[i] {
		
			replies
		
		}
	
	}

}


func StartServer(me string) *ViewServer {

  vs := new(ViewServer)
  vs.me = me

	// set modified fields
	vs.currentView = View{}
	vs.viewPending = false
	vs.pendingView = View{}
	vs.criticalMassReached = false
	vs.serverPings = make(map[string] time.Time)
	vs.serversAlive map[string]
	vs.viewsAcknowledged = make(map[string] uint)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PING_INTERVAL)
    }
  }()

  return vs
}


/*
********************
Utility Functions
********************
*/
