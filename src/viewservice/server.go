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

// kill the server in tests
func (vs *ViewServer) Kill() {

	vs.dead = true
	vs.l.Close()

}


// start the server
// actually modified, but just to add the modified fields, and it was getting annoying down below
func StartServer(me string) *ViewServer {

	vs := new(ViewServer)
	vs.me = me

	// set modified fields
	vs.view = View{}
	vs.criticalMassReached = false
	vs.serverPings = make(map[string] time.Time)
	vs.serversAlive = make(map[string] bool)
	vs.primaryServers = make(map[string] bool)

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
	view View
	criticalMassReached bool				// minimum number of servers/primaries reached
	
	// server states
	serverPings map[string] time.Time		// all servers including primaries, backups, and unused
	serversAlive map[string] bool			// all servers which can currently communicate with the viewservice
	primaryServers map[string] bool			// tracks which servers are primaries

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
	reply.View = vs.view

	return nil

}


// updates server states
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// grab the vs lock for the duration of the method
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// update the last ping and liveness for the sender
	vs.serverPings[args.ServerName] = time.Now()
	vs.serversAlive[args.ServerName] = true
	
	// reply with the current view and serversAlive
	reply.ViewNumber = vs.view.viewNumber
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
			
			// convert vs.primaryServers to a slice in order to index into it
			primaryServersSlice := make([]string, len(vs.primaryServers)
			
			i := 0
			
			for primaryServer, _ := range vs.primaryServers {
			
				primaryServersSlice[i] = primaryServer
			
			}
			
			// create an initial view with shards distributed round-robin
			vs.view = View{viewNumber: 1, shardsToPrimaries: make([]string, NUMBER_OF_SHARDS), serversAlive: vs.serversAlive}
			
			for (c := 0; c < NUMBER_OF_SHARDS; ++c) {
			
				vs.view.shardsToPrimaries[c] = primaryServersSlice[c % len(primaryServersSlice)]
			
			}
			
			// set vs.criticalMassReached to true so we never enter this block of code again
			vs.criticalMassReached = true
		
		}
	
	}
	
	// note that even if we dip below critical mass, we will keep skipping the previous block
	// it's just there to prevent the system from flooding the first live server with shards prematurely
	
	// launch recovery for all dead primaries
	for deadServer, _ := range deadServers {
	
		if vs.view.shardsToPrimaries[deadServer] {
		
			go vs.Recover(deadServer)
		
		}
	
	}

}


// runs recovery for deadServer's shards
func (vs *ViewServer) Recover(deadServer string) {

	// grab the vs lock
	vs.mu.Lock()

	// figures out which shards are owned by deadServer
	shardsToRecover := make(map[int] bool)
	
	for shard, server := range vs.view.shardsToPrimaries {
	
		if server == deadServer {
		
			shardsToRecover[shard] = true
		
		}
	
	}
	
	// ask every live server which shards and segments they have
	querySegmentsArgs := QuerySegmentsArgs{ShardsToRecover: shardsToRecover}
	querySegmentsReplies := make([]QuerySegmentsReply, len(vs.serversAlive))
	// the earlier we stop using vs fields, the earlier we can release the lock

	// release the vs lock
	vs.mu.Unlock()

	querySegmentsRepliesSuccesful := make([]bool, len(querySegmentsReplies))
	
	i := 0
	querySegmentsRepliesFinished := 0
	querySegmentsRepliesFinishedLock sync.Mutex
	
	for server, _ := range vs.serversAlive {
	
		go func(j int) {
		
			querySegmentsRepliesFinishedLock.Lock()
			defer querySegmentsRepliesFinishedLock.Unlock()
		
			querySegmentsRepliesepliesSuccesful[j] = call(server, "PBService.QuerySegments", querySegmentsArgs, &querySegmentsReplies[j])
			querySegmentsRepliesFinished++
		
		}(i)
		
		i++
	
	}
	
	// wait until all of the threads have returned
	for {
	
		time.Sleep(QUERY_SEGMENTS_SLEEP_INTERVAL)
		
		querySegmentsRepliesFinishedLock.Lock()
		
		if querySegmentsRepliesFinished >= len(querySegmentsReplies) {
		
			querySegmentsRepliesFinished.Unlock()
			
			break
		
		}
		
		querySegmentsRepliesFinishedLock.Unlock()
	
	}
	
	// keep track of which segments are owned by which servers for each shard
	shardsToSegmentsToServers := make(map[int] (map[LogSegmentID] (map[string] bool)))
	
	for shard, _ := range shardsToRecover {
	
		shardsToSegmentsToServers[shard] := make(map[LogSegmentID]) (map[string] bool))
	
	}
	
	// for each successful reply, iterate through ShardsToSegments and populate shardsToSegmentsToServers
	for (i = 0; i < len(querySegmentsReplies); i++) {
	
		if querySegmentsReplySuccessful[i] {
		
			for shard, segments := range querySegmentsReplies[i].ShardsToSegments {
			
				for logSegmentID, _ := range segments {
				
					_, present := shardsToSegmentsToServers[shard][logSegmentID]
			
					if !present {
				
						shardsToSegmentsToServers[shard][logSegmentID] = make(map[string] bool)
				
					}
				
					shardsToSegmentsToServers[shard][logSegmentID][querySegmentsReplies[i].ServerName] = true
				
				}
			
			}
		
		}
	
	}
	
	// grab the vs lock
	vs.mu.Lock()
	
	// convert vs.serversAlive to a slice in order to index into it
	serversAliveSlice := make([]string, len(vs.serversAlive))
	
	i := 0
	
	for serverAlive, _ := range vs.serversAlive {
	
		serversAliveSlice[i] = serverAlive
	
	}
	
	// release the vs lock
	vs.mu.Unlock()
	
	// elect recovery masters in serversToShardsToSegmentsToServers
	serversToShardsToSegmentsToServers := make(map[string] (map[int] (map[LogSegmentID] (map[string] bool))))
	
	c := 0
	
	for shard, segmentsToServers := range shardsToSegmentsToServers {
	
		_, present := serversToShardsToSegmentsToServers[serversAliveSlice[c % len(serversAliveSlice)]]
		
		if !present {
		
			serversToShardsToSegmentsToServers[serversAliveSlice[c % len(serversAliveSlice)]] = make(map[int] (map[LogSegmentID] (map[string] bool)))
		
		}
		
		serversToShardsToSegmentsToServers[serversAliveSlice[c % len(serversAliveSlice)]][shard] = segmentsToServers
		
		c++
	
	}
	
	// send shardsToSegmentsToServers to each of the recovery masters in serversToShardsToSegmentsToServers
	for recoveryMaster, recoveryData := range serversToShardsToSegmentsToServers {
	
		electRecoveryMasterArgs := ElectRecoveryMasterArgs{ShardsToSegmentsToServers: recoveryData}
		electRecoveryMasterReply := ElectRecoveryMasterReply{}
		
		go call(recoveryMaster, "PBService.ElectRecoveryMaster", electRecoveryMasterArgs, &electRecoveryMasterReply)
	
	}

}


// updates the view after recovery of a shard is successful
func (vs *ViewServer) RecoveryCompleted(args *RecoveryCompletedArgs, reply *RecoveryCompletedReply) error {

	// grab the vs lock for the duration of the method
	vs.mu.Lock()
	defer vs.mu.Unlock()
	
	// notice this is a much simpler model than lab 2
	// there is no acknowledgement of views stored
	// our failure model only deals with a single primary server failing
	// we also don't support moving shards except for recovery
	// under these circumstances, we have perfect consistency because only one primary is in charge of each shard until it dies
	// when dead servers come back up, they can become log segment backups or primaries for future dead servers

	// inject all of the reassigned shards into the view and increment vs.view.viewNumber
	for _, shard := range args.ShardsRecovered {
	
		vs.view.shardsToPrimaries[shard] = args.ServerName
	
	}
	
	vs.view.viewNumber++

	return nil

}


/*
********************
Utility Functions
********************
*/
