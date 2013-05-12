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
import "math/rand"


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

func StartServer(me string) *ViewServer {
	return StartMe(me, "unix")
}

// start the server
// actually modified, but just to add the modified fields, and it was getting annoying down below
func StartMe(me string, networkMode string) *ViewServer {

	vs := new(ViewServer)
	vs.me = me

	// set modified fields
	vs.view = View{}
	vs.criticalMassReached = false
	vs.serverPings = make(map[string] time.Time)
	vs.serversAlive = make(map[string] bool)
	vs.primaryServers = make(map[string] bool)
	vs.networkMode = networkMode

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	if networkMode == "unix" {
		os.Remove(vs.me)
	}

	l, e := net.Listen(networkMode, vs.me);

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
	networkMode string

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
	reply.View = vs.view
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
		if len(vs.serversAlive) >= CRITICAL_MASS {

			// if so, iterate through the live servers and make them primaries
			for server, _ := range vs.serversAlive {

				vs.primaryServers[server] = true

			}

			// convert vs.primaryServers to a slice in order to index into it
			primaryServersSlice := make([]string, len(vs.primaryServers))

			i := 0

			for primaryServer, _ := range vs.primaryServers {

				primaryServersSlice[i] = primaryServer

				i++

			}

			// create an initial view with shards distributed round-robin
			vs.view = View{ViewNumber: 1, ShardsToPrimaries: make(map[int] string)}

			for c := 0; c < NUMBER_OF_SHARDS; c++ {

				vs.view.ShardsToPrimaries[c] = primaryServersSlice[c % len(primaryServersSlice)]

			}

			// set vs.criticalMassReached to true so we never enter this block of code again
			vs.criticalMassReached = true

		}

		// return regardless; this simplifies the logic below as it's entirely independent
		return

	}

	// note that even if we dip below critical mass, we will keep skipping the previous block
	// it's just there to prevent the system from flooding the first live server with shards prematurely

	// keep track of the need for an intermediate view
	intermediateView := false

	// if primaries have died, update to an intermediate view so that clients don't contact dead primaries
	for deadServer, _ := range deadServers {

		for shard, primary := range vs.view.ShardsToPrimaries {

			if primary == deadServer {

				delete(vs.view.ShardsToPrimaries, shard)
				intermediateView = true

			} else {

				// deadServers should only contain dead primaries
				delete(deadServers, deadServer)

			}

		}

	}

	// if switched to an intermediate view, increment the ViewNumber
	if intermediateView {

		vs.view.ViewNumber++

	}

	// launch recovery for all dead primaries
	go vs.recover(deadServers)

}


// runs recovery for deadPrimaries
func (vs *ViewServer) recover(deadPrimaries map[string] bool) {

	// grab the vs lock until vs.serversAlive has been copied into a slice
	vs.mu.Lock()

	// convert vs.serversAlive to a slice in order to index into it
	serversAliveSlice := make([]string, len(vs.serversAlive))

	i := 0

	for serverAlive, _ := range vs.serversAlive {

		serversAliveSlice[serversAlivePerm[i]] = serverAlive

	}

	// release the vs lock
	vs.mu.Unlock()

	// ask every live server which shards and segments they have
	// note that if recovery can't be fully satisfied by the live servers,
	// it means all of the backups for at least 1 segment have failed, and there is nothing we can do
	// there is no point in checking if full recovery is satisfied because even if it isn't (due to failures), we can't satisfy it any more
	querySegmentsArgs := QuerySegmentsArgs{DeadPrimaries: deadPrimaries}
	// elect recovery masters in serversToShards
	serversToShards := make(map[string] (map[int] bool))
	// keep track of which segments are owned by which servers for each shard
	shardsToSegmentsToBackups := make(map[int] (map[int64] (map[string] bool)))

	// synchronize around the decision phases of the separate query threads
	var queryLock sync.Mutex
	// keep track of how many replies have finished so we can wait after
	queryRepliesFinished := 0
	// keep track of how many shards have been assigned so we can assign recovery masters
	shardsAssigned := 0

	for server, _ := range vs.serversAlive {

		go func() {

			querySegmentsReply := QuerySegmentsReply{}
			successful := call(server, "PBService.QuerySegments", querySegmentsArgs, &querySegmentsReply)

			queryLock.Lock()
			defer queryLock.Unlock()

			if successful {

				// for each successful reply, iterate through ShardsToSegments and populate shardsToSegmentsToBackups
				for shard, segments := range querySegmentsReply.ShardsToSegments {

					// make a map for the shard
					_, shardHasSegmentsBackups := shardsToSegmentsToBackups[shard]

					if !shardHasSegmentsBackups {

						shardsToSegmentsToBackups[shard] = make(map[int64] (map[string] bool))

					}

					// elect a recoveryMaster to recover the shard
					recoveryMaster := serversAliveSlice[shardsAssigned % len(serversAliveSlice)]

					_, recoveryMasterHasShards := serversToShards[recoveryMaster]

					if !recoveryMasterHasShards {

						serversToShards[recoveryMaster] = make(map[int] bool)

					}

					serversToShards[recoveryMaster][shard] = true

					// increment shardsAssigned for the next shard to be assigned to the next recoveryMaster
					shardsAssigned++

					for segment, _ := range segments {

						_, shardSegmentPairHasBackups = shardsToSegmentsToBackups[shard][segment]

						if !shardSegmentPairHasBackups {

							shardsToSegmentsToBackups[shard][segment] = make(map[string] bool)

						}

						shardsToSegmentsToBackups[shard][segment][querySegmentsReply.ServerName] = true

					}

				}

			}

			queryRepliesFinished++

		}()

	}

	// wait until all of the threads have returned
	for {

		time.Sleep(QUERY_SEGMENTS_SLEEP_INTERVAL)

		queryLock.Lock()

		if queryRepliesFinished >= len(serversAliveSlice) {

			queryLock.Unlock()

			break

		}

		queryLock.Unlock()

	}

	// send shardsToSegmentsToBackups to each of the recovery masters in serversToShards
	for recoveryMaster, recoveryShards := range serversToShards {

		recoveryData := make(map[int] (map[int64] (map[string] bool)))

		for recoveryShard, _ := range recoveryShards {

			recoveryData[recoveryShard] = shardsToSegmentsToBackups[recoveryShard]

		}

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
	// our failure model only deals with simultaneous primary server failure
	// we also don't support moving shards except for recovery
	// under these circumstances, we have perfect consistency because only one primary is in charge of each shard until it dies
	// when dead servers come back up, they can become log segment backups or primaries for future dead servers

	vs.view.ShardsToPrimaries[args.ShardRecovered] = args.ServerName
	vs.view.ViewNumber++

	return nil

}


/*
********************
Utility Functions
********************
*/
