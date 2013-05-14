/*
*********************
Package and Imports
*********************
*/


package viewservice

import (
	"net"
	"net/rpc"
	"log"
	"time"
	"sync"
	"fmt"
	"os"
	"strings"
	"sort"
)


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
	recoveryInProcess map[string][]int

	//keep track of which shards need to be recovered
	recoveryMasters map[string]map[int]bool
	recoveryTimes map[string] time.Time

	networkMode string

}


// replies with current view
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// reply with the current view
	reply.View = vs.view

	return nil

}


// updates server states
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// update the last ping and liveness for the sender
	vs.serverPings[args.ServerName] = time.Now()
	vs.serversAlive[args.ServerName] = true

	reply.View = vs.view
	reply.ServersAlive = vs.serversAlive

	return nil

}


// tick cleans data structures, manages critical mass, updates views, and launches recovery
func (vs *ViewServer) tick() {
	vs.mu.Lock()

	newFailures := make(map[string][]int)

	// keep track of the need for an intermediate view
	intermediateView := false

	// liveness check
	for server, lastPingTime := range vs.serverPings {
		if time.Since(lastPingTime) >= PING_INTERVAL * DEAD_PINGS {

			// are we already working on recovering this guy?
			_, inProcess := vs.recoveryInProcess[server]
			if inProcess {
				// TODO: see if the recovery master is also alive?
				continue
			}

			// record recovery information
			shardsOwned := make([]int, 0)

			// based on previous view, which shards do we need to recover?
			for shard, primary := range vs.view.ShardsToPrimaries {
				if primary == server {
					shardsOwned = append(shardsOwned, shard)
					delete(vs.view.ShardsToPrimaries, shard)
					intermediateView = true
				}
			}

			if len(shardsOwned) > 0 {
				newFailures[server]          = shardsOwned
				vs.recoveryInProcess[server] = shardsOwned
			}

			delete(vs.serversAlive, server)
		}
	}

	if ! vs.criticalMassReached {

		if len(vs.serversAlive) >= CRITICAL_MASS {

			for server, _ := range vs.serversAlive {
				vs.primaryServers[server] = true
			}

			primaryServersSlice := make([]string, len(vs.primaryServers))

			i := 0
			for primaryServer, _ := range vs.primaryServers {
				primaryServersSlice[i] = primaryServer
				i++
			}

			sort.Strings(primaryServersSlice)

			// create an initial view with shards distributed round-robin
			vs.view = View{ViewNumber: 1, ShardsToPrimaries: make(map[int] string)}

			for c := 0; c < NUMBER_OF_SHARDS; c++ {
				vs.view.ShardsToPrimaries[c] = primaryServersSlice[c % len(primaryServersSlice)]
			}

			fmt.Println("Reached critical mass ", vs.primaryServers)
			vs.criticalMassReached = true
		}

		vs.mu.Unlock()
		return
	}

	// if switched to an intermediate view, increment the ViewNumber
	if intermediateView {
		vs.view.ViewNumber++
	}

	vs.mu.Unlock()

	if len(newFailures) > 0 {
		// kick off recovery
		go vs.recover(newFailures)
	}

}


// runs recovery for deadPrimaries
func (vs *ViewServer) recover(deadPrimaries map[string][]int) {

	fmt.Println("Recovery initiated ", deadPrimaries)
	fmt.Println("")

	vs.mu.Lock()
	serversAliveCpy := make([]string, len(vs.serversAlive))
	i := 0
	for serverAlive, _ := range vs.serversAlive {
		serversAliveCpy[i] = serverAlive
		i++
	}
	vs.mu.Unlock()

	querySegArgs := QuerySegmentsArgs{}
	querySegArgs.DeadPrimaries = deadPrimaries

	var wg sync.WaitGroup

	numLiveServers := len(serversAliveCpy)
	queryReplies   := make([]*QuerySegmentsReply, numLiveServers)
	acks 				   := make([]bool, numLiveServers)

	// query all live servers to see if they back up shards for failed primaries
	for idx, server := range serversAliveCpy {
		wg.Add(1)
		go func(idx int, server string) {
			querySegReply := new(QuerySegmentsReply)
			acks[idx] = call(server, "PBServer.QuerySegments", vs.networkMode, querySegArgs, querySegReply)
			queryReplies[idx] = querySegReply
			wg.Done()
		}(idx, server)
	}
	wg.Wait()

	// for each shard, which segments does it need and where are they each located?
	shrdToSegToSrv := make(map[int]map[int64][]string)

	// run through replies from potential backups and figure out what useful data each has
	for i:=0; i < numLiveServers; i++ {

		if acks[i] {

			for _, segsToShards := range queryReplies[i].BackedUpSegments {

				for segment, shards := range segsToShards {

					for shard, _ := range shards {

						// make sure that all levels of shrdToSegToSrv are init'd
						_, shardok := shrdToSegToSrv[shard]
						if ! shardok {
							shrdToSegToSrv[shard] = make(map[int64][]string)
						}

						_, segok := shrdToSegToSrv[shard][segment]
						if ! segok {
							shrdToSegToSrv[shard][segment] = make([]string, 0)
						}

						shrdToSegToSrv[shard][segment] = append(shrdToSegToSrv[shard][segment], serversAliveCpy[i])

					}

				}

			}

		}

	}

	// TODO: check to make sure that list of shards is complete

	// assign shards to recover to recovery masters
	shardToAssign  := 0

	// recovery master host -> shards to recover
	recoveryMasters := make(map[string][]int)

	if len(serversAliveCpy) == 0 {
		fmt.Println("No servers alive; nothing to do.")
		return
	}

	for _, shards := range deadPrimaries {

		for _, shard := range shards {

			// round robin assignment to recovery masters

			// TODO: restrict number of RM's
			// TODO: load balancing

			recoveryMaster := serversAliveCpy[shardToAssign % len(serversAliveCpy)]
			shardToAssign++

			_, ok := recoveryMasters[recoveryMaster]
			if ! ok {
				recoveryMasters[recoveryMaster] = make([]int, 0)
			}

			recoveryMasters[recoveryMaster] = append(recoveryMasters[recoveryMaster], shard)
		}

	}

	// TODO: record recovery masters for in-progress recoveries.

	for recoveryMaster, recoveryShards := range recoveryMasters {

		// relevant subset of shrdToSegToSrv
		recoveryData := make(map[int]map[int64][]string)
		for _, shard := range recoveryShards {
			recoveryData[shard] = shrdToSegToSrv[shard]

			// keep track of shards that this guy is supposed to recover
			vs.mu.Lock()
			shards, ok := vs.recoveryMasters[recoveryMaster]
			if ! ok {
				shards = make(map[int]bool)
			}
			shards[shard] = true
			vs.recoveryMasters[recoveryMaster] = shards
			vs.recoveryTimes[recoveryMaster] = time.Now()
			vs.mu.Unlock()

		}

		electionArgs  := new(ElectRecoveryMasterArgs)
		electionReply := new(ElectRecoveryMasterReply)
		electionArgs.RecoveryData = recoveryData
		electionArgs.DeadPrimaries = deadPrimaries

		go call(recoveryMaster, "PBServer.ElectRecoveryMaster", vs.networkMode, electionArgs, electionReply)

	}



}


// updates the view after recovery of a shard is successful
func (vs *ViewServer) RecoveryCompleted(args *RecoveryCompletedArgs, reply *RecoveryCompletedReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	fmt.Printf("Recovered shard %d from server %s \n", args.ShardRecovered, args.ServerName)

	shards, ok := vs.recoveryMasters[args.ServerName]
	if ! ok {
		panic("Recovery master has no shards to recover. Should never happen.")
	}

	delete(shards, args.ShardRecovered)
	if len(shards) == 0 {
		delete(vs.recoveryMasters, args.ServerName)
	}

	vs.view.ShardsToPrimaries[args.ShardRecovered] = args.ServerName
	vs.view.ViewNumber++

	if len(vs.recoveryMasters) == 0 {
		fmt.Println("recovery completed in ", time.Since(vs.recoveryTimes[args.ServerName]))
		fmt.Printf("recovery master recieved %d bytes of data\n", args.DataRecieved)
		delete(vs.recoveryTimes, args.ServerName)
	}

	return nil

}


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
	vs.recoveryInProcess = make(map[string][]int)
	vs.recoveryMasters = make(map[string]map[int]bool)
	vs.recoveryTimes = make(map[string] time.Time)

	vs.networkMode = networkMode

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

  	hostname := vs.me
	if networkMode == "unix" {
    	os.Remove(hostname)
  	} else if networkMode == "tcp" {
    	arr := strings.Split(hostname, ":")
    	hostname = ":" + arr[1]
  	}

	l, e := net.Listen(networkMode, hostname);

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
