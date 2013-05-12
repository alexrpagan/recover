package pbservice

import (
  "viewservice"
  "testing"
  "runtime"
  "time"
  "fmt"
  "os"
  "strconv"
)

func port(suffix string) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "pbtest-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += suffix
  return s
}


func Test1(t *testing.T) {
  runtime.GOMAXPROCS(8)

  vshost := port("v")
  vs := viewservice.StartServer(vshost)

  numOfClients := 5
  numOfServers := 6

  clients := make([]*Clerk, numOfClients)
  servers := make([]*PBServer, numOfServers)

  for i:=0; i < numOfServers; i++ {
    hostname := port(fmt.Sprintf("servers%d", i))
    servers[i] = StartServer(hostname, vshost)
  }

  for i:=0; i < numOfClients; i++ {
    clients[i] = MakeClerk(port(fmt.Sprintf("client%d", i)), vshost)
  }

  iters := 200

  times := make([]int64, iters)

  //round of puts
  for i:=0; i < iters; i++ {
    t1 := time.Now().UnixNano()
    clients[0].Put(fmt.Sprintf("%d", i % 10), fmt.Sprintf("gibberish%d", i % 10))
    t2 := time.Now().UnixNano()
    times[i] = t2-t1
  }
  printStats(times)

  for i:=0; i < iters; i++ {
    t1 := time.Now().UnixNano()
    clients[0].Get(fmt.Sprintf("%d", i % 10))
    t2 := time.Now().UnixNano()
    times[i] = t2-t1
  }
  printStats(times)

  vs.Kill()

  for i:=0; i < numOfServers; i++ {
    servers[i].kill()
  }

}

func printStats(samples []int64) {
  var sum int64 = 0
  var min int64 = 1<<63 - 1
  var max int64 = -(1<<63)

  n := len(samples)

  for _, val := range samples {
    sum += val
    if val > max {
      max = val
    }
    if val < min {
      min = val
    }
  }

  fmt.Printf("Avg time %d\n", sum/int64(n*1000))
  fmt.Printf("Min %d\n", min/int64(1000))
  fmt.Printf("Max %d\n", max/int64(1000))

}