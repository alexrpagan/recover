package pbservice

import (
  "viewservice"
  "testing"
  "runtime"
  "time"
  "fmt"
  "os"
  "strconv"
  "math/rand"
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

func hostname(base string, hosts map[int]bool) string {
  i := 0
  for {
    port := int(rand.Int63() % 10000)
    if port > 1000 && !hosts[port] {
      i = port
      hosts[port] = true
      break
    }
  }
  return fmt.Sprintf("%s:%d", base, i)
}


func Test1(t *testing.T) {
  runtime.GOMAXPROCS(4)

  mode := "unix"
  // mode := "tcp"

  // localhost := "127.0.0.1"
  // names := make(map[int]bool)

  vshost := port("vs")
  // vshost := hostname(localhost, names)

  vs := viewservice.StartMe(vshost, mode)

  numOfClients := 5
  numOfServers := 8

  clients := make([]*Clerk, numOfClients)
  servers := make([]*PBServer, numOfServers)

  for i:=0; i < numOfServers; i++ {
    hostname := port(fmt.Sprintf("server%d", i))
    // hostname := hostname(localhost, names)
    servers[i] = StartMe(hostname, vshost, mode)
  }

  for i:=0; i < numOfClients; i++ {
    hostname := port(fmt.Sprintf("client%d", i))
    // hostname := hostname(localhost, names)
    clients[i] = MakeClerk(hostname, vshost, mode)
  }

  iters := 300

  times := make([]int64, iters)

  //round of puts
  for i:=0; i < iters; i++ {
    go func(i int) {
      valbase := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      t1 := time.Now().UnixNano()
      clients[0].Put(fmt.Sprintf("%d", i % 10), fmt.Sprintf("%s%d",valbase, i))
      t2 := time.Now().UnixNano()
      times[i] = t2-t1
    }(i)
    time.Sleep(10 * time.Millisecond)
  }
  printStats(times)

  // for i:=0; i < iters; i++ {
  //   t1 := time.Now().UnixNano()
  //   clients[0].Get(fmt.Sprintf("%d", i % 10))
  //   t2 := time.Now().UnixNano()
  //   times[i] = t2-t1
  // }
  // printStats(times)

  servers[0].kill()
  servers[3].kill()

  time.Sleep(10 * time.Second)

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

  fmt.Printf("Avg time (micros) %d\n", sum/int64(n*1000))
  fmt.Printf("Min %d\n", min/int64(1000))
  fmt.Printf("Max %d\n", max/int64(1000))

}