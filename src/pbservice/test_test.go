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
  "bytes"
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

  // mode := "unix"
  mode := "tcp"

  localhost := "127.0.0.1"
  names := make(map[int]bool)

  // vshost := port("vs")
  vshost := hostname(localhost, names)

  vs := viewservice.StartMe(vshost, mode)

  numOfClients := 5
  numOfServers := 8

  clients := make([]*Clerk, numOfClients)
  servers := make([]*PBServer, numOfServers)

  for i:=0; i < numOfServers; i++ {
    // hostname := port(fmt.Sprintf("server%d", i))
    hostname := hostname(localhost, names)
    servers[i] = StartMe(hostname, vshost, mode)
  }

  for i:=0; i < numOfClients; i++ {
    // hostname := port(fmt.Sprintf("client%d", i))
    hostname := hostname(localhost, names)
    clients[i] = MakeClerk(hostname, vshost, mode)
  }

  // allow some time for critical mass to be reached
  time.Sleep(1 * time.Second)

  fmt.Println("sending out some puts")

  iters := 1000

  times := make([]int64, iters)

  var buffer bytes.Buffer
  for i:=0; i < 8 * 1024 ; i++ {
    buffer.WriteString("a")
  }
  valbase := buffer.String()

  for round:=0; round < 1; round++ {
    //round of puts
    for i:=0; i < iters; i++ {
      t1 := time.Now().UnixNano()
      clients[0].Put(fmt.Sprintf("%d", i), fmt.Sprintf("%c%s",48 + rand.Intn(122-48), valbase))
      t2 := time.Now().UnixNano()
      times[i] = t2-t1

      if i % 100 == 0 {
        fmt.Println("finished ", i)
      }
    }
    time.Sleep(1 * time.Second)
  }

  printStats(times)

  // for i:=0; i < iters; i++ {
  //   t1 := time.Now().UnixNano()
  //   clients[0].Get(fmt.Sprintf("%d", i % 10))
  //   t2 := time.Now().UnixNano()
  //   times[i] = t2-t1
  // }
  // printStats(times)

  // single failure
  servers[1].kill()

  time.Sleep(50 * time.Second)

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