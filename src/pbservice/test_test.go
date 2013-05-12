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
  s += "viewserver-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += suffix
  return s
}

func Test1(t *testing.T) {
  runtime.GOMAXPROCS(4)

  vshost := port("v")
  vs := viewservice.StartServer(vshost)

  numOfClients := 5
  numOfServers := 10

  clients := make([]*Clerk, numOfClients)
  servers := make([]*PBServer, numOfServers)

  for i:=0; i < numOfServers; i++ {
    hostname := port(fmt.Sprintf("client%d", i))
    servers[i] = StartServer(hostname, vshost)
  }

  for i:=0; i < numOfClients; i++ {
    clients[i] = MakeClerk(port(fmt.Sprintf("client%d", i)), vshost)
  }

  time.Sleep(1 * time.Second)

  vs.Kill()

}