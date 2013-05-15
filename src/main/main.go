package main

import (
  "flag"
  "fmt"
  "runtime"
  "runtime/pprof"
  "viewservice"
  "pbservice"
  "os"
  "time"
  "math/rand"
  "bytes"
  "bufio"
  "strings"
  "path"
)

var vs_idx = 0

var mode = "tcp"
var port = ":5000"

// turn on profiling
var cpuprofile = flag.String("prof", "", "write cpu profile to file")
var repl       = flag.Bool("repl", false, "run a repl")
var me         = flag.Int("me", -1, "who am I")
var bench      = flag.Int("bench", -1, "run a benchmark")
var hostfile   = flag.String("hosts", "", "File containing the names of servers in the cluster")


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

func reportError(error interface{}) {
  fmt.Printf("Error opening hosts file: %v", error)
}

func readHosts() []string {
  hosts := make([]string, 0)

  var filepath string

  if *hostfile == "" {
    cwd, err := os.Getwd()
    if err != nil {
      reportError(err)
    }
    filepath = path.Join(cwd, "../../scripts/servers")
  } else {
    filepath = *hostfile
  }

  f, err := os.Open(filepath)
  if err != nil {
    reportError(err)
  }
  reader := bufio.NewReader(f)
  for {
    str, err := reader.ReadString('\n')
    if err != nil {
      break
    }
    hosts = append(hosts, str)
  }
  return hosts
}

func main() {

  runtime.GOMAXPROCS(8)

  flag.Parse()
  block := make(chan int)

  if *cpuprofile != "" {
    f, _ := os.Create(*cpuprofile)
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }

  hosts := readHosts()

  if *repl {
    reader := bufio.NewReader(os.Stdin)
    ck := pbservice.MakeClerk("", hosts[0] + port, mode)
    for {
      fmt.Printf("> ")
      str, err := reader.ReadString('\n')
      if err == nil {
        input := strings.Fields(str)
        if len(input) > 0 {
          switch strings.ToUpper(input[0]) {
          case "GET":
            if len(input) == 2 {
              t1 := time.Now().UnixNano()
              val := ck.Get(input[1])
              t2 := time.Now().UnixNano()
              fmt.Println(val)
              fmt.Println("(ran in %d ms)", float32(t2-t1)/float32(1000 * 1000))
            }
          case "PUT":
            if len(input) == 3 {
              // TODO: error handling
              t1 := time.Now().UnixNano()
              ck.Put(input[1], input[2])
              t2 := time.Now().UnixNano()
              fmt.Println("(ran in %d ms)", float32(t2-t1)/float32(1000 * 1000))
            }
          case "STATUS":
            fmt.Println("status")
          case "KILL":
            fmt.Println("status")
          case "QUIT":
            os.Exit(0)
          }
        }
      }
    }
  }


  if *bench >= 0 {

    fmt.Println("running benchmark ", *bench)

    ck := pbservice.MakeClerk("", hosts[0] + port, mode)
    iters := 20000
    times := make([]int64, iters)

    switch *bench {
    case 0:
      //round of puts
      var buffer bytes.Buffer
      for i:=0; i < 8 * 1024 * 100; i++ {
        buffer.WriteString("a")
      }
      valbase := buffer.String()

      for i:=0; i < iters; i++ {
        t1 := time.Now().UnixNano()
        ck.Put(fmt.Sprintf("%d", i), fmt.Sprintf("%c%s",48 + rand.Intn(122-48), valbase))
        t2 := time.Now().UnixNano()
        times[i] = t2-t1
        if i % 100 == 0 {
          fmt.Println("finished ", i)
        }
      }
      printStats(times)

    case 1:
      for i:=0; i < iters; i++ {
        t1 := time.Now().UnixNano()
        val := ck.Get(fmt.Sprintf("%d", i % 10))
        if len(val) > 0 && val != "errnokey" {
          //no op
        } else {
          fmt.Println("failure!")
        }
        if i % 100 == 0 {
          fmt.Println("finished ", i)
        }
        t2 := time.Now().UnixNano()
        times[i] = t2-t1
      }
      printStats(times)
    }

    block <- 1
  }


  if *me == 0 {

    go func() {
      viewservice.StartMe(hosts[0] + port, mode)
    }()

  } else if *me > 0 && *me < len(hosts) {

    go func() {
      pbservice.StartMe(hosts[*me] + port, hosts[0] + port, mode)
    }()

  } else {

    go func() {
      fmt.Println("Not a valid host.")
      block <- 1
    }()

  }

  <- block
}