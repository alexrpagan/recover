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
  "strconv"
)

var vs_idx = 0

var mode = "tcp"
var kvport = ":5000"
var vsport = ":5001"

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

func reportTiming(timing int64) {
  fmt.Printf("[completed in %d ms]\n", float32(timing)/float32(1000 * 1000))
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
    hosts = append(hosts, strings.Trim(str, " \n"))
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

  vshostname := hosts[0] + vsport

  if *repl {
    reader := bufio.NewReader(os.Stdin)
    ck := pbservice.MakeClerk("", vshostname, mode)
    timing := false  // show timing stats
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
              if timing {
                reportTiming(t2-t1)
              }
            }
          case "PUT":
            if len(input) == 3 {
              t1 := time.Now().UnixNano()
              ck.Put(input[1], input[2])
              t2 := time.Now().UnixNano()
              if timing {
                reportTiming(t2-t1)
              }
            }
          case "TIMING":
            timing := !timing
            if timing {
              fmt.Println("Timing enabled.")
            } else {
              fmt.Println("Timing disabled.")
            }
          case "SHOWSHARD":
            if len(input) == 2 {
              fmt.Printf("Key '%s' belongs to shard %d.\n", input[1], ck.WhichShard(input[1]))
            }
          case "VIEW":
            view := ck.GetView()
            if len(input) == 1 {
              fmt.Println(view)
            } else {
              shard, err := strconv.Atoi(input[1])
              if err == nil {
                fmt.Println(view.ShardsToPrimaries[shard])
              }
            }
          case "STATUS":
            fmt.Println(ck.Status().ServersAlive)
          case "KILL":
            fmt.Println("kill")
          case "QUIT":
            os.Exit(0)
          }
        }
      }
    }
    os.Exit(0)
  }


  if *bench >= 0 {

    fmt.Println("running benchmark ", *bench)

    ck := pbservice.MakeClerk("", vshostname, mode)
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

    os.Exit(0)
  }


  if *me == 0 {

    go func() {

      // NODE 0 is special: start the viewserver too
      fmt.Println("Starting Viewserver on ", vshostname)
      viewservice.StartMe(vshostname, mode)

      hostname := hosts[*me] + kvport
      fmt.Println("Starting KV Server on ", hostname)
      pbservice.StartMe(hostname, vshostname, mode)

    }()

  } else if *me > 0 && *me < len(hosts) {

    go func() {
      hostname := hosts[*me] + kvport
      fmt.Println("Starting KV Server on ", hostname)
      pbservice.StartMe(hostname, vshostname, mode)
    }()

  } else {

    go func() {
      fmt.Println("Not a valid host.")
      block <- 1
    }()

  }

  <- block
}