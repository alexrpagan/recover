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
)

var vs_idx = 0

var mode = "tcp"
var port = ":5000"

// turn on profiling
var cpuprofile = flag.String("prof", "", "write cpu profile to file")
var me         = flag.Int("me", -1, "who am I")
var bench      = flag.Int("bench", -1, "run a benchmark")

var hosts = []string{ "ec2-184-72-166-194.compute-1.amazonaws.com",
                      "ec2-23-20-132-169.compute-1.amazonaws.com",
                      "ec2-184-73-58-176.compute-1.amazonaws.com",
                      "ec2-107-21-173-100.compute-1.amazonaws.com",
                      "ec2-23-22-234-159.compute-1.amazonaws.com",
                      "ec2-54-224-151-239.compute-1.amazonaws.com",
                      "ec2-72-44-46-122.compute-1.amazonaws.com",
                      "ec2-54-242-209-195.compute-1.amazonaws.com",
                      "ec2-50-16-130-137.compute-1.amazonaws.com",
                      "ec2-54-242-42-105.compute-1.amazonaws.com" }


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

func main() {

  runtime.GOMAXPROCS(16)

  flag.Parse()
  block := make(chan int)

  if *cpuprofile != "" {
    f, _ := os.Create(*cpuprofile)
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }

  if *bench >= 0 {

    fmt.Println("running benchmark ", *bench)

    ck := pbservice.MakeClerk("", hosts[0] + port, mode)
    iters := 10000
    times := make([]int64, iters)

    switch *bench {
    case 0:
      //round of puts
      for i:=0; i < iters; i++ {
        fmt.Println(i)
        t1 := time.Now().UnixNano()
        ck.Put(fmt.Sprintf("%d", i), fmt.Sprintf("%cvalue",48 + rand.Intn(122-48)))
        t2 := time.Now().UnixNano()
        times[i] = t2-t1

        if i % 100 == 0 {
          fmt.Println("finished ", i)
        }
      }
      printStats(times)

      for i:=0; i < iters; i++ {
        t1 := time.Now().UnixNano()
        fmt.Println(ck.Get(fmt.Sprintf("%d", i % 10)))
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