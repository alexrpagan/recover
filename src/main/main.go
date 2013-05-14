package main

import (
  "flag"
  "fmt"
  "runtime"
  "runtime/pprof"
  "viewservice"
  "pbservice"
  "os"
)

var vs_idx = 0

var mode = "tcp"
var port = ":5000"

// turn on profiling
var cpuprofile = flag.String("prof", "", "write cpu profile to file")
var me         = flag.Int("me", -1, "who am I")

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

func main() {

  runtime.GOMAXPROCS(16)

  flag.Parse()
  block := make(chan int)

  if *cpuprofile != "" {
    f, _ := os.Create(*cpuprofile)
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
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