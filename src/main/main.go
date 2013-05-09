package main

import "pbservice"
import "flag"
import "fmt"
import "runtime"
import "runtime/pprof"

// turn on profiling
var cpuprofile = flag.String("prof", "", "write cpu profile to file")

// required
var hostname = flag.String("h", "localhost", "The hostname of this server")

// test reading segments from disk and sending to another server
var testsend = flag.Bool("ts", false, "Try sending some segments, then give up")

// test writing segments to disk
var testwrite = flag.Bool("tw", false, "Write a few segments")

// test reading segments from disk
var testread = flag.Bool("tr", false, "Read a few segments")

func main() {

  runtime.GOMAXPROCS(16)


  flag.Parse()
  block := make(chan int)

  fmt.Println("Starting kvserver.")

  pb := pbservice.StartServer(*hostname)

  if *cpuprofile != "" {
    f, err := os.Create(*cpuprofile)
    if err != nil {
      log.Fatal(err)
    }
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }

  if *testsend {
    fmt.Println("Entering send host")
    go func() {

      args  := &pbservice.TestPullSegmentsArgs{}
      reply := &pbservice.TestPullSegmentsReply{}

      // numServers := 3
      // hosts := make([]string, numServers)
      // for i:=2; i <= numServers; i++ {
      //   hosts[i-1] = fmt.Sprintf("istc%d.csail.mit.edu", i)
      // }

      args.Size = 1  // how many 8mb log segs?
      args.Hosts = []string{"istc12.csail.mit.edu", "istc13.csail.mit.edu"}
      pb.TestPullSegments(args, reply)
      block <- 1
    }()
  } else if (*testwrite) {
    go func() {
      fmt.Println("Generating and writing segments")

      args  := &pbservice.TestWriteSegmentArgs{}
      reply := &pbservice.TestWriteSegmentReply{}

      args.NumOfSegs = 30

      pb.TestWriteSegment(args, reply)
      block <- 1
    }()
  } else if (*testread) {
    go func() {
      fmt.Println("Reading segments")

      args  := &pbservice.TestReadSegmentArgs{}
      reply := &pbservice.TestReadSegmentReply{}

      args.NumOfSegs = 30

      pb.TestReadSegment(args, reply)
      block <- 1
    }()
  }

  <-block
}