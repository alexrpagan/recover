package main

import "pbservice"
import "flag"
import "fmt"
import "runtime"

func main() {

  runtime.GOMAXPROCS(runtime.NumCPU())

  // required
  var hostname = flag.String("h", "localhost", "The hostname of this server")

  // test reading segments from disk and sending to another server
  var testsend = flag.Bool("ts", false, "Try sending some segments, then give up")
  var testsendhost = flag.String("tsh", "localhost", "The host to send segments to")

  // test writing segments to disk
  var testwrite = flag.Bool("tw", false, "Write a few segments")

  // test reading segments from disk
  var testread = flag.Bool("tr", false, "Read a few segments")

  flag.Parse()
  block := make(chan int)

  fmt.Println("Starting kvserver.")

  pb := pbservice.StartServer(*hostname)

  if *testsend && *testsendhost != "" {
    fmt.Println("Entering send host")
    go func() {

      args  := &pbservice.TestSendSegmentArgs{}
      reply := &pbservice.TestSendSegmentReply{}

      args.Size = 1
      args.TestSendHost = *testsendhost

      pb.TestSendSegment(args, reply)
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