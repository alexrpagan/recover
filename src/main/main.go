package main

import (
  "flag"
  "fmt"
  "runtime"
  "runtime/pprof"
  "os"
)

// turn on profiling
var cpuprofile = flag.String("prof", "", "write cpu profile to file")

func main() {

  runtime.GOMAXPROCS(16)
  flag.Parse()
  // block := make(chan int)

  if *cpuprofile != "" {
    f, _ := os.Create(*cpuprofile)
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }

  name, err := os.Hostname()

  if err == nil {
    fmt.Println(name)
  }

}