package viewservice

import "testing"
import "runtime"
import "time"
import "fmt"
import "os"
import "strconv"

func check(t *testing.T, ck *Clerk, shardsToPrimaries map[int]string, n uint) {
  view, _ := ck.Get()

  for s,p := range shardsToPrimaries{
    if view.ShardsToPrimaries[s] != p {
      t.Fatalf("wanted primary %v for shard %d, got %v", p,s, view.ShardsToPrimaries[s])
    }
  }
  if n != 0 && n != view.ViewNumber {
    t.Fatalf("wanted viewnumber %v, got %v", n, view.ViewNumber)
  }
}

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
  vs := StartServer(vshost)

  ck := make([]*Clerk,CRITICAL_MASS+1)

  for i := 0; i < CRITICAL_MASS+1; i+= 1{

  ck[i] = MakeClerk(port(strconv.Itoa(i)), vshost)
  }
  //

  if ck[1].view.ViewNumber != 0 {
    t.Fatalf("there was a view too soon")
  }

  fmt.Printf("vs = %v\n",vs)

  // very first primary
  fmt.Printf("Test: First view ...\n")

  for i := 0; i < len(ck)-3; i++ {
    go func(vi int){
      var num uint
      num = 0
      for {
        view,_,_ := ck[vi].Ping(num)
        num = view.ViewNumber
        time.Sleep(PING_INTERVAL)
      }
    }(i)
  }
    go func(){
      var num uint
      num = 0
      for j := 0; j< DEAD_PINGS * 3; j++ {
        view,_,_ := ck[len(ck)-2].Ping(num)
        num = view.ViewNumber
      time.Sleep(PING_INTERVAL)
      }
    }()
    go func(){
      var num uint
      num = 0
      for j := 0; j< DEAD_PINGS * 10; j++ {
        view,_,_ := ck[len(ck)-3].Ping(num)
        num = view.ViewNumber
        time.Sleep(PING_INTERVAL)
      }
    }()
  time.Sleep(PING_INTERVAL*DEAD_PINGS*2)

  check(t, ck[1],ck[0].view.ShardsToPrimaries, 1)
  //view :=ck[1].view

  shardplacement := make([]int, len(ck))
  for _,p := range ck[0].view.ShardsToPrimaries{
     for n,c := range ck {
       if c.me == p{
         shardplacement[n] += 1
         break
       }
     }
  }
//  fmt.Printf("shardstoprimaries %v\n", ck[0].view.ShardsToPrimaries)
  fmt.Printf("shardplacement %v\n",shardplacement)
  fmt.Printf("  ... Passed\n")
  time.Sleep(PING_INTERVAL*DEAD_PINGS*5)

  fmt.Printf("Test: One Stops Pinging with no empty\n")
  //view =ck[1].view

  check(t, ck[1],ck[0].view.ShardsToPrimaries, 2)
  shardplacement = make([]int, len(ck))
  for _,p := range ck[0].view.ShardsToPrimaries{
     for n,c := range ck {
       if c.me == p{
         shardplacement[n] += 1
         break
       }
     }
  }
//  fmt.Printf("shardstoprimaries %v\n", ck[0].view.ShardsToPrimaries)
  fmt.Printf("shardplacement %v\n",shardplacement)
  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Two Start Pinging\n")
 for i := 1; i <= 2; i++ {
    go func(vi int){
      var num uint
      num = 0
      for {
        view,_,_ := ck[vi].Ping(num)
        num = view.ViewNumber
        time.Sleep(PING_INTERVAL)
      }
    }(len(ck)-i)
  }
  time.Sleep(PING_INTERVAL*DEAD_PINGS*3)
  check(t, ck[1],ck[len(ck)-1].view.ShardsToPrimaries, 2)
  shardplacement = make([]int, len(ck))
  for _,p := range ck[len(ck)-1].view.ShardsToPrimaries{
     for n,c := range ck {
       if c.me == p{
         shardplacement[n] += 1
         break
       }
     }
  }
//  fmt.Printf("shardstoprimaries %v\n", ck[0].view.ShardsToPrimaries)
  fmt.Printf("shardplacement %v\n",shardplacement)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: One Stops Pinging with Two Empty\n")
  //view =ck[1].view
  time.Sleep(PING_INTERVAL*DEAD_PINGS*10)

  check(t, ck[1],ck[0].view.ShardsToPrimaries, 3)
  shardplacement = make([]int, len(ck))
  for _,p := range ck[0].view.ShardsToPrimaries{
     for n,c := range ck {
       if c.me == p{
         shardplacement[n] += 1
         break
       }
     }
  }
//  fmt.Printf("shardstoprimaries %v\n", ck[0].view.ShardsToPrimaries)
  fmt.Printf("shardplacement %v\n",shardplacement)
  fmt.Printf("  ... Passed\n")


}




/*
  // very first backup
  fmt.Printf("Test: First backup ...\n")

  {
    vx, _ := ck[1].Get()
    for i := 0; i < DEAD_PINGS * 2; i++ {
      ck1.Ping(1)
      view, _ := ck[2].Ping(0)
      if view.Backup == ck[2].me {
        break
      }
      time.Sleep(PING_INTERVAL)
    }
    check(t, ck[1], ck[1].me, ck[2].me, vx.Viewnum + 1)
  }
  fmt.Printf("  ... Passed\n")

  // primary dies, backup should take over
  fmt.Printf("Test: Backup takes over if primary fails ...\n")

  {
    ck1.Ping(2)
    vx, _ := ck[2].Ping(2)
    for i := 0; i < DEAD_PINGS * 2; i++ {
      v, _ := ck[2].Ping(vx.Viewnum)
      if v.Primary == ck[2].me && v.Backup == "" {
        break
      }
      time.Sleep(PING_INTERVAL)
    }
    check(t, ck[2], ck[2].me, "", vx.Viewnum + 1)
  }
  fmt.Printf("  ... Passed\n")

  // revive ck[1], should become backup
  fmt.Printf("Test: Restarted server becomes backup ...\n")

  {
    vx, _ := ck[2].Get()
    ck[2].Ping(vx.Viewnum)
    for i := 0; i < DEAD_PINGS * 2; i++ {
      ck[1].Ping(0)
      v, _ := ck[2].Ping(vx.Viewnum)
      if v.Primary == ck[2].me && v.Backup == ck[1].me {
        break
      }
      time.Sleep(PING_INTERVAL)
    }
    check(t, ck[2], ck[2].me, ck[1].me, vx.Viewnum + 1)
  }
  fmt.Printf("  ... Passed\n")

  // start ck[3], kill the primary (ck[2]), the previous backup (ck[1])
  // should become the server, and ck[3] the backup
  fmt.Printf("Test: Idle third server becomes backup if primary fails ...\n")

  {
    vx, _ := ck[2].Get()
    ck[2].Ping(vx.Viewnum)
    for i := 0; i < DEAD_PINGS * 2; i++ {
      ck[3].Ping(0)
      v, _ := ck[1].Ping(vx.Viewnum)
      if v.Primary == ck[1].me && v.Backup == ck[3].me {
        break;
      }
      vx = v
      time.Sleep(PING_INTERVAL)
    }
    check(t, ck[1], ck[1].me, ck[3].me, vx.Viewnum + 1)
  }
  fmt.Printf("  ... Passed\n")

  // kill and immediately restart the primary -- does viewservice
  // conclude primary is down even though it's pinging?
  fmt.Printf("Test: Restarted primary treated as dead ...\n")

  {
    vx, _ := ck[1].Get()
    ck[1].Ping(vx.Viewnum)
    for i := 0; i < DEAD_PINGS * 2; i++ {
      ck[1].Ping(0)
      ck[3].Ping(vx.Viewnum)
      v, _ := ck[3].Get()
      if v.Primary != ck[1].me {
        break
      }
      time.Sleep(PING_INTERVAL)
    }
    vy, _ := ck[3].Get()
    if vy.Primary != ck[3].me {
      t.Fatalf("expected primary=%v, got %v\n", ck[3].me, vy.Primary)
    }
  }
  fmt.Printf("  ... Passed\n")


  // set up a view with just 3 as primary,
  // to prepare for the next test.
  {
    for i := 0; i < DEAD_PINGS * 3; i++ {
      vx, _ := ck[3].Get()
      ck[3].Ping(vx.Viewnum)
      time.Sleep(PING_INTERVAL)
    }
    v, _ := ck[3].Get()
    if v.Primary != ck[3].me || v.Backup != "" {
      t.Fatalf("wrong primary or backup")
    }
  }

  // does viewserver wait for ack of previous view before
  // starting the next one?
  fmt.Printf("Test: Viewserver waits for primary to ack view ...\n")

  {
    // set up p=ck[3] b=ck[1], but
    // but do not ack
    vx, _ := ck[1].Get()
    for i := 0; i < DEAD_PINGS * 3; i++ {
      ck[1].Ping(0)
      ck[3].Ping(vx.Viewnum)
      v, _ := ck[1].Get()
      if v.Viewnum > vx.Viewnum {
        break
      }
      time.Sleep(PING_INTERVAL)
    }
    check(t, ck[1], ck[3].me, ck[1].me, vx.Viewnum+1)
    vy, _ := ck[1].Get()
    // ck[3] is the primary, but it never acked.
    // let ck[3] die. check that ck[1] is not promoted.
    for i := 0; i < DEAD_PINGS * 3; i++ {
      v, _ := ck[1].Ping(vy.Viewnum)
      if v.Viewnum > vy.Viewnum {
        break
      }
      time.Sleep(PING_INTERVAL)
    }
    check(t, ck[2], ck[3].me, ck[1].me, vy.Viewnum)
  }
  fmt.Printf("  ... Passed\n")

  // if old servers die, check that a new (uninitialized) server
  // cannot take over.
  fmt.Printf("Test: Uninitialized server can't become primary ...\n")

  {
    for i := 0; i < DEAD_PINGS * 2; i++ {
      v, _ := ck[1].Get()
      ck[1].Ping(v.Viewnum)
      ck[2].Ping(0)
      ck[3].Ping(v.Viewnum)
      time.Sleep(PING_INTERVAL)
    }
    for i := 0; i < DEAD_PINGS * 2; i++ {
      ck[2].Ping(0)
      time.Sleep(PING_INTERVAL)
    }
    vz, _ := ck[2].Get()
    if vz.Primary == ck[2].me {
      t.Fatalf("uninitialized backup promoted to primary")
    }
  }
  fmt.Printf("  ... Passed\n")

  vs.Kill()
}
*/