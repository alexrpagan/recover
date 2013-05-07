package pbservice
/*
import "fmt"
import "io"
import "net"
import "testing"
import "time"
import "log"
import "runtime"
import "math/rand"
import "os"
import "strconv"

func check(ck *Clerk, key string, value string) {
  v := ck.Get(key)
  if v != value {
    log.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
  }
}

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "pb-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}


func TestBasicFail(t *testing.T) {
  runtime.GOMAXPROCS(8)

}

*/