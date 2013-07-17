package zookeeper_test

import (
	"fmt"
	. "launchpad.net/gocheck"
	zk "launchpad.net/gozk/zookeeper"
	"os"
	"testing"
	"time"
)

func TestAll(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&S{})

type S struct {
	zkServer   *zk.Server
	zkTestRoot string
	zkTestPort int
	zkProcess  *os.Process // The running ZooKeeper process
	zkAddr     string

	handles     []*zk.Conn
	events      []*zk.Event
	liveWatches int
	deadWatches chan bool
}

var logLevel = 0 //zk.LOG_ERROR

func (s *S) init(c *C) (*zk.Conn, chan zk.Event) {
	c.Logf("init dialling %q", s.zkAddr)
	conn, watch, err := zk.Dial(s.zkAddr, 5e9)
	c.Assert(err, IsNil)
	s.handles = append(s.handles, conn)
	bufferedWatch := make(chan zk.Event, 256)

	select {
	case e, ok := <-watch:
		c.Assert(ok, Equals, true)
		c.Assert(e.Type, Equals, zk.EVENT_SESSION)
		c.Assert(e.State, Equals, zk.STATE_CONNECTED)
		bufferedWatch <- e
	case <-time.After(5e9):
		c.Fatalf("timeout dialling zookeeper addr %v", s.zkAddr)
	}

	s.liveWatches += 1
	go func() {
	loop:
		for {
			select {
			case event, ok := <-watch:
				if !ok {
					close(bufferedWatch)
					break loop
				}
				select {
				case bufferedWatch <- event:
				default:
					panic("Too many events in buffered watch!")
				}
			}
		}
		s.deadWatches <- true
	}()

	return conn, bufferedWatch
}

func (s *S) SetUpTest(c *C) {
	c.Assert(zk.CountPendingWatches(), Equals, 0,
		Commentf("Test got a dirty watch state before running!"))
	zk.SetLogLevel(logLevel)
}

func (s *S) TearDownTest(c *C) {
	// Close all handles opened in s.init().
	for _, handle := range s.handles {
		handle.Close()
	}

	// Wait for all the goroutines created in s.init() to terminate.
	for s.liveWatches > 0 {
		select {
		case <-s.deadWatches:
			s.liveWatches -= 1
		case <-time.After(5e9):
			panic("There's a locked watch goroutine :-(")
		}
	}

	// Reset the list of handles.
	s.handles = make([]*zk.Conn, 0)

	c.Assert(zk.CountPendingWatches(), Equals, 0,
		Commentf("Test left live watches behind!"))
}

// We use the suite set up and tear down to manage a custom ZooKeeper
//
func (s *S) SetUpSuite(c *C) {
	var err error
	s.deadWatches = make(chan bool)

	// N.B. We need to create a subdirectory because zk.CreateServer
	// insists on creating its own directory.

	s.zkTestRoot = c.MkDir() + "/zk"
	port := 21812
	s.zkAddr = fmt.Sprint("localhost:", port)

	s.zkServer, err = zk.CreateServer(port, s.zkTestRoot, "")
	if err != nil {
		c.Fatal("Cannot set up server environment: ", err)
	}
	err = s.zkServer.Start()
	if err != nil {
		c.Fatal("Cannot start ZooKeeper server: ", err)
	}
}

func (s *S) TearDownSuite(c *C) {
	if s.zkServer != nil {
		s.zkServer.Destroy()
	}
}
