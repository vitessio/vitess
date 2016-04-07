package zookeeper_test

import (
	"io"
	. "launchpad.net/gocheck"
	zk "launchpad.net/gozk/zookeeper"
	"log"
	"net"
	"time"
)

// requestFuncs holds all the requests that take a read lock
// on the zk connection except those that don't actually
// make a round trip to the server.
var requestFuncs = []func(conn *zk.Conn, path string) error{
	func(conn *zk.Conn, path string) error {
		_, err := conn.Create(path, "", 0, nil)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, err := conn.Exists(path)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, _, err := conn.ExistsW(path)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, _, err := conn.Get(path)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, _, _, err := conn.GetW(path)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, _, err := conn.Children(path)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, _, _, err := conn.ChildrenW(path)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, err := conn.Set(path, "", 0)
		return err
	},
	func(conn *zk.Conn, path string) error {
		_, _, err := conn.ACL(path)
		return err
	},
	func(conn *zk.Conn, path string) error {
		return conn.SetACL(path, []zk.ACL{{
			Perms:  zk.PERM_ALL,
			Scheme: "digest",
			Id:     "foo",
		}}, 0)
	},
	func(conn *zk.Conn, path string) error {
		return conn.Delete(path, 0)
	},
}

func (s *S) TestConcurrentClose(c *C) {
	// make sure the server is ready to receive connections.
	s.init(c)

	// Close should wait until all outstanding requests have
	// completed before returning.  The idea of this test is that
	// any request that requests or changes a zookeeper node must
	// make at least one round trip to the server, so we interpose a
	// proxy between the client and the server which can stop all
	// incoming traffic on demand, thus blocking the request until
	// we want it to unblock.
	//
	// We assume that all requests take less than 0.1s to complete,
	// thus when we wait below, neither of the above goroutines
	// should complete within the allotted time (the request because
	// it's waiting for a reply from the server and the close
	// because it's waiting for the request to complete).  If the
	// locking doesn't work, the Close will return early.  If the
	// proxy blocking doesn't work, the request will return early.
	//
	// When we reenable incoming messages from the server, both
	// goroutines should complete.  We can't tell which completes
	// first, but the fact that the close blocked is sufficient to
	// tell that the locking is working correctly.
	for i, f := range requestFuncs {
		c.Logf("iter %d", i)
		p := newProxy(c, s.zkAddr)
		conn, watch, err := zk.Dial(p.addr(), 5e9)
		c.Assert(err, IsNil)
		c.Assert((<-watch).Ok(), Equals, true)

		// sanity check that the connection is actually
		// up and running.
		_, err = conn.Exists("/nothing")
		c.Assert(err, IsNil)

		p.stopIncoming()
		reqDone := make(chan bool)
		closeDone := make(chan bool)
		go func() {
			f(conn, "/closetest")
			reqDone <- true
		}()
		go func() {
			// sleep for long enough for the request to be initiated and the read lock taken.
			time.Sleep(0.05e9)
			conn.Close()
			closeDone <- true
		}()
		select {
		case <-reqDone:
			c.Fatalf("request %d finished early", i)
		case <-closeDone:
			c.Fatalf("request %d close finished early", i)
		case <-time.After(0.1e9):
		}
		p.startIncoming()
		for reqDone != nil || closeDone != nil {
			select {
			case <-reqDone:
				reqDone = nil
			case <-closeDone:
				closeDone = nil
			case <-time.After(0.4e9):
				c.Fatalf("request %d timed out waiting for req (%p) and close(%p)", i, reqDone, closeDone)
			}
		}
		p.close()
		err = f(conn, "/closetest")
		c.Check(zk.IsError(err, zk.ZCLOSING), Equals, true, Commentf("%v", err))
	}
}

type proxy struct {
	stop, start chan bool
	listener    net.Listener
}

// newProxy will listen on proxyAddr and connect its client to dstAddr, and return
// a proxy instance that can be used to control the connection.
func newProxy(c *C, dstAddr string) *proxy {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)
	p := &proxy{
		stop:     make(chan bool, 1),
		start:    make(chan bool, 1),
		listener: listener,
	}

	go func() {
		for {
			client, err := p.listener.Accept()
			if err != nil {
				// Ignore the error, because the connection will fail anyway.
				return
			}
			go func() {
				defer client.Close()
				server, err := net.Dial("tcp", dstAddr)
				if err != nil {
					log.Printf("cannot dial %q: %v", dstAddr, err)
					return
				}
				defer server.Close()
				go io.Copy(&haltableWriter{
					w:     client,
					stop:  p.stop,
					start: p.start},
					server)
				// When the client is closed, the deferred closes will
				// take down the other io.Copy too.
				io.Copy(server, client)
			}()
		}
	}()
	return p
}

func (p *proxy) close() error {
	return p.listener.Close()
}

func (p *proxy) addr() string {
	return p.listener.Addr().String()
}

func (p *proxy) stopIncoming() {
	if p.stop == nil {
		panic("cannot stop twice")
	}
	p.stop <- true
	p.stop = nil
}

func (p *proxy) startIncoming() {
	if p.start == nil {
		panic("cannot start twice")
	}
	p.start <- true
	p.start = nil
}

type haltableWriter struct {
	w           io.Writer
	stop, start chan bool
}

func (w *haltableWriter) Write(buf []byte) (int, error) {
	select {
	case <-w.stop:
		w.stop <- true
		<-w.start
		w.start <- true
	default:
	}
	return w.w.Write(buf)
}
