package servenv

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/proc"
)

var (
	onCloseHooks hooks

	Port           = flag.Int("port", 0, "port for the server")
	LameduckPeriod = flag.Duration("lameduck-period", 30*time.Second, "how long to keep the server running on SIGTERM before stopping")

	// filled in when calling Run or RunSecure
	ListeningURL url.URL
)

// Run starts listening for RPC and HTTP requests,
// and blocks until it the process gets a signal.
// It may also listen on a secure port, or on a unix socket.
func Run() {
	onRunHooks.Fire()
	ServeRPC()

	l, err := proc.Listen(fmt.Sprintf("%v", *Port))
	if err != nil {
		log.Fatal(err)
	}

	host, err := netutil.FullyQualifiedHostname()
	if err != nil {
		host, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() failed: %v", err)
		}
	}
	ListeningURL = url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%v:%v", host, *Port),
		Path:   "/",
	}

	go http.Serve(l, nil)
	serveSecurePort()
	serveSocketFile()

	proc.Wait()
	l.Close()
	log.Info("Entering lameduck mode")
	go onTermHooks.Fire()
	time.Sleep(*LameduckPeriod)
	log.Info("Shutting down")
	Close()
}

// Close runs any registered exit hooks in parallel.
func Close() {
	onCloseHooks.Fire()
	ListeningURL = url.URL{}
}

// OnTerm registers f to be run when the process receives a SIGTERM.
// All hooks are run in parallel.
// This allows the program to change its behavior during the lameduck period.
func OnTerm(f func()) {
	onTermHooks.Add(f)
}

// OnClose registers f to be run at the end of the app lifecycle.
// This happens after the lameduck period just before the program exits.
// All hooks are run in parallel.
func OnClose(f func()) {
	onCloseHooks.Add(f)
}
