package servenv

import (
	"fmt"
	"net/http"
	"net/url"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/proc"
)

var (
	onCloseHooks hooks

	// filled in when calling Run or RunSecure
	ListeningURL url.URL
)

// Run starts listening for RPC and HTTP requests on the given port,
// and blocks until it the process gets a signal.
// It may also listen on a secure port, or on a unix socket.
func Run(port int) {
	onRunHooks.Fire()
	ServeRPC()

	l, err := proc.Listen(fmt.Sprintf("%v", port))
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
		Host:   fmt.Sprintf("%v:%v", host, port),
		Path:   "/",
	}

	go http.Serve(l, nil)
	serveSecurePort()
	serveSocketFile()

	proc.Wait()
	l.Close()
	Close()
}

// Close runs any registered exit hooks in parallel.
func Close() {
	onCloseHooks.Fire()
	ListeningURL = url.URL{}
}

// OnClose registers f to be run at the end of the app lifecycle. All
// hooks are run in parallel.
func OnClose(f func()) {
	onCloseHooks.Add(f)
}
