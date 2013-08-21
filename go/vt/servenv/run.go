package servenv

import (
	"fmt"
	"net/http"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/proc"
)

// Run starts listening for RPC and HTTP requests on the given port,
// and blocks until it the process gets a signal.
func Run(port int) {
	RunSecure(port, 0, "", "", "")
}

// RunSecure is like Run, but it additionally listens for RPC and HTTP
// requests using TLS on securePort, using the passed certificate,
// key, and CA certificate.
func RunSecure(port int, securePort int, cert, key, caCert string) {
	ServeRPC()

	l, err := proc.Listen(fmt.Sprintf("%v", port))
	if err != nil {
		log.Fatal(err)
	}

	go http.Serve(l, nil)

	if securePort != 0 {
		log.Infof("listening on secure port %v", securePort)
		SecureServe(fmt.Sprintf(":%d", securePort), cert, key, caCert)
	}
	proc.Wait()
	Close()
}

// Close runs any registered exit hooks in parallel.
func Close() {
	mu.Lock()
	defer mu.Unlock()

	wg := sync.WaitGroup{}

	for _, f := range onCloseHooks {
		wg.Add(1)
		go func(f func()) {
			f()
			wg.Done()
		}(f)
	}
	wg.Wait()
}

// OnClose registers f to be run at the end of the app lifecycle. All
// hooks are run in parallel.
func OnClose(f func()) {
	mu.Lock()
	defer mu.Unlock()
	onCloseHooks = append(onCloseHooks, f)
}
