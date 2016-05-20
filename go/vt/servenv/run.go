package servenv

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/proc"
)

var (
	onCloseHooks event.Hooks
)

// Run starts listening for RPC and HTTP requests,
// and blocks until it the process gets a signal.
func Run(port int) {
	populateListeningURL()
	createGRPCServer()
	onRunHooks.Fire()
	serveGRPC()
	serveSocketFile()

	l, err := proc.Listen(fmt.Sprintf("%v", port))
	if err != nil {
		log.Fatal(err)
	}
	go http.Serve(l, nil)

	proc.Wait()
	l.Close()

	startTime := time.Now()
	log.Infof("Entering lameduck mode for at least %v", *lameduckPeriod)
	log.Infof("Firing asynchronous OnTerm hooks")
	go onTermHooks.Fire()

	fireOnTermSyncHooks(*onTermTimeout)
	if remain := *lameduckPeriod - time.Since(startTime); remain > 0 {
		log.Infof("Sleeping an extra %v after OnTermSync to finish lameduck period", remain)
		time.Sleep(remain)
	}

	log.Info("Shutting down gracefully")
	Close()
}

// Close runs any registered exit hooks in parallel.
func Close() {
	onCloseHooks.Fire()
	ListeningURL = url.URL{}
}

// OnClose registers f to be run at the end of the app lifecycle.
// This happens after the lameduck period just before the program exits.
// All hooks are run in parallel.
func OnClose(f func()) {
	onCloseHooks.Add(f)
}
