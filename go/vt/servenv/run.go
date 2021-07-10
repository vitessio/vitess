/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/log"
)

var (
	onCloseHooks event.Hooks
	// ExitChan waits for a signal that tells the process to terminate
	ExitChan chan os.Signal
)

// Run starts listening for RPC and HTTP requests,
// and blocks until it the process gets a signal.
func Run(port int) {
	populateListeningURL(int32(port))
	createGRPCServer()
	onRunHooks.Fire()
	serveGRPC()
	serveSocketFile()

	l, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Exit(err)
	}
	go http.Serve(l, nil)

	ExitChan = make(chan os.Signal, 1)
	signal.Notify(ExitChan, syscall.SIGTERM, syscall.SIGINT)
	// Wait for signal
	<-ExitChan
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
