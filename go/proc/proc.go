// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package proc allows you to configure servers to be
// restarted with negligible downtime.
package proc

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/youtube/vitess/go/relog"
)

const pidURL = "/debug/pid"

// Listen tries to create a listener on the specified tcp port.
// Before creating the listener, it checks to see if there is another
// server already using the port. If there is one, it sends a USR1
// signal requesting the server to shutdown, and then attempts to
// to create the listener.
func Listen(port string) (l net.Listener, err error) {
	killPredecessor(port)
	return listen(port)
}

// Wait creates an HTTP handler on pidURL, and serves the current process
// pid on it. It then creates a signal handler and waits for SIGTERM or
// SIGUSR1, and returns when the signal is received. A new server that comes
// up will query this URL. If it receives a valid response, it will send a
// SIGUSR1 signal and attempt to bind to the port the current server is using.
func Wait() os.Signal {
	http.HandleFunc(pidURL, func(r http.ResponseWriter, req *http.Request) {
		r.Write(strconv.AppendInt(nil, int64(os.Getpid()), 10))
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGUSR1)
	return <-c
}

func killPredecessor(port string) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s%s", port, pidURL))
	if err != nil {
		if !strings.Contains(err.Error(), "connection refused") {
			relog.Error("unexpected error on port %v: %v, trying to start anyway", port, err)
		}
		return
	}
	num, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		relog.Error("could not read pid: %vd, trying to start anyway", err)
		return
	}
	pid, err := strconv.Atoi(string(num))
	if err != nil {
		relog.Error("could not read pid: %vd, trying to start anyway", err)
		return
	}
	err = syscall.Kill(pid, syscall.SIGUSR1)
	if err != nil {
		relog.Error("error killing %v: %v, trying to start anyway", pid, err)
	}
}

func listen(port string) (l net.Listener, err error) {
	for i := 0; i < 100; i++ {
		l, err = net.Listen("tcp", ":"+port)
		if err != nil {
			if strings.Contains(err.Error(), "already in use") {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			return nil, err
		}
		break
	}
	return l, err
}
