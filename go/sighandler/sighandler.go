// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package signal implements operating system-independent signal handling.
package sighandler

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type SignalHandler func(signal os.Signal)

// Registered handlers will be dispatched automatically when a signal arrives.
var mutex sync.Mutex
var signalHandlerMap map[os.Signal]SignalHandler

func init() {
	signalHandlerMap = make(map[os.Signal]SignalHandler)
	SetSignalHandler(syscall.SIGINT, DefaultSignalHandler)
	SetSignalHandler(syscall.SIGTERM, DefaultSignalHandler)
	c := make(chan os.Signal)
	signal.Notify(c)
	go signalDispatcher(c)
}

func DefaultSignalHandler(signal os.Signal) {
	os.Exit(2)
}

func signalDispatcher(signalChan <-chan os.Signal) {
	for sig := range signalChan {
		mutex.Lock()
		sigHandler, ok := signalHandlerMap[sig]
		mutex.Unlock()
		if ok {
			sigHandler(sig)
		}
	}
}

func SetSignalHandler(signal os.Signal, signalHandler SignalHandler) SignalHandler {
	mutex.Lock()
	defer mutex.Unlock()

	oldHandler, _ := signalHandlerMap[signal]
	signalHandlerMap[signal] = signalHandler
	return oldHandler
}

func ClearSignalHandler(signal os.Signal) {
	mutex.Lock()
	defer mutex.Unlock()

	delete(signalHandlerMap, signal)
}
