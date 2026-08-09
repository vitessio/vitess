//go:build !windows

/*
Copyright 2023 The Vitess Authors.

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
	"os"
	"os/signal"
	"sync"
	"syscall"

	"vitess.io/vitess/go/vt/log"
)

func pprofInit() {
	if stop := startPprof(); stop != nil {
		OnTerm(stop)
	}
}

func startPprof() (stop func()) {
	prof, err := parseProfileFlag(pprofFlag)
	if err != nil {
		log.Error(fmt.Sprint(err))
		os.Exit(1)
	}

	if prof == nil {
		return nil
	}

	start, stopProf := prof.init()
	startSignal := make(chan os.Signal, 1)
	stopSignal := make(chan os.Signal, 1)

	startDoneCh := make(chan struct{})
	stopDoneCh := make(chan struct{})
	stopCh := make(chan struct{})

	if prof.waitSig {
		signal.Notify(startSignal, syscall.SIGUSR1)
	} else {
		start()
		signal.Notify(stopSignal, syscall.SIGUSR1)
	}

	go func() {
		defer close(startDoneCh)
		for {
			select {
			case <-startSignal:
				start()
				signal.Reset(syscall.SIGUSR1)
				signal.Notify(stopSignal, syscall.SIGUSR1)
			case <-stopCh:
				return
			}
		}
	}()

	go func() {
		defer close(stopDoneCh)
		for {
			select {
			case <-stopSignal:
				stopProf()
				signal.Reset(syscall.SIGUSR1)
				signal.Notify(startSignal, syscall.SIGUSR1)
			case <-stopCh:
				return
			}
		}
	}()

	return sync.OnceFunc(func() {
		close(stopCh)
		<-startDoneCh
		<-stopDoneCh
		// Unregister the channels only after both listeners have exited;
		// a listener handling a signal could otherwise re-register a
		// channel via signal.Notify after it was stopped here.
		signal.Stop(startSignal)
		signal.Stop(stopSignal)
		stopProf()
	})
}
