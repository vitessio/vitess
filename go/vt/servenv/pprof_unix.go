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
	"os"
	"os/signal"
	"syscall"

	"vitess.io/vitess/go/vt/log"
)

func pprofInit() {
	prof, err := parseProfileFlag(pprofFlag)
	if err != nil {
		log.Fatal(err)
	}
	if prof != nil {
		start, stop := prof.init()
		startSignal := make(chan os.Signal, 1)
		stopSignal := make(chan os.Signal, 1)

		if prof.waitSig {
			signal.Notify(startSignal, syscall.SIGUSR1)
		} else {
			start()
			signal.Notify(stopSignal, syscall.SIGUSR1)
		}

		go func() {
			for {
				<-startSignal
				start()
				signal.Reset(syscall.SIGUSR1)
				signal.Notify(stopSignal, syscall.SIGUSR1)
			}
		}()

		go func() {
			for {
				<-stopSignal
				stop()
				signal.Reset(syscall.SIGUSR1)
				signal.Notify(startSignal, syscall.SIGUSR1)
			}
		}()

		OnTerm(stop)
	}
}
