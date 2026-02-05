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
	"runtime/debug"
	"syscall"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

// Init is the first phase of the server startup.
func Init() {
	mu.Lock()
	defer mu.Unlock()
	initStartTime = time.Now()

	// Uptime metric
	_ = stats.NewGaugeFunc("Uptime", "Uptime in nanoseconds", func() int64 {
		return int64(time.Since(serverStart).Nanoseconds())
	})

	// Ignore SIGPIPE if specified
	// The Go runtime catches SIGPIPE for us on all fds except stdout/stderr
	// See https://golang.org/pkg/os/signal/#hdr-SIGPIPE
	if catchSigpipe {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGPIPE)
		go func() {
			<-sigChan
			log.Warning("Caught SIGPIPE (ignoring all future SIGPIPEs)")
			signal.Ignore(syscall.SIGPIPE)
		}()
	}

	// Add version tag to every info log
	log.Infof(AppVersion.String())
	if inited {
		log.Fatal("servenv.Init called second time")
	}
	inited = true

	// Once you run as root, you pretty much destroy the chances of a
	// non-privileged user starting the program correctly.
	if uid := os.Getuid(); uid == 0 {
		log.Exitf("servenv.Init: running this as root makes no sense")
	}

	// We used to set this limit directly, but you pretty much have to
	// use a root account to allow increasing a limit reliably. Dropping
	// privileges is also tricky. The best strategy is to make a shell
	// script set up the limits as root and switch users before starting
	// the server.
	fdLimit := &syscall.Rlimit{}
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		log.Errorf("max-open-fds failed: %v", err)
	}
	fdl := stats.NewGauge("MaxFds", "File descriptor limit")
	fdl.Set(int64(fdLimit.Cur))

	// Limit the stack size. We don't need huge stacks and smaller limits mean
	// any infinite recursion fires earlier and on low memory systems avoids
	// out of memory issues in favor of a stack overflow error.
	debug.SetMaxStack(maxStackSize)

	onInitHooks.Fire()
}
