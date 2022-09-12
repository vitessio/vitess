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
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
)

var (
	pprofFlag []string
)

type profmode string

const (
	profileCPU       profmode = "cpu"
	profileMemHeap   profmode = "mem_heap"
	profileMemAllocs profmode = "mem_allocs"
	profileMutex     profmode = "mutex"
	profileBlock     profmode = "block"
	profileTrace     profmode = "trace"
	profileThreads   profmode = "threads"
	profileGoroutine profmode = "goroutine"
)

func (p profmode) filename() string {
	return fmt.Sprintf("%s.pprof", string(p))
}

type profile struct {
	mode    profmode
	rate    int
	path    string
	quiet   bool
	waitSig bool
}

func parseProfileFlag(pf []string) (*profile, error) {
	if len(pf) == 0 {
		return nil, nil
	}

	var p profile

	switch pf[0] {
	case "cpu":
		p.mode = profileCPU
	case "mem", "mem=heap":
		p.mode = profileMemHeap
		p.rate = 4096
	case "mem=allocs":
		p.mode = profileMemAllocs
		p.rate = 4096
	case "mutex":
		p.mode = profileMutex
		p.rate = 1
	case "block":
		p.mode = profileBlock
		p.rate = 1
	case "trace":
		p.mode = profileTrace
	case "threads":
		p.mode = profileThreads
	case "goroutine":
		p.mode = profileGoroutine
	default:
		return nil, fmt.Errorf("unknown profile mode: %q", pf[0])
	}

	for _, kv := range pf[1:] {
		var err error
		fields := strings.SplitN(kv, "=", 2)

		switch fields[0] {
		case "rate":
			if len(fields) == 1 {
				return nil, fmt.Errorf("missing value for 'rate'")
			}
			p.rate, err = strconv.Atoi(fields[1])
			if err != nil {
				return nil, fmt.Errorf("invalid profile rate %q: %v", fields[1], err)
			}

		case "path":
			if len(fields) == 1 {
				return nil, fmt.Errorf("missing value for 'path'")
			}
			p.path = fields[1]

		case "quiet":
			if len(fields) == 1 {
				p.quiet = true
				continue
			}

			p.quiet, err = strconv.ParseBool(fields[1])
			if err != nil {
				return nil, fmt.Errorf("invalid quiet flag %q: %v", fields[1], err)
			}
		case "waitSig":
			if len(fields) == 1 {
				p.waitSig = true
				continue
			}
			p.waitSig, err = strconv.ParseBool(fields[1])
			if err != nil {
				return nil, fmt.Errorf("invalid waitSig flag %q: %v", fields[1], err)
			}
		default:
			return nil, fmt.Errorf("unknown flag: %q", fields[0])
		}
	}

	return &p, nil
}

var profileStarted uint32

func startCallback(start func()) func() {
	return func() {
		if atomic.CompareAndSwapUint32(&profileStarted, 0, 1) {
			start()
		} else {
			log.Fatal("profile: Start() already called")
		}
	}
}

func stopCallback(stop func()) func() {
	return func() {
		if atomic.CompareAndSwapUint32(&profileStarted, 1, 0) {
			stop()
		}
	}
}

func (prof *profile) mkprofile() io.WriteCloser {
	var (
		path string
		err  error
		logf = func(format string, args ...any) {}
	)

	if prof.path != "" {
		path = prof.path
		err = os.MkdirAll(path, 0777)
	} else {
		path, err = os.MkdirTemp("", "profile")
	}
	if err != nil {
		log.Fatalf("pprof: could not create initial output directory: %v", err)
	}

	if !prof.quiet {
		logf = log.Infof
	}

	fn := filepath.Join(path, prof.mode.filename())
	f, err := os.Create(fn)
	if err != nil {
		log.Fatalf("pprof: could not create profile %q: %v", fn, err)
	}
	logf("pprof: %s profiling enabled, %s", string(prof.mode), fn)

	return f
}

// init returns a start function that begins the configured profiling process and
// returns a cleanup function that must be executed before process termination to
// flush the profile to disk.
// Based on the profiling code in github.com/pkg/profile
func (prof *profile) init() (start func(), stop func()) {
	var pf io.WriteCloser

	switch prof.mode {
	case profileCPU:
		start = startCallback(func() {
			pf = prof.mkprofile()
			pprof.StartCPUProfile(pf)
		})
		stop = stopCallback(func() {
			pprof.StopCPUProfile()
			pf.Close()
		})
		return start, stop

	case profileMemHeap, profileMemAllocs:
		old := runtime.MemProfileRate
		start = startCallback(func() {
			pf = prof.mkprofile()
			runtime.MemProfileRate = prof.rate
		})
		stop = stopCallback(func() {
			tt := "heap"
			if prof.mode == profileMemAllocs {
				tt = "allocs"
			}
			pprof.Lookup(tt).WriteTo(pf, 0)
			pf.Close()
			runtime.MemProfileRate = old
		})
		return start, stop

	case profileMutex:
		start = startCallback(func() {
			pf = prof.mkprofile()
			runtime.SetMutexProfileFraction(prof.rate)
		})
		stop = stopCallback(func() {
			if mp := pprof.Lookup("mutex"); mp != nil {
				mp.WriteTo(pf, 0)
			}
			pf.Close()
			runtime.SetMutexProfileFraction(0)
		})
		return start, stop

	case profileBlock:
		start = startCallback(func() {
			pf = prof.mkprofile()
			runtime.SetBlockProfileRate(prof.rate)
		})
		stop = stopCallback(func() {
			pprof.Lookup("block").WriteTo(pf, 0)
			pf.Close()
			runtime.SetBlockProfileRate(0)
		})
		return start, stop

	case profileThreads:
		start = startCallback(func() {
			pf = prof.mkprofile()
		})
		stop = stopCallback(func() {
			if mp := pprof.Lookup("threadcreate"); mp != nil {
				mp.WriteTo(pf, 0)
			}
			pf.Close()
		})
		return start, stop

	case profileTrace:
		start = startCallback(func() {
			pf = prof.mkprofile()
			if err := trace.Start(pf); err != nil {
				log.Fatalf("pprof: could not start trace: %v", err)
			}
		})
		stop = stopCallback(func() {
			trace.Stop()
			pf.Close()
		})
		return start, stop

	case profileGoroutine:
		start = startCallback(func() {
			pf = prof.mkprofile()
		})
		stop = stopCallback(func() {
			if mp := pprof.Lookup("goroutine"); mp != nil {
				mp.WriteTo(pf, 0)
			}
			pf.Close()
		})
		return start, stop

	default:
		panic("unsupported profile mode")
	}
}

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

func init() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.StringSliceVar(&pprofFlag, "pprof", pprofFlag, "enable profiling")
	})
	OnInit(pprofInit)
}
