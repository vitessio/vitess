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

// Package servenv contains functionality that is common for all
// Vitess server programs.  It defines and initializes command line
// flags that control the runtime environment.
//
// After a server program has called flag.Parse, it needs to call
// env.Init to make env use the command line variables to initialize
// the environment. It also needs to call env.Close before exiting.
//
// Note: If you need to plug in any custom initialization/cleanup for
// a vitess distribution, register them using OnInit and onClose. A
// clean way of achieving that is adding to this package a file with
// an init() function that registers the hooks.
package servenv

import (
	// register the HTTP handlers for profiling
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"

	// register the proper init and shutdown hooks for logging
	_ "vitess.io/vitess/go/vt/logutil"

	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
	_flag "vitess.io/vitess/go/internal/flag"
)

var (
	// port is part of the flags used when calling RegisterDefaultFlags.
	port int

	// mutex used to protect the Init function
	mu sync.Mutex

	onInitHooks     event.Hooks
	onTermHooks     event.Hooks
	onTermSyncHooks event.Hooks
	onRunHooks      event.Hooks
	inited          bool

	// ListeningURL is filled in when calling Run, contains the server URL.
	ListeningURL url.URL
)

// Flags specific to Init, Run, and RunDefault functions.
var (
	lameduckPeriod = 50 * time.Millisecond
	onTermTimeout  = 10 * time.Second
	onCloseTimeout = 10 * time.Second
	catchSigpipe   bool
	maxStackSize   = 64 * 1024 * 1024
	initStartTime  time.Time // time when tablet init started: for debug purposes to time how long a tablet init takes
)

// RegisterFlags installs the flags used by Init, Run, and RunDefault.
//
// This must be called before servenv.ParseFlags if using any of those
// functions.
func RegisterFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.DurationVar(&lameduckPeriod, "lameduck-period", lameduckPeriod, "keep running at least this long after SIGTERM before stopping")
		fs.DurationVar(&onTermTimeout, "onterm_timeout", onTermTimeout, "wait no more than this for OnTermSync handlers before stopping")
		fs.DurationVar(&onCloseTimeout, "onclose_timeout", onCloseTimeout, "wait no more than this for OnClose handlers before stopping")
		fs.BoolVar(&catchSigpipe, "catch-sigpipe", catchSigpipe, "catch and ignore SIGPIPE on stdout and stderr if specified")
		fs.IntVar(&maxStackSize, "max-stack-size", maxStackSize, "configure the maximum stack size in bytes")

		// pid_file.go
		fs.StringVar(&pidFile, "pid_file", pidFile, "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")
	})
}

func GetInitStartTime() time.Time {
	mu.Lock()
	defer mu.Unlock()
	return initStartTime
}

// Init is the first phase of the server startup.
func Init() {
	mu.Lock()
	defer mu.Unlock()
	initStartTime = time.Now()

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

func populateListeningURL(port int32) {
	host, err := netutil.FullyQualifiedHostname()
	if err != nil {
		host, err = os.Hostname()
		if err != nil {
			log.Exitf("os.Hostname() failed: %v", err)
		}
	}
	ListeningURL = url.URL{
		Scheme: "http",
		Host:   netutil.JoinHostPort(host, port),
		Path:   "/",
	}
}

// OnInit registers f to be run at the beginning of the app
// lifecycle. It should be called in an init() function.
func OnInit(f func()) {
	onInitHooks.Add(f)
}

// OnTerm registers a function to be run when the process receives a SIGTERM.
// This allows the program to change its behavior during the lameduck period.
//
// All hooks are run in parallel, and there is no guarantee that the process
// will wait for them to finish before dying when the lameduck period expires.
//
// See also: OnTermSync
func OnTerm(f func()) {
	onTermHooks.Add(f)
}

// OnTermSync registers a function to be run when the process receives SIGTERM.
// This allows the program to change its behavior during the lameduck period.
//
// All hooks are run in parallel, and the process will do its best to wait
// (up to -onterm_timeout) for all of them to finish before dying.
//
// See also: OnTerm
func OnTermSync(f func()) {
	onTermSyncHooks.Add(f)
}

// fireOnTermSyncHooks returns true iff all the hooks finish before the timeout.
func fireOnTermSyncHooks(timeout time.Duration) bool {
	return fireHooksWithTimeout(timeout, "OnTermSync", onTermSyncHooks.Fire)
}

// fireOnCloseHooks returns true iff all the hooks finish before the timeout.
func fireOnCloseHooks(timeout time.Duration) bool {
	return fireHooksWithTimeout(timeout, "OnClose", func() {
		onCloseHooks.Fire()
		ListeningURL = url.URL{}
	})
}

// fireHooksWithTimeout returns true iff all the hooks finish before the timeout.
func fireHooksWithTimeout(timeout time.Duration, name string, hookFn func()) bool {
	defer log.Flush()
	log.Infof("Firing %s hooks and waiting up to %v for them", name, timeout)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{})
	go func() {
		hookFn()
		close(done)
	}()

	select {
	case <-done:
		log.Infof("%s hooks finished", name)
		return true
	case <-timer.C:
		log.Infof("%s hooks timed out", name)
		return false
	}
}

// OnRun registers f to be run right at the beginning of Run. All
// hooks are run in parallel.
func OnRun(f func()) {
	onRunHooks.Add(f)
}

// FireRunHooks fires the hooks registered by OnHook.
// Use this in a non-server to run the hooks registered
// by servenv.OnRun().
func FireRunHooks() {
	onRunHooks.Fire()
}

// RegisterDefaultFlags registers the default flags for
// listening to a given port for standard connections.
// If calling this, then call RunDefault()
func RegisterDefaultFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.IntVar(&port, "port", port, "port for the server")
	})
}

// Port returns the value of the `--port` flag.
func Port() int {
	return port
}

// RunDefault calls Run() with the parameters from the flags.
func RunDefault() {
	Run(port)
}

var (
	flagHooksM      sync.Mutex
	globalFlagHooks = []func(*pflag.FlagSet){
		vterrors.RegisterFlags,
	}
	commandFlagHooks = map[string][]func(*pflag.FlagSet){}
)

// OnParse registers a callback function to register flags on the flagset that are
// used by any caller of servenv.Parse or servenv.ParseWithArgs.
func OnParse(f func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()

	globalFlagHooks = append(globalFlagHooks, f)
}

// OnParseFor registers a callback function to register flags on the flagset
// used by servenv.Parse or servenv.ParseWithArgs. The provided callback will
// only be called if the `cmd` argument passed to either Parse or ParseWithArgs
// exactly matches the `cmd` argument passed to OnParseFor.
//
// To register for flags for multiple commands, for example if a package's flags
// should be used for only vtgate and vttablet but no other binaries, call this
// multiple times with the same callback function. To register flags for all
// commands globally, use OnParse instead.
func OnParseFor(cmd string, f func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()

	commandFlagHooks[cmd] = append(commandFlagHooks[cmd], f)
}

func getFlagHooksFor(cmd string) (hooks []func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()

	hooks = append(hooks, globalFlagHooks...) // done deliberately to copy the slice

	if commandHooks, ok := commandFlagHooks[cmd]; ok {
		hooks = append(hooks, commandHooks...)
	}

	return hooks
}

// ParseFlags initializes flags and handles the common case when no positional
// arguments are expected.
func ParseFlags(cmd string) {
	fs := GetFlagSetFor(cmd)

	_flag.Parse(fs)

	if version {
		AppVersion.Print()
		os.Exit(0)
	}

	args := fs.Args()
	if len(args) > 0 {
		_flag.Usage()
		log.Exitf("%s doesn't take any positional arguments, got '%s'", cmd, strings.Join(args, " "))
	}

	logutil.PurgeLogs()
}

// GetFlagSetFor returns the flag set for a given command.
// This has to exported for the Vitess-operator to use
func GetFlagSetFor(cmd string) *pflag.FlagSet {
	fs := pflag.NewFlagSet(cmd, pflag.ExitOnError)
	for _, hook := range getFlagHooksFor(cmd) {
		hook(fs)
	}

	return fs
}

// ParseFlagsWithArgs initializes flags and returns the positional arguments
func ParseFlagsWithArgs(cmd string) []string {
	fs := GetFlagSetFor(cmd)

	_flag.Parse(fs)

	if version {
		AppVersion.Print()
		os.Exit(0)
	}

	args := fs.Args()
	if len(args) == 0 {
		log.Exitf("%s expected at least one positional argument", cmd)
	}

	logutil.PurgeLogs()

	return args
}

// Flag installations for packages that servenv imports. We need to register
// here rather than in those packages (which is what we would normally do)
// because that would create a dependency cycle.
func init() {
	// These are the binaries that call trace.StartTracing.
	for _, cmd := range []string{
		"vtadmin",
		"vtclient",
		"vtcombo",
		"vtctl",
		"vtctlclient",
		"vtctld",
		"vtgate",
		"vttablet",
	} {
		OnParseFor(cmd, trace.RegisterFlags)
	}

	// These are the binaries that make gRPC calls.
	for _, cmd := range []string{
		"vtbackup",
		"vtcombo",
		"vtctl",
		"vtctlclient",
		"vtctld",
		"vtgate",
		"vtgateclienttest",
		"vtgr",
		"vtorc",
		"vttablet",
		"vttestserver",
	} {
		OnParseFor(cmd, grpccommon.RegisterFlags)
	}

	// These are the binaries that export stats
	for _, cmd := range []string{
		"vtbackup",
		"vtcombo",
		"vtctld",
		"vtgate",
		"vtgr",
		"vttablet",
		"vtorc",
	} {
		OnParseFor(cmd, stats.RegisterFlags)
	}

	// Flags in package log are installed for all binaries.
	OnParse(log.RegisterFlags)
	// Flags in package logutil are installed for all binaries.
	OnParse(logutil.RegisterFlags)
}

func RegisterFlagsForTopoBinaries(registerFlags func(fs *pflag.FlagSet)) {
	topoBinaries := []string{
		"vtbackup",
		"vtcombo",
		"vtctl",
		"vtctld",
		"vtgate",
		"vtgr",
		"vttablet",
		"vttestserver",
		"zk",
		"vtorc",
	}
	for _, cmd := range topoBinaries {
		OnParseFor(cmd, registerFlags)
	}
}
