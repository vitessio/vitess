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
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/viperutil"
	viperdebug "vitess.io/vitess/go/viperutil/debug"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"

	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
	_flag "vitess.io/vitess/go/internal/flag"
)

var (
	// port is part of the flags used when calling RegisterDefaultFlags.
	port        int
	bindAddress string

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
	lameduckPeriod       = 50 * time.Millisecond
	onTermTimeout        = 10 * time.Second
	onCloseTimeout       = 10 * time.Second
	catchSigpipe         bool
	maxStackSize         = 64 * 1024 * 1024
	initStartTime        time.Time // time when tablet init started: for debug purposes to time how long a tablet init takes
	tableRefreshInterval int
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
		fs.IntVar(&tableRefreshInterval, "table-refresh-interval", tableRefreshInterval, "interval in milliseconds to refresh tables in status page with refreshRequired class")

		// pid_file.go
		fs.StringVar(&pidFile, "pid_file", pidFile, "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")
	})
}

func GetInitStartTime() time.Time {
	mu.Lock()
	defer mu.Unlock()
	return initStartTime
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
		fs.StringVar(&bindAddress, "bind-address", bindAddress, "Bind address for the server. If empty, the server will listen on all available unicast and anycast IP addresses of the local system.")
	})
}

// Port returns the value of the `--port` flag.
func Port() int {
	return port
}

// RunDefault calls Run() with the parameters from the flags.
func RunDefault() {
	Run(bindAddress, port)
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

// Needed because some tests require multiple parse passes, so we guard against
// that here.
var debugConfigRegisterOnce sync.Once

// ParseFlags initializes flags and handles the common case when no positional
// arguments are expected.
func ParseFlags(cmd string) {
	fs := GetFlagSetFor(cmd)

	viperutil.BindFlags(fs)

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

	loadViper(cmd)

	logutil.PurgeLogs()
}

// ParseFlagsForTests initializes flags but skips the version, filesystem
// args and go flag related work.
// Note: this should not be used outside of unit tests.
func ParseFlagsForTests(cmd string) {
	fs := GetFlagSetFor(cmd)
	pflag.CommandLine = fs
	pflag.Parse()
	viperutil.BindFlags(fs)
	loadViper(cmd)
}

// MoveFlagsToCobraCommand moves the servenv-registered flags to the flagset of
// the given cobra command, then copies over the glog flags that otherwise
// require manual transferring.
func MoveFlagsToCobraCommand(cmd *cobra.Command) {
	moveFlags(cmd.Use, cmd.Flags())
}

// MovePersistentFlagsToCobraCommand functions exactly like MoveFlagsToCobraCommand,
// but moves the servenv-registered flags to the persistent flagset of
// the given cobra command, then copies over the glog flags that otherwise
// require manual transferring.
//
// Useful for transferring flags to a parent command whose subcommands should
// inherit the servenv-registered flags.
func MovePersistentFlagsToCobraCommand(cmd *cobra.Command) {
	moveFlags(cmd.Use, cmd.PersistentFlags())
}

func moveFlags(name string, fs *pflag.FlagSet) {
	fs.AddFlagSet(GetFlagSetFor(name))

	// glog flags, no better way to do this
	_flag.PreventGlogVFlagFromClobberingVersionFlagShorthand(fs)
	fs.AddGoFlag(flag.Lookup("logtostderr"))
	fs.AddGoFlag(flag.Lookup("log_backtrace_at"))
	fs.AddGoFlag(flag.Lookup("alsologtostderr"))
	fs.AddGoFlag(flag.Lookup("stderrthreshold"))
	fs.AddGoFlag(flag.Lookup("log_dir"))
	fs.AddGoFlag(flag.Lookup("vmodule"))

	pflag.CommandLine = fs
}

// CobraPreRunE returns the common function that commands will need to load
// viper infrastructure. It matches the signature of cobra's (Pre|Post)RunE-type
// functions.
func CobraPreRunE(cmd *cobra.Command, args []string) error {
	_flag.TrickGlog()

	watchCancel, err := viperutil.LoadConfig()
	if err != nil {
		return fmt.Errorf("%s: failed to read in config: %s", cmd.Name(), err)
	}

	OnTerm(watchCancel)
	HTTPHandleFunc("/debug/config", viperdebug.HandlerFunc)

	logutil.PurgeLogs()

	return nil
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

	viperutil.BindFlags(fs)

	_flag.Parse(fs)

	if version {
		AppVersion.Print()
		os.Exit(0)
	}

	args := fs.Args()
	if len(args) == 0 {
		log.Exitf("%s expected at least one positional argument", cmd)
	}

	loadViper(cmd)

	logutil.PurgeLogs()

	return args
}

func loadViper(cmd string) {
	watchCancel, err := viperutil.LoadConfig()
	if err != nil {
		log.Exitf("%s: failed to read in config: %s", cmd, err.Error())
	}
	OnTerm(watchCancel)
	debugConfigRegisterOnce.Do(func() {
		HTTPHandleFunc("/debug/config", viperdebug.HandlerFunc)
	})
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
		"vttablet",
		"vtorc",
	} {
		OnParseFor(cmd, stats.RegisterFlags)
	}

	// Flags in package log are installed for all binaries.
	OnParse(log.RegisterFlags)
	// Flags in package logutil are installed for all binaries.
	OnParse(logutil.RegisterFlags)
	// Flags in package viperutil/config are installed for all binaries.
	OnParse(viperutil.RegisterFlags)
}

func RegisterFlagsForTopoBinaries(registerFlags func(fs *pflag.FlagSet)) {
	topoBinaries := []string{
		"vtbackup",
		"vtcombo",
		"vtctl",
		"vtctld",
		"vtgate",
		"vttablet",
		"vttestserver",
		"zk",
		"vtorc",
	}
	for _, cmd := range topoBinaries {
		OnParseFor(cmd, registerFlags)
	}
}

// TestingEndtoend is true when this Vitess binary is being ran as part of an endtoend test suite
var TestingEndtoend = false

func init() {
	TestingEndtoend = os.Getenv("VTTEST") == "endtoend"
}
