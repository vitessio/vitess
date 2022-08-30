/*
   Copyright 2014 Outbrain Inc.

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

package main

import (
	"os"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/pflag"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/vt/log"
	vtlog "vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/orchestrator/app"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	GitCommit  string
	AppVersion string
)

// transformArgsForPflag turns a slice of raw args passed on the command line,
// possibly incompatible with pflag (because the user is expecting stdlib flag
// parsing behavior) and transforms them into the arguments that should have
// been passed to conform to pflag parsing behavior.
//
// the primary function is to catch any cases where the user specified a longopt
// with only a single hyphen (e.g. `-myflag`) and correct it to be
// double-hyphenated.
//
// note that this transformation does _not_ actually validate the arguments; for
// example if the user specifies `--myflag`, but the FlagSet has no such flag
// defined, that will still appear in the returned result and will (correctly)
// cause a parse error later on in `main`, at which point the CLI usage will
// be printed.
//
// note also that this transformation is incomplete. pflag allows interspersing
// of flag and positional arguments, whereas stdlib flag does not. however, for
// vtorc specifically, with the exception of `vtorc help <topic>`, the CLI only
// consumes flag arguments (in other words, there are no supported subcommands),
// so this is a non-issue, and is not implemented here in order to make this
// function a bit simpler.
func transformArgsForPflag(fs *pflag.FlagSet, args []string) (result []string) {
	for i, arg := range args {
		switch {
		case arg == "--":
			// pflag stops parsing at `--`, so we're done transforming the CLI
			// arguments. Just append everything remaining and be done.
			result = append(result, args[i:]...)
			return result
		case strings.HasPrefix(arg, "--"):
			// Long-hand flag. Append it and continue.
			result = append(result, arg)
		case strings.HasPrefix(arg, "-"):
			// Most complex case. This is either:
			// 1. A legacy long-hand flag that needs a double-dash (e.g. `-myflag` => `--myflag`).
			// 2. One _or more_ pflag shortopts all shoved together (think `rm -rf` as `rm -r -f`).
			//
			// In the latter case, we don't need to do any transformations, but
			// in the former, we do.
			name := strings.SplitN(arg[1:], "=", 2)[0] // discard any potential value (`-myflag` and `-myflag=10` both have the name of `myflag`)
			if fs.Lookup(name) != nil {
				// Case 1: We have a long opt with this name, so we need to
				// prepend an additional hyphen.
				result = append(result, "-"+arg)
			} else {
				// Case 2: No transformation needed.
				result = append(result, arg)
			}
		default:
			// Just a flag argument. Nothing to transform.
			result = append(result, arg)
		}
	}

	return result
}

// main is the application's entry point. It will spawn an HTTP interface.
func main() {
	// TODO(ajm188): after v15, remove this pflag hack and use servenv.ParseFlags
	// directly.
	fs := pflag.NewFlagSet("vtorc", pflag.ExitOnError)
	vtlog.RegisterFlags(fs)
	logutil.RegisterFlags(fs)

	args := append([]string{}, os.Args...)
	os.Args = os.Args[0:1]
	servenv.ParseFlags("vtorc")

	// N.B. This code has to be duplicated from go/internal/flag in order to
	// correctly transform `-help` => `--help`. Otherwise passing `-help` would
	// produce:
	//	unknown shorthand flag: 'e' in -elp
	var help bool
	if fs.Lookup("help") == nil {
		if fs.ShorthandLookup("h") == nil {
			fs.BoolVarP(&help, "help", "h", false, "display usage and exit")
		} else {
			fs.BoolVar(&help, "help", false, "display usage and exit")
		}
	}

	configFile := fs.String("config", "", "config file name")
	sibling := fs.StringP("sibling", "s", "", "sibling instance, host_fqdn[:port]")
	destination := fs.StringP("destination", "d", "", "destination instance, host_fqdn[:port] (synonym to -s)")
	discovery := fs.Bool("discovery", true, "auto discovery mode")
	config.RuntimeCLIFlags.SkipUnresolve = fs.Bool("skip-unresolve", false, "Do not unresolve a host name")
	config.RuntimeCLIFlags.SkipUnresolveCheck = fs.Bool("skip-unresolve-check", false, "Skip/ignore checking an unresolve mapping (via hostname_unresolve table) resolves back to same hostname")
	config.RuntimeCLIFlags.Noop = fs.Bool("noop", false, "Dry run; do not perform destructing operations")
	config.RuntimeCLIFlags.BinlogFile = fs.String("binlog", "", "Binary log file name")
	config.RuntimeCLIFlags.Statement = fs.String("statement", "", "Statement/hint")
	config.RuntimeCLIFlags.GrabElection = fs.Bool("grab-election", false, "Grab leadership (only applies to continuous mode)")
	config.RuntimeCLIFlags.PromotionRule = fs.String("promotion-rule", "prefer", "Promotion rule for register-andidate (prefer|neutral|prefer_not|must_not)")
	config.RuntimeCLIFlags.SkipContinuousRegistration = fs.Bool("skip-continuous-registration", false, "Skip cli commands performaing continuous registration (to reduce orchestratrator backend db load")
	config.RuntimeCLIFlags.EnableDatabaseUpdate = fs.Bool("enable-database-update", false, "Enable database update, overrides SkipOrchestratorDatabaseUpdate")
	config.RuntimeCLIFlags.IgnoreRaftSetup = fs.Bool("ignore-raft-setup", false, "Override RaftEnabled for CLI invocation (CLI by default not allowed for raft setups). NOTE: operations by CLI invocation may not reflect in all raft nodes.")
	config.RuntimeCLIFlags.Tag = fs.String("tag", "", "tag to add ('tagname' or 'tagname=tagvalue') or to search ('tagname' or 'tagname=tagvalue' or comma separated 'tag0,tag1=val1,tag2' for intersection of all)")

	os.Args = append(os.Args, transformArgsForPflag(fs, args[1:])...)
	if !reflect.DeepEqual(args, os.Args) {
		// warn the user so they can adjust their CLI scripts
		warning := `CLI args passed do not conform to pflag parsing behavior
The arguments have been transformed for compatibility as follows:
	%v => %v
Please update your scripts before the next version, when this will begin to break.
`
		log.Warningf(warning, args, os.Args)
	}

	_flag.Parse(fs)
	// N.B. Also duplicated from go/internal/flag, for the same reason as above.
	if help || fs.Arg(0) == "help" {
		pflag.Usage()
		os.Exit(0)
	}

	if *destination != "" && *sibling != "" {
		log.Fatalf("-s and -d are synonyms, yet both were specified. You're probably doing the wrong thing.")
	}
	switch *config.RuntimeCLIFlags.PromotionRule {
	case "prefer", "neutral", "prefer_not", "must_not":
		{
			// OK
		}
	default:
		{
			log.Fatalf("--promotion-rule only supports prefer|neutral|prefer_not|must_not")
		}
	}
	if *destination == "" {
		*destination = *sibling
	}

	startText := "starting orchestrator"
	if AppVersion != "" {
		startText += ", version: " + AppVersion
	}
	if GitCommit != "" {
		startText += ", git commit: " + GitCommit
	}
	log.Info(startText)

	if len(*configFile) > 0 {
		config.ForceRead(*configFile)
	} else {
		config.Read("/etc/orchestrator.conf.json", "conf/orchestrator.conf.json", "orchestrator.conf.json")
	}
	if *config.RuntimeCLIFlags.EnableDatabaseUpdate {
		config.Config.SkipOrchestratorDatabaseUpdate = false
	}
	if config.Config.AuditToSyslog {
		inst.EnableAuditSyslog()
	}
	config.RuntimeCLIFlags.ConfiguredVersion = AppVersion
	config.MarkConfigurationLoaded()

	app.HTTP(*discovery)
}
