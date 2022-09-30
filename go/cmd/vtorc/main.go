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

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/log"
	vtlog "vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
	"vitess.io/vitess/go/vt/vtorc/server"
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
			if fs.Lookup(name) != nil || name == "help" {
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
	grpccommon.RegisterFlags(fs)
	vtlog.RegisterFlags(fs)
	logutil.RegisterFlags(fs)
	logic.RegisterFlags(fs)
	server.RegisterFlags(fs)
	config.RegisterFlags(fs)
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	acl.RegisterFlags(fs)
	servenv.OnParseFor("vtorc", func(flags *pflag.FlagSet) { flags.AddFlagSet(fs) })

	args := append([]string{}, os.Args...)
	os.Args = os.Args[0:1]

	configFile := fs.String("config", "", "config file name")

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

	servenv.ParseFlags("vtorc")
	config.UpdateConfigValuesFromFlags()

	log.Info("starting vtorc")
	if len(*configFile) > 0 {
		config.ForceRead(*configFile)
	} else {
		config.Read("/etc/vtorc.conf.json", "conf/vtorc.conf.json", "vtorc.conf.json")
	}
	if config.Config.AuditToSyslog {
		inst.EnableAuditSyslog()
	}
	config.MarkConfigurationLoaded()

	// Log final config values to debug if something goes wrong.
	config.LogConfigValues()
	server.StartVTOrcDiscovery()

	server.RegisterVTOrcAPIEndpoints()
	servenv.OnRun(func() {
		addStatusParts()
	})

	// For backward compatability, we require that VTOrc functions even when the --port flag is not provided.
	// In this case, it should function like before but without the servenv pages.
	// Therefore, currently we don't check for the --port flag to be necessary, but release 16+ that check
	// can be added to always have the serenv page running in VTOrc.
	servenv.RunDefault()
}
