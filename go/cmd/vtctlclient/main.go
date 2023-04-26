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

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we don't want to depend on them at all.
var (
	actionTimeout = time.Hour
	server        string
)

func init() {
	servenv.OnParse(func(fs *pflag.FlagSet) {
		fs.DurationVar(&actionTimeout, "action_timeout", actionTimeout, "timeout for the total command")
		fs.StringVar(&server, "server", server, "server to use for connection")

		acl.RegisterFlags(fs)
	})
}

// checkDeprecations runs quick and dirty checks to see whether any command or flag are deprecated.
// For any depracated command or flag, the function issues a warning message.
// this function will change on each Vitess version. Each depracation message should only last a version.
// VEP-4 will replace the need for this function. See https://github.com/vitessio/enhancements/blob/main/veps/vep-4.md
func checkDeprecations(args []string) {
	// utility:
	// name this to findSubstring if you need to use it
	_ = func(s string) (arg string, ok bool) {
		for _, arg := range args {
			if strings.Contains(arg, s) {
				return arg, true
			}
		}
		return "", false
	}
}

func main() {
	defer exit.Recover()

	args := servenv.ParseFlagsWithArgs("vtctlclient")

	closer := trace.StartTracing("vtctlclient")
	defer trace.LogErrorsWhenClosing(closer)

	logger := logutil.NewConsoleLogger()

	// We can't do much without a --server flag
	if server == "" {
		log.Error(errors.New("please specify --server <vtctld_host:vtctld_port> to specify the vtctld server to connect to"))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), actionTimeout)
	defer cancel()

	checkDeprecations(args)

	err := vtctlclient.RunCommandAndWait(ctx, server, args, func(e *logutilpb.Event) {
		logutil.LogEvent(logger, e)
	})
	if err != nil {
		if strings.Contains(err.Error(), "flag: help requested") {
			return
		}

		errStr := strings.Replace(err.Error(), "remote error: ", "", -1)
		fmt.Printf("%s Error: %s\n", args[0], errStr)
		log.Error(err)
		os.Exit(1)
	}
}
