/*
Copyright 2017 Google Inc.

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
	"errors"
	"flag"
	"os"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we do't want to depend on them at all.
var (
	actionTimeout = flag.Duration("action_timeout", time.Hour, "timeout for the total command")
	server        = flag.String("server", "", "server to use for connection")
)

func main() {
	defer exit.Recover()

	flag.Parse()

	closer := trace.StartTracing("vtctlclient")
	defer trace.LogErrorsWhenClosing(closer)

	logger := logutil.NewConsoleLogger()

	// We can't do much without a -server flag
	if *server == "" {
		log.Error(errors.New("Please specify -server <vtctld_host:vtctld_port> to specify the vtctld server to connect to"))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *actionTimeout)
	defer cancel()

	err := vtctlclient.RunCommandAndWait(
		ctx, *server, flag.Args(),
		func(e *logutilpb.Event) {
			logutil.LogEvent(logger, e)
		})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
