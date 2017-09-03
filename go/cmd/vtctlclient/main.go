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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we do't want to depend on them at all.
var (
	actionTimeout = flag.Duration("action_timeout", time.Hour, "timeout for the total command")
	dialTimeout   = flag.Duration("dial_timeout", 30*time.Second, "time to wait for the dial phase")
	server        = flag.String("server", "", "server to use for connection")
)

func main() {
	defer exit.Recover()

	flag.Parse()

	logger := logutil.NewConsoleLogger()

	// We can't do much without a -server flag
	if *server == "" {
		log.Error(errors.New("Please specify -server <vtctld_host:vtctld_port> to specify the vtctld server to connect to"))
		os.Exit(1)
	}

	err := vtctlclient.RunCommandAndWait(
		context.Background(), *server, flag.Args(),
		*dialTimeout, *actionTimeout,
		func(e *logutilpb.Event) {
			logutil.LogEvent(logger, e)
		})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
