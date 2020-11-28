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
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we do't want to depend on them at all.
//
// (TODO:@amason) - These flags are also duplicated. This also only works for
// GetKeyspaces, because I cannot currently define subcommands and also have
// them take the global flags scattered throughout the codebase.
var (
	actionTimeout = flag.Duration("action_timeout", time.Hour, "timeout for the total command")
	server        = flag.String("server", "", "server to use for connection")
)

func main() {
	defer exit.Recover()

	// fs := flag.NewFlagSet("vtctldclient", flag.PanicOnError)
	// flag.CommandLine.VisitAll(func(f *flag.Flag) {
	// 	fs.Var(f.Value, f.Name, f.Usage)
	// })

	// flag.CommandLine = fs

	flag.Parse()

	closer := trace.StartTracing("vtctldclient")
	defer trace.LogErrorsWhenClosing(closer)

	// We can't do much without a -server flag
	if *server == "" {
		log.Error(errors.New("please specify -server <vtctld_host:vtctld_port> to specify the vtctld server to connect to"))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *actionTimeout)
	defer cancel()

	client, err := vtctldclient.New("grpc", *server)
	if err != nil {
		log.Fatalf("could not create gRPC client to %s; err = %s", *server, err)
	}
	defer client.Close()

	switch strings.ToLower(flag.Args()[0]) {
	case "getkeyspaces":
		resp, err := client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		if err != nil {
			log.Errorf("GetKeyspaces error: %s", err)
			return
		}
		fmt.Println(strings.Join(resp.Keyspaces, "\n"))
	}
}
