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
	"flag"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
)

var (
	client        vtctldclient.VtctldClient
	traceCloser   io.Closer
	commandCtx    context.Context
	commandCancel func()

	server        string
	actionTimeout time.Duration

	// We use cobra to make subcommands easier to manage. And do a hack below
	// in main to grab the rest of the flags globally scattered to make sure we
	// pick up things like common servenv flags, tracing flags, etc. Refer to
	// commands.go for all of the subcommands.
	rootCmd = &cobra.Command{
		// We use PersistentPreRun to set up the tracer, grpc client, and
		// command context for every command.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			traceCloser = trace.StartTracing("vtctldclient")
			if server == "" {
				err = errors.New("please specify -server <vtctld_host:vtctld_port> to specify the vtctld server to connect to")
				log.Error(err)
				return err
			}

			client, err = vtctldclient.New("grpc", server)

			commandCtx, commandCancel = context.WithTimeout(context.Background(), actionTimeout)
			return err
		},
		// Similarly, PersistentPostRun cleans up the resources spawned by
		// PersistentPreRun.
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			commandCancel()
			err := client.Close()
			trace.LogErrorsWhenClosing(traceCloser)
			return err
		},
		TraverseChildren: true,
	}
)

func main() {
	defer exit.Recover()

	// Grab all those global flags across the codebase and shove 'em on in.
	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	// Attach our local flags
	rootCmd.PersistentFlags().StringVar(&server, "server", "", "server to use for connection")
	rootCmd.PersistentFlags().DurationVar(&actionTimeout, "action_timeout", time.Hour, "timeout for the total command")

	// hack to get rid of an "ERROR: logging before flag.Parse"
	args := os.Args[:]
	os.Args = os.Args[:1]
	flag.Parse()
	os.Args = args

	// back to your regularly scheduled cobra programming
	if err := rootCmd.Execute(); err != nil {
		log.Error(err)
		exit.Return(1)
	}
}
