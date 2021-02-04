/*
Copyright 2021 The Vitess Authors.

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

package command

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/spf13/cobra"

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

	// Root is the main entrypoint to the vtctldclient CLI.
	Root = &cobra.Command{
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
		// By default, cobra will print any error returned by a child command to
		// stderr, and then return that error back up the call chain. Since we
		// use vitess's log package to log any error we get back from
		// Root.Execute() (in ../../main.go) this actually results in duplicate
		// stderr lines. So, somewhat counterintuitively, we actually "silence"
		// all errors in cobra (just from being output, they still get
		// propagated).
		SilenceErrors: true,
	}
)

func init() {
	Root.PersistentFlags().StringVar(&server, "server", "", "server to use for connection")
	Root.PersistentFlags().DurationVar(&actionTimeout, "action_timeout", time.Hour, "timeout for the total command")
}
