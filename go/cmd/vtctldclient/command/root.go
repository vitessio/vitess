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
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	// These imports ensure init()s within them get called and they register their commands/subcommands.
	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	vreplcommon "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/lookupvindex"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/materialize"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/migrate"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/mount"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/movetables"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/reshard"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/vdiff"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/workflow"

	// These imports register the topo factories to use when --server=internal.
	_ "vitess.io/vitess/go/vt/topo/consultopo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"
)

// The --server value if you want to use a "local" vtctld server.
const useInternalVtctld = "internal"

var (
	// VtctldClientProtocol is the protocol to use when creating the vtctldclient.VtctldClient.
	VtctldClientProtocol = "grpc"

	client      vtctldclient.VtctldClient
	traceCloser io.Closer

	commandCtx    context.Context
	commandCancel func()

	// Register functions to be called when the command completes.
	onTerm = []func(){}

	// Register our nil tmclient grpc handler only one time.
	// This is primarily for tests where we execute the root
	// command multiple times.
	once = sync.Once{}

	server        string
	actionTimeout time.Duration
	compactOutput bool

	env *vtenv.Environment

	topoOptions = struct {
		implementation        string
		globalServerAddresses []string
		globalRoot            string
	}{ // Set defaults
		implementation:        "etcd2",
		globalServerAddresses: []string{"localhost:2379"},
		globalRoot:            "/vitess/global",
	}

	// Root is the main entrypoint to the vtctldclient CLI.
	Root = &cobra.Command{
		Use:   "vtctldclient",
		Short: "Executes a cluster management command on the remote vtctld server.",
		Long: fmt.Sprintf(`Executes a cluster management command on the remote vtctld server.
If there are no running vtctld servers -- for example when bootstrapping
a new Vitess cluster -- you can specify a --server value of '%s'.
When doing so, you would use the --topo* flags so that the client can
connect directly to the topo server(s).`, useInternalVtctld),
		// We use PersistentPreRun to set up the tracer, grpc client, and
		// command context for every command.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			logutil.PurgeLogs()
			traceCloser = trace.StartTracing("vtctldclient")
			client, err = getClientForCommand(cmd)
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			commandCtx, commandCancel = context.WithTimeout(ctx, actionTimeout)
			if compactOutput {
				cli.DefaultMarshalOptions.EmitUnpopulated = false
			}
			vreplcommon.SetClient(client)
			vreplcommon.SetCommandCtx(commandCtx)
			return err
		},
		// Similarly, PersistentPostRun cleans up the resources spawned by
		// PersistentPreRun.
		PersistentPostRunE: func(cmd *cobra.Command, args []string) (err error) {
			commandCancel()
			if client != nil {
				err = client.Close()
			}
			// Execute any registered onTerm functions.
			for _, f := range onTerm {
				f()
			}
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
		Version:       servenv.AppVersion.String(),
		// If we've reached this function, it means that:
		//
		// (1) The user specified some positional arguments, which, for the way
		// we've structured things can only be a subcommand name, **and**
		//
		// (2) Cobra was unable to find a subcommand with that name for which to
		// call a Run or RunE function.
		//
		// From this we conclude that the user was trying to either run a
		// command that doesn't exist (e.g. "vtctldclient delete-my-data") or
		// has misspelled a legitimate command (e.g. "vtctldclient StapReplication").
		// If we think this has happened, return an error, which will get
		// displayed to the user in main.go along with the usage.
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().NArg() > 0 {
				return fmt.Errorf("unknown command: %s", cmd.Flags().Arg(0))
			}

			return nil
		},
	}
)

var errNoServer = errors.New("please specify --server <vtctld_host:vtctld_port> to specify the vtctld server to connect to")

const skipClientCreationKey = "skip_client_creation"

// getClientForCommand returns a vtctldclient.VtctldClient for a given command.
// It validates that --server was passed to the CLI for commands that need it.
func getClientForCommand(cmd *cobra.Command) (vtctldclient.VtctldClient, error) {
	if skipStr, ok := cmd.Annotations[skipClientCreationKey]; ok {
		skipClientCreation, err := strconv.ParseBool(skipStr)
		if err != nil {
			skipClientCreation = false
		}

		if skipClientCreation {
			return nil, nil
		}
	}

	// Reserved cobra commands for shell completion that we don't want to fail
	// here.
	switch {
	case cmd.Name() == "__complete", cmd.Parent() != nil && cmd.Parent().Name() == "completion":
		return nil, nil
	}

	if VtctldClientProtocol != "local" && server == "" {
		return nil, errNoServer
	}

	if server == useInternalVtctld {
		ts, err := topo.OpenServer(topoOptions.implementation, strings.Join(topoOptions.globalServerAddresses, ","), topoOptions.globalRoot)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to the topology server: %v", err)
		}
		onTerm = append(onTerm, ts.Close)

		// Use internal vtctld server implementation.
		// Register a nil grpc handler -- we will not use tmclient at all but
		// a factory still needs to be registered.
		once.Do(func() {
			tmclient.RegisterTabletManagerClientFactory("grpc", func() tmclient.TabletManagerClient {
				return nil
			})
		})
		vtctld := grpcvtctldserver.NewVtctldServer(env, ts)
		localvtctldclient.SetServer(vtctld)
		VtctldClientProtocol = "local"
		server = ""
	}

	return vtctldclient.New(VtctldClientProtocol, server)
}

func init() {
	Root.PersistentFlags().StringVar(&server, "server", "", "server to use for the connection (required)")
	Root.PersistentFlags().DurationVar(&actionTimeout, "action_timeout", time.Hour, "timeout to use for the command")
	Root.PersistentFlags().BoolVar(&compactOutput, "compact", false, "use compact format for otherwise verbose outputs")
	Root.PersistentFlags().StringVar(&topoOptions.implementation, "topo-implementation", topoOptions.implementation, "the topology implementation to use")
	Root.PersistentFlags().StringSliceVar(&topoOptions.globalServerAddresses, "topo-global-server-address", topoOptions.globalServerAddresses, "the address of the global topology server(s)")
	Root.PersistentFlags().StringVar(&topoOptions.globalRoot, "topo-global-root", topoOptions.globalRoot, "the path of the global topology data in the global topology server")
	vreplcommon.RegisterCommands(Root)

	var err error
	env, err = vtenv.New(vtenv.Options{
		MySQLServerVersion: servenv.MySQLServerVersion(),
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		log.Fatalf("failed to initialize vtenv: %v", err)
	}
}
