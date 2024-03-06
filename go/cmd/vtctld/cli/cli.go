/*
Copyright 2023 The Vitess Authors.

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

package cli

import (
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctld"
	"vitess.io/vitess/go/vt/vtenv"
)

var (
	ts   *topo.Server
	env  *vtenv.Environment
	Main = &cobra.Command{
		Use:   "vtctld",
		Short: "The Vitess cluster management daemon.",
		Long: `vtctld provides web and gRPC interfaces to manage a single Vitess cluster.
It is usually the first Vitess component to be started after a valid global topology service has been created.

For the last several releases, vtctld has been transitioning to a newer gRPC service for well-typed cluster management requests.
This is **required** to use programs such as vtadmin and vtctldclient, and The old API and service are deprecated and will be removed in a future release.
To enable this newer service, include "grpc-vtctld" in the --service_map argument.
This is demonstrated in the example usage below.`,
		Example: `vtctld \
	--topo_implementation etcd2 \
	--topo_global_server_address localhost:2379 \
	--topo_global_root /vitess/ \
	--service_map 'grpc-vtctl,grpc-vtctld' \
	--backup_storage_implementation file \
	--file_backup_storage_root $VTDATAROOT/backups \
	--port 15000 \
	--grpc_port 15999`,
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
)

func run(cmd *cobra.Command, args []string) error {
	servenv.Init()

	ts = topo.Open()
	defer ts.Close()

	var err error
	env, err = vtenv.New(vtenv.Options{
		MySQLServerVersion: servenv.MySQLServerVersion(),
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		return err
	}
	// Init the vtctld core
	if err := vtctld.InitVtctld(env, ts); err != nil {
		return err
	}

	// Register http debug/health
	vtctld.RegisterDebugHealthHandler(ts)

	// Start schema manager service.
	initSchema()

	// And run the server.
	servenv.RunDefault()

	return nil
}

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()

	servenv.MoveFlagsToCobraCommand(Main)

	acl.RegisterFlags(Main.Flags())
}
