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

package cli

import (
	"context"
	"time"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schemamanager"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	schemaChangeDir             string
	schemaChangeController      string
	schemaChangeUser            string
	schemaChangeCheckInterval   = time.Minute
	schemaChangeReplicasTimeout = grpcvtctldserver.DefaultWaitReplicasTimeout
)

func init() {
	Main.Flags().StringVar(&schemaChangeDir, "schema_change_dir", schemaChangeDir, "Directory containing schema changes for all keyspaces. Each keyspace has its own directory, and schema changes are expected to live in '$KEYSPACE/input' dir. (e.g. 'test_keyspace/input/*sql'). Each sql file represents a schema change.")
	Main.Flags().StringVar(&schemaChangeController, "schema_change_controller", schemaChangeController, "Schema change controller is responsible for finding schema changes and responding to schema change events.")
	Main.Flags().StringVar(&schemaChangeUser, "schema_change_user", schemaChangeUser, "The user who schema changes are submitted on behalf of.")

	Main.Flags().DurationVar(&schemaChangeCheckInterval, "schema_change_check_interval", schemaChangeCheckInterval, "How often the schema change dir is checked for schema changes. This value must be positive; if zero or lower, the default of 1m is used.")
	Main.Flags().DurationVar(&schemaChangeReplicasTimeout, "schema_change_replicas_timeout", schemaChangeReplicasTimeout, "How long to wait for replicas to receive a schema change.")
}

func initSchema() {
	// Start schema manager service if needed.
	if schemaChangeDir != "" {
		interval := schemaChangeCheckInterval
		if interval <= 0 {
			interval = time.Minute
		}
		timer := timer.NewTimer(interval)
		controllerFactory, err :=
			schemamanager.GetControllerFactory(schemaChangeController)
		if err != nil {
			log.Fatalf("unable to get a controller factory, error: %v", err)
		}

		timer.Start(func() {
			controller, err := controllerFactory(map[string]string{
				schemamanager.SchemaChangeDirName: schemaChangeDir,
				schemamanager.SchemaChangeUser:    schemaChangeUser,
			})
			if err != nil {
				log.Errorf("failed to get controller, error: %v", err)
				return
			}
			ctx := context.Background()
			wr := wrangler.New(env, logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
			_, err = schemamanager.Run(
				ctx,
				controller,
				schemamanager.NewTabletExecutor("vtctld/schema", wr.TopoServer(), wr.TabletManagerClient(), wr.Logger(), schemaChangeReplicasTimeout, 0, env.Parser()),
			)
			if err != nil {
				log.Errorf("Schema change failed, error: %v", err)
			}
		})
		servenv.OnClose(func() { timer.Stop() })
	}
}
