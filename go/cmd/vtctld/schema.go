package main

import (
	"flag"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/schemamanager"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
)

var (
	schemaChangeDir           = flag.String("schema_change_dir", "", "directory contains schema changes for all keyspaces. Each keyspace has its own directory and schema changes are expected to live in '$KEYSPACE/input' dir. e.g. test_keyspace/input/*sql, each sql file represents a schema change")
	schemaChangeController    = flag.String("schema_change_controller", "", "schema change controller is responsible for finding schema changes and responsing schema change events")
	schemaChangeCheckInterval = flag.Int("schema_change_check_interval", 60, "this value decides how often we check schema change dir, in seconds")
	schemaChangeUser          = flag.String("schema_change_user", "", "The user who submits this schema change.")
)

func initSchema() {
	// Start schema manager service if needed.
	if *schemaChangeDir != "" {
		interval := 60
		if *schemaChangeCheckInterval > 0 {
			interval = *schemaChangeCheckInterval
		}
		timer := timer.NewTimer(time.Duration(interval) * time.Second)
		controllerFactory, err :=
			schemamanager.GetControllerFactory(*schemaChangeController)
		if err != nil {
			log.Fatalf("unable to get a controller factory, error: %v", err)
		}

		timer.Start(func() {
			controller, err := controllerFactory(map[string]string{
				schemamanager.SchemaChangeDirName: *schemaChangeDir,
				schemamanager.SchemaChangeUser:    *schemaChangeUser,
			})
			if err != nil {
				log.Errorf("failed to get controller, error: %v", err)
				return
			}
			ctx := context.Background()
			err = schemamanager.Run(
				ctx,
				controller,
				schemamanager.NewTabletExecutor(
					tmclient.NewTabletManagerClient(), ts),
			)
			if err != nil {
				log.Errorf("Schema change failed, error: %v", err)
			}
		})
		servenv.OnClose(func() { timer.Stop() })
	}
}
