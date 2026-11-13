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
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func startVschemaWatcher(ctx context.Context, vschemaPersistenceDir string, ts *topo.Server) {
	// Create the directory if it doesn't exist.
	if err := createDirectoryIfNotExists(vschemaPersistenceDir); err != nil {
		log.Error("Unable to create vschema persistence directory", slog.String("dir", vschemaPersistenceDir), slog.Any("error", err))
		os.Exit(1)
	}

	// If there are keyspace files, load them.
	loadKeyspacesFromDir(ctx, vschemaPersistenceDir, ts)

	// Rebuild the SrvVSchema object in case we loaded vschema from file
	if err := ts.RebuildSrvVSchema(ctx, tpb.Cells); err != nil {
		log.Error("RebuildSrvVSchema failed", slog.Any("error", err))
		os.Exit(1)
	}

	// Now watch for changes in the SrvVSchema object and persist them to disk.
	go watchSrvVSchema(ctx, ts, tpb.Cells[0])
}

func loadKeyspacesFromDir(ctx context.Context, dir string, ts *topo.Server) {
	for _, ks := range tpb.Keyspaces {
		ksFile := path.Join(dir, ks.Name+".json")
		if _, err := os.Stat(ksFile); err == nil {
			jsonData, err := os.ReadFile(ksFile)
			if err != nil {
				log.Error("Unable to read keyspace file", slog.String("path", ksFile), slog.Any("error", err))
				os.Exit(1)
			}

			ksvs := &topo.KeyspaceVSchemaInfo{
				Name:     ks.Name,
				Keyspace: &vschemapb.Keyspace{},
			}
			err = json.Unmarshal(jsonData, ksvs.Keyspace)
			if err != nil {
				log.Error("Unable to parse keyspace file", slog.String("path", ksFile), slog.Any("error", err))
				os.Exit(1)
			}

			_, err = vindexes.BuildKeyspace(ksvs.Keyspace, env.Parser())
			if err != nil {
				log.Error("Invalid keyspace definition",
					slog.String("keyspace", ks.Name),
					slog.Any("error", err),
					slog.String("error_code", vterrors.Code(err).String()))
				os.Exit(1)
			}
			ts.SaveVSchema(ctx, ksvs)
			log.Info("Loaded keyspace from path", slog.String("keyspace", ks.Name), slog.String("path", ksFile))
		}
	}
}

func watchSrvVSchema(ctx context.Context, ts *topo.Server, cell string) {
	data, ch, err := ts.WatchSrvVSchema(ctx, tpb.Cells[0])
	if err != nil {
		log.Error("WatchSrvVSchema failed", slog.Any("error", err))
		os.Exit(1)
	}

	if data.Err != nil {
		log.Error("WatchSrvVSchema could not retrieve initial vschema", slog.Any("error", data.Err))
		os.Exit(1)
	}
	persistNewSrvVSchema(data.Value)

	for update := range ch {
		if update.Err != nil {
			log.Error("WatchSrvVSchema returned an error", slog.Any("error", update.Err))
		} else {
			persistNewSrvVSchema(update.Value)
		}
	}
}

func persistNewSrvVSchema(srvVSchema *vschemapb.SrvVSchema) {
	for ksName, ks := range srvVSchema.Keyspaces {
		jsonBytes, err := json.MarshalIndent(ks, "", "  ")
		if err != nil {
			log.Error("Error marshaling keyspace", slog.String("keyspace", ksName), slog.Any("error", err))
			continue
		}

		ksFile := path.Join(vschemaPersistenceDir, ksName+".json")
		err = os.WriteFile(ksFile, jsonBytes, 0o644)
		if err != nil {
			log.Error("Error writing keyspace file", slog.String("keyspace", ksName), slog.String("path", ksFile), slog.Any("error", err))
		}
		log.Info("Persisted keyspace to dir", slog.String("keyspace", ksName), slog.String("dir", vschemaPersistenceDir))
	}
}

func createDirectoryIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.Mkdir(dir, 0o755)
	}
	return nil
}
