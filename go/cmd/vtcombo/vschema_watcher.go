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

package main

import (
	"context"
	"encoding/json"
	"os"
	"path"

	"vitess.io/vitess/go/vt/log"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/topo"
)

func startVschemaWatcher(vschemaPersistenceDir string, keyspaces []*vttestpb.Keyspace, ts *topo.Server) {
	// Create the directory if it doesn't exist.
	if err := createDirectoryIfNotExists(vschemaPersistenceDir); err != nil {
		log.Fatalf("Unable to create vschema persistence directory %v: %v", vschemaPersistenceDir, err)
	}

	// If there are keyspace files, load them.
	loadKeyspacesFromDir(vschemaPersistenceDir, keyspaces, ts)

	// Rebuild the SrvVSchema object in case we loaded vschema from file
	if err := ts.RebuildSrvVSchema(context.Background(), tpb.Cells); err != nil {
		log.Fatalf("RebuildSrvVSchema failed: %v", err)
	}

	// Now watch for changes in the SrvVSchema object and persist them to disk.
	go watchSrvVSchema(context.Background(), ts, tpb.Cells[0])
}

func loadKeyspacesFromDir(dir string, keyspaces []*vttestpb.Keyspace, ts *topo.Server) {
	for _, ks := range tpb.Keyspaces {
		ksFile := path.Join(dir, ks.Name+".json")
		if _, err := os.Stat(ksFile); err == nil {
			jsonData, err := os.ReadFile(ksFile)
			if err != nil {
				log.Fatalf("Unable to read keyspace file %v: %v", ksFile, err)
			}

			keyspace := &vschemapb.Keyspace{}
			err = json.Unmarshal(jsonData, keyspace)
			if err != nil {
				log.Fatalf("Unable to parse keyspace file %v: %v", ksFile, err)
			}

			ts.SaveVSchema(context.Background(), ks.Name, keyspace)
			log.Infof("Loaded keyspace %v from %v\n", ks.Name, ksFile)
		}
	}
}

func watchSrvVSchema(ctx context.Context, ts *topo.Server, cell string) {
	data, ch, err := ts.WatchSrvVSchema(context.Background(), tpb.Cells[0])
	if err != nil {
		log.Fatalf("WatchSrvVSchema failed: %v", err)
	}

	if data.Err != nil {
		log.Fatalf("WatchSrvVSchema could not retrieve initial vschema: %v", data.Err)
	}
	persistNewSrvVSchema(data.Value)

	for update := range ch {
		if update.Err != nil {
			log.Errorf("WatchSrvVSchema returned an error: %v", update.Err)
		} else {
			persistNewSrvVSchema(update.Value)
		}
	}
}

func persistNewSrvVSchema(srvVSchema *vschemapb.SrvVSchema) {
	for ksName, ks := range srvVSchema.Keyspaces {
		jsonBytes, err := json.MarshalIndent(ks, "", "  ")
		if err != nil {
			log.Errorf("Error marshaling keyspace: %v", err)
			continue
		}

		err = os.WriteFile(path.Join(*vschemaPersistenceDir, ksName+".json"), jsonBytes, 0644)
		if err != nil {
			log.Errorf("Error writing keyspace file: %v", err)
		}
		log.Infof("Persisted keyspace %v to %v", ksName, *vschemaPersistenceDir)
	}
}

func createDirectoryIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.Mkdir(dir, 0755)
	}
	return nil
}
