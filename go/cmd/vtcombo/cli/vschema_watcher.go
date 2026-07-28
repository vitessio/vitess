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
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"path"
	"sync"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

type (
	// vschemaPersister writes keyspace vschemas to disk. All writes go through
	// it so that the final flush at shutdown cannot be undone by an older
	// update that the watcher goroutine had not processed yet.
	vschemaPersister struct {
		dir string

		mu sync.Mutex
		// sealed is set once flush has read the authoritative topo state.
		// Watcher updates arriving after that point are dropped, because they
		// can only be equal to or older than what flush read.
		sealed bool
	}
)

func startVschemaWatcher(ctx context.Context, vschemaPersistenceDir string, ts *topo.Server) {
	// Create the directory if it doesn't exist.
	if err := createDirectoryIfNotExists(vschemaPersistenceDir); err != nil {
		log.Error(fmt.Sprintf("Unable to create vschema persistence directory %v: %v", vschemaPersistenceDir, err))
		os.Exit(1)
	}

	// If there are keyspace files, load them.
	loadKeyspacesFromDir(ctx, vschemaPersistenceDir, ts)

	// Rebuild the SrvVSchema object in case we loaded vschema from file
	if err := ts.RebuildSrvVSchema(ctx, tpb.Cells); err != nil {
		log.Error(fmt.Sprintf("RebuildSrvVSchema failed: %v", err))
		os.Exit(1)
	}

	persister := &vschemaPersister{dir: vschemaPersistenceDir}

	// Persisting a vschema change is asynchronous: an `alter vschema` statement
	// returns as soon as the change is in the topo, and the watcher below only
	// writes it to disk once the topo notifies it. Without a flush at shutdown,
	// a change made shortly before the process exits is lost, and the next
	// startup silently comes back with a stale vschema.
	//
	// OnClose runs after the gRPC and MySQL servers have stopped, so no further
	// vschema change can arrive, and both ctx and ts stay valid until after all
	// hooks have fired.
	servenv.OnClose(func() {
		persister.flush(ctx, ts, tpb.Cells[0])
	})

	// Now watch for changes in the SrvVSchema object and persist them to disk.
	go watchSrvVSchema(ctx, persister, ts, tpb.Cells[0])
}

func loadKeyspacesFromDir(ctx context.Context, dir string, ts *topo.Server) {
	for _, ks := range tpb.Keyspaces {
		ksFile := path.Join(dir, ks.Name+".json")
		if _, err := os.Stat(ksFile); err == nil {
			jsonData, err := os.ReadFile(ksFile)
			if err != nil {
				log.Error(fmt.Sprintf("Unable to read keyspace file %v: %v", ksFile, err))
				os.Exit(1)
			}

			ksvs := &topo.KeyspaceVSchemaInfo{
				Name:     ks.Name,
				Keyspace: &vschemapb.Keyspace{},
			}
			err = json.Unmarshal(jsonData, ksvs.Keyspace)
			if err != nil {
				log.Error(fmt.Sprintf("Unable to parse keyspace file %v: %v", ksFile, err))
				os.Exit(1)
			}

			_, err = vindexes.BuildKeyspace(ksvs.Keyspace, env.Parser())
			if err != nil {
				log.Error(fmt.Sprintf("Invalid keyspace definition: %v", err))
				os.Exit(1)
			}
			ts.SaveVSchema(ctx, ksvs)
			log.Info(fmt.Sprintf("Loaded keyspace %v from %v\n", ks.Name, ksFile))
		}
	}
}

func watchSrvVSchema(ctx context.Context, persister *vschemaPersister, ts *topo.Server, cell string) {
	data, ch, err := ts.WatchSrvVSchema(ctx, cell)
	if err != nil {
		log.Error(fmt.Sprintf("WatchSrvVSchema failed: %v", err))
		os.Exit(1)
	}

	if data.Err != nil {
		log.Error(fmt.Sprintf("WatchSrvVSchema could not retrieve initial vschema: %v", data.Err))
		os.Exit(1)
	}
	persister.persistNewSrvVSchema(data.Value)

	for update := range ch {
		if update.Err != nil {
			log.Error(fmt.Sprintf("WatchSrvVSchema returned an error: %v", update.Err))
		} else {
			persister.persistNewSrvVSchema(update.Value)
		}
	}
}

// flush writes the vschema currently in the topo to disk and seals the
// persister, so that a watcher update the goroutine had not consumed yet cannot
// replace it with an older vschema afterwards.
//
// Reading the topo state is what seals, not writing it out: keyspaces are
// written a file at a time, and an update the watcher is still holding is older
// than this snapshot whether or not those writes succeed. Letting one through
// could only replace a keyspace this flush did write with older content, while a
// keyspace this flush could not write keeps whatever is already on disk.
//
// A failed topo read seals nothing, since without an authoritative snapshot
// there is nothing to order the watcher's updates against.
func (p *vschemaPersister) flush(ctx context.Context, ts *topo.Server, cell string) {
	srvVSchema, err := ts.GetSrvVSchema(ctx, cell)
	if err != nil {
		log.Error("Unable to read SrvVSchema to persist it on shutdown",
			slog.String("cell", cell),
			slog.Any("error", err),
		)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sealed {
		return
	}
	p.sealed = true
	p.persistNewSrvVSchemaLocked(srvVSchema)
}

func (p *vschemaPersister) persistNewSrvVSchema(srvVSchema *vschemapb.SrvVSchema) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sealed {
		return
	}
	p.persistNewSrvVSchemaLocked(srvVSchema)
}

func (p *vschemaPersister) persistNewSrvVSchemaLocked(srvVSchema *vschemapb.SrvVSchema) {
	for ksName, ks := range srvVSchema.Keyspaces {
		if err := persistKeyspace(p.dir, ksName, ks); err != nil {
			log.Error(fmt.Sprintf("Error persisting keyspace %v: %v", ksName, err))
			continue
		}
		log.Info(fmt.Sprintf("Persisted keyspace %v to %v", ksName, p.dir))
	}
}

// persistKeyspace writes a keyspace's vschema to <dir>/<ksName>.json atomically.
// Why: the previous implementation used os.WriteFile, which truncates the
// destination before writing. A process kill between the truncate and the write
// leaves an empty file on disk, and the next vtcombo startup then fails to
// parse the file with "unexpected end of JSON input". Writing to a sibling
// temp file and renaming over the destination keeps the existing file intact
// until the new contents are fully on disk.
func persistKeyspace(dir, ksName string, ks *vschemapb.Keyspace) error {
	jsonBytes, err := json.MarshalIndent(ks, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling keyspace: %w", err)
	}

	finalPath := path.Join(dir, ksName+".json")

	tmp, err := createTempFile(dir, ksName)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if we don't reach the rename. Harmless after a
	// successful rename (the temp name no longer exists).
	defer os.Remove(tmpName)

	// Renaming over the destination replaces its inode, and with it its
	// permissions, so carry over the mode of the file being replaced. Operators
	// who tightened a persisted file keep their mode, matching os.WriteFile,
	// which leaves the mode of an existing file alone. New files instead keep
	// what createTempFile got from 0o644 and the umask, which is what
	// os.WriteFile produced when it created the file.
	if info, err := os.Stat(finalPath); err == nil {
		if err := tmp.Chmod(info.Mode().Perm()); err != nil {
			tmp.Close()
			return fmt.Errorf("chmod temp file %s: %w", tmpName, err)
		}
	}
	if _, err := tmp.Write(jsonBytes); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temp file %s: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("syncing temp file %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", tmpName, finalPath, err)
	}
	return nil
}

// createTempFile creates a uniquely named file in dir. It exists because
// os.CreateTemp hardcodes 0o600: requesting 0o644 here lets the umask apply to
// the new file the same way it applied to the os.WriteFile call this replaced,
// instead of forcing a mode an operator's umask meant to exclude.
func createTempFile(dir, ksName string) (*os.File, error) {
	for range 10 {
		name := path.Join(dir, fmt.Sprintf("%s.%d.tmp", ksName, rand.Uint32()))
		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return f, err
	}
	return nil, fmt.Errorf("no unused temp file name available in %s", dir)
}

func createDirectoryIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.Mkdir(dir, 0o755)
	}
	return nil
}
