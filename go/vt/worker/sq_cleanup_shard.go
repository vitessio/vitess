/*
Copyright 2017 Google Inc.

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

package worker

import (
	"fmt"
	"html/template"
	"strings"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"
)

// CleanUpShardWorker drops tables in a shard. Useful when you are reusing an existing database.
type CleanUpShardWorker struct {
	StatusWorker

	wr            *wrangler.Wrangler
	cell          string
	keyspace      string
	shard         string
	excludeTables []string
	secondPhase   bool
	cleaner       *wrangler.Cleaner

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo *topo.KeyspaceInfo
	shardInfo    *topo.ShardInfo

	// populated during WorkerStateFindTargets, read-only after that
	tablet *topo.TabletInfo
}

// NewCleanUpShardWorker returns a new CleanUpShardWorker object.
func NewCleanUpShardWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, secondPhase bool, excludeTables []string) Worker {
	return &CleanUpShardWorker{
		StatusWorker:  NewStatusWorker(),
		wr:            wr,
		cell:          cell,
		keyspace:      keyspace,
		shard:         shard,
		secondPhase:   secondPhase,
		excludeTables: excludeTables,
		cleaner:       &wrangler.Cleaner{},
	}
}

// StatusAsHTML is part of the Worker interface
func (dtw *CleanUpShardWorker) StatusAsHTML() template.HTML {
	state := dtw.State()

	result := "<b>Working on:</b> " + dtw.keyspace + "/" + dtw.shard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateWorking:
		result += "<b>Running...</b></br>\n"
	case WorkerStateDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (dtw *CleanUpShardWorker) StatusAsText() string {
	state := dtw.State()

	result := "Working on: " + dtw.keyspace + "/" + dtw.shard + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateWorking:
		result += "Running...\n"
	case WorkerStateDone:
		result += "Success.\n"
	}
	return result
}

// Run is mostly a wrapper to run the cleanup at the end.
func (dtw *CleanUpShardWorker) Run(ctx context.Context) error {
	resetVars()
	err := dtw.run(ctx)

	dtw.SetState(WorkerStateCleanUp)
	cerr := dtw.cleaner.CleanUp(dtw.wr)
	if cerr != nil {
		if err != nil {
			dtw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		dtw.wr.Logger().Errorf("Run() error: %v", err)
		dtw.SetState(WorkerStateError)
		return err
	}
	dtw.SetState(WorkerStateDone)
	return nil
}

func (dtw *CleanUpShardWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := dtw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find target
	if err := dtw.findTarget(ctx); err != nil {
		return fmt.Errorf("findTarget() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// fourth phase: cleanUpShard tables
	if err := dtw.cleanUpShard(ctx); err != nil {
		return fmt.Errorf("cleanUpShard() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	return nil
}

func (dtw *CleanUpShardWorker) init(ctx context.Context) error {
	dtw.SetState(WorkerStateInit)

	var err error
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	dtw.keyspaceInfo, err = dtw.wr.TopoServer().GetKeyspace(shortCtx, dtw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", dtw.keyspace, err)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	dtw.shardInfo, err = dtw.wr.TopoServer().GetShard(shortCtx, dtw.keyspace, dtw.shard)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read shard %v/%v: %v", dtw.keyspace, dtw.shard, err)
	}

	if !dtw.shardInfo.HasMaster() {
		return fmt.Errorf("shard %v/%v has no master", dtw.keyspace, dtw.shard)
	}

	return nil
}

func (dtw *CleanUpShardWorker) findTarget(ctx context.Context) error {
	dtw.SetState(WorkerStateFindTargets)

	var err error

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	masterInfo, err := dtw.wr.TopoServer().GetTablet(shortCtx, dtw.shardInfo.MasterAlias)
	cancel()
	if err != nil {
		return fmt.Errorf("findTarget: cannot get Tablet record for master %v: %v", dtw.shardInfo.MasterAlias, err)
	}
	dtw.tablet = masterInfo
	return nil
}

func (dtw *CleanUpShardWorker) renameTableToDeleted(ctx context.Context, table string) error {
	if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_deleted") {
		return nil
	}

	newName := fmt.Sprintf("_%v_deleted", table)
	dtw.wr.Logger().Infof("Renaming table %v -> %v", table, newName)

	tmc := tmclient.NewTabletManagerClient()
	defer tmc.Close()

	_, err := tmc.ExecuteFetchAsDba(ctx, dtw.tablet.Tablet, true, []byte(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", table, newName)), 0, false, true)
	if err != nil {
		return vterrors.Wrapf(err, "failed to drop table: %v", table)
	}

	dtw.wr.Logger().Infof("Renamed %v", table)

	return nil
}

func (dtw *CleanUpShardWorker) dropDeletedTable(ctx context.Context, table string) error {
	if !strings.HasPrefix(table, "_") {
		return nil
	}
	if !strings.HasSuffix(table, "_deleted") {
		return nil
	}
	dtw.wr.Logger().Infof("Dropping table %v", table)

	tmc := tmclient.NewTabletManagerClient()
	defer tmc.Close()

	_, err := tmc.ExecuteFetchAsDba(ctx, dtw.tablet.Tablet, true, []byte(fmt.Sprintf("DROP TABLE %s", table)), 0, false, true)
	if err != nil {
		return vterrors.Wrapf(err, "failed to drop table: %v", table)
	}

	dtw.wr.Logger().Infof("Table %v dropped", table)

	return nil
}

func (dtw *CleanUpShardWorker) truncateTable(ctx context.Context, table string) error {
	dtw.wr.Logger().Infof("Truncate table %v", table)

	tmc := tmclient.NewTabletManagerClient()
	defer tmc.Close()

	_, err := tmc.ExecuteFetchAsDba(ctx, dtw.tablet.Tablet, true, []byte(fmt.Sprintf("TRUNCATE TABLE %s", table)), 0, false, true)
	if err != nil {
		return vterrors.Wrapf(err, "failed to truncate table: %v", table)
	}

	dtw.wr.Logger().Infof("Table %v truncated", table)

	return nil
}

func (dtw *CleanUpShardWorker) cleanUpShard(ctx context.Context) error {
	dtw.SetState(WorkerStateWorking)

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	schemaDefinition, err := dtw.wr.GetSchema(
		shortCtx, dtw.tablet.Alias, nil /* tables */, dtw.excludeTables, false /* includeViews */)
	cancel()
	if err != nil {
		return err
	}

	tableDefinitions := schemaDefinition.TableDefinitions
	if !dtw.secondPhase {
		dtw.wr.Logger().Infof("Phase one - renaming tables...")
		for _, tableDefinition := range tableDefinitions {
			err := dtw.renameTableToDeleted(ctx, tableDefinition.Name)
			if err != nil {
				return err
			}
		}
		err = dtw.truncateTable(ctx, "_vt.blp_checkpoint")
		if err != nil {
			dtw.wr.Logger().Warningf("could not truncate _vt.blp_checkpoint (it probably doesn't exist): %v", err)
		}
		err = dtw.truncateTable(ctx, "_vt.vreplication")
		if err != nil {
			dtw.wr.Logger().Warningf("could not truncate _vt.vreplication (it probably doesn't exist): %v", err)
		}
		dtw.wr.Logger().Infof("Phase one done, run second phase to actually delete the tables")
	} else {
		dtw.wr.Logger().Infof("Phase two - dropping renamed tables...")
		for _, tableDefinition := range tableDefinitions {
			err := dtw.dropDeletedTable(ctx, tableDefinition.Name)
			if err != nil {
				return err
			}
		}
		dtw.wr.Logger().Infof("Phase two done")
	}

	return nil
}
