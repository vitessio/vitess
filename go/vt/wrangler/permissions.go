// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"path"
	"sort"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

func (wr *Wrangler) GetPermissions(zkTabletPath string) (*mysqlctl.Permissions, error) {
	return wr.ai.RpcGetPermissions(zkTabletPath, wr.actionTimeout())
}

// helper method to asynchronously diff a permissions
func (wr *Wrangler) diffPermissions(masterPermissions *mysqlctl.Permissions, zkMasterTabletPath string, alias tm.TabletAlias, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	zkTabletPath := tm.TabletPathForAlias(alias)
	relog.Info("Gathering permissions for %v", zkTabletPath)
	slavePermissions, err := wr.GetPermissions(zkTabletPath)
	if err != nil {
		er.RecordError(err)
		return
	}

	relog.Info("Diffing permissions for %v", zkTabletPath)
	mysqlctl.DiffPermissions(zkMasterTabletPath, masterPermissions, zkTabletPath, slavePermissions, er)
}

func (wr *Wrangler) ValidatePermissionsShard(zkShardPath string) error {
	si, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	// get permissions from the master, or error
	if si.MasterAlias.Uid == tm.NO_TABLET {
		return fmt.Errorf("No master in shard " + zkShardPath)
	}
	zkMasterTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	relog.Info("Gathering permissions for master %v", zkMasterTabletPath)
	masterPermissions, err := wr.GetPermissions(zkMasterTabletPath)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, si.ShardPath())
	if err != nil {
		return err
	}

	// then diff all of them, except master
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if alias == si.MasterAlias {
			continue
		}
		wg.Add(1)
		go wr.diffPermissions(masterPermissions, zkMasterTabletPath, alias, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Permissions diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) ValidatePermissionsKeyspace(zkKeyspacePath string) error {
	// find all the shards
	zkShardsPath := path.Join(zkKeyspacePath, "shards")
	shards, _, err := wr.zconn.Children(zkShardsPath)
	if err != nil {
		return err
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("No shards in keyspace " + zkKeyspacePath)
	}
	sort.Strings(shards)
	referenceShardPath := path.Join(zkShardsPath, shards[0])
	if len(shards) == 1 {
		return wr.ValidatePermissionsShard(referenceShardPath)
	}

	// find the reference permissions using the first shard's master
	si, err := tm.ReadShard(wr.zconn, referenceShardPath)
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == tm.NO_TABLET {
		return fmt.Errorf("No master in shard " + referenceShardPath)
	}
	zkReferenceTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	relog.Info("Gathering permissions for reference master %v", zkReferenceTabletPath)
	referencePermissions, err := wr.GetPermissions(zkReferenceTabletPath)
	if err != nil {
		return err
	}

	// then diff with all tablets but master 0
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, shard := range shards {
		shardPath := path.Join(zkShardsPath, shard)
		aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, shardPath)
		if err != nil {
			er.RecordError(err)
			continue
		}

		for _, alias := range aliases {
			if alias == si.MasterAlias {
				continue
			}

			wg.Add(1)
			go wr.diffPermissions(referencePermissions, zkReferenceTabletPath, alias, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Permissions diffs:\n%v", er.Error().Error())
	}
	return nil
}
