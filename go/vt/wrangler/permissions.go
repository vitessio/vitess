// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sort"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/topo"
)

func (wr *Wrangler) GetPermissions(tabletAlias topo.TabletAlias) (*mysqlctl.Permissions, error) {
	return wr.ai.RpcGetPermissions(tabletAlias, wr.actionTimeout())
}

// helper method to asynchronously diff a permissions
func (wr *Wrangler) diffPermissions(masterPermissions *mysqlctl.Permissions, masterAlias topo.TabletAlias, alias topo.TabletAlias, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	relog.Info("Gathering permissions for %v", alias)
	slavePermissions, err := wr.GetPermissions(alias)
	if err != nil {
		er.RecordError(err)
		return
	}

	relog.Info("Diffing permissions for %v", alias)
	mysqlctl.DiffPermissions(masterAlias.String(), masterPermissions, alias.String(), slavePermissions, er)
}

func (wr *Wrangler) ValidatePermissionsShard(keyspace, shard string) error {
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// get permissions from the master, or error
	if si.MasterAlias.Uid == topo.NO_TABLET {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shard)
	}
	relog.Info("Gathering permissions for master %v", si.MasterAlias)
	masterPermissions, err := wr.GetPermissions(si.MasterAlias)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shard)
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
		go wr.diffPermissions(masterPermissions, si.MasterAlias, alias, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Permissions diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) ValidatePermissionsKeyspace(keyspace string) error {
	// find all the shards
	shards, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		return err
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("No shards in keyspace %v", keyspace)
	}
	sort.Strings(shards)
	if len(shards) == 1 {
		return wr.ValidatePermissionsShard(keyspace, shards[0])
	}

	// find the reference permissions using the first shard's master
	si, err := wr.ts.GetShard(keyspace, shards[0])
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == topo.NO_TABLET {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shards[0])
	}
	referenceAlias := si.MasterAlias
	relog.Info("Gathering permissions for reference master %v", referenceAlias)
	referencePermissions, err := wr.GetPermissions(si.MasterAlias)
	if err != nil {
		return err
	}

	// then diff with all tablets but master 0
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, shard := range shards {
		aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shard)
		if err != nil {
			er.RecordError(err)
			continue
		}

		for _, alias := range aliases {
			if alias == si.MasterAlias {
				continue
			}

			wg.Add(1)
			go wr.diffPermissions(referencePermissions, referenceAlias, alias, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Permissions diffs:\n%v", er.Error().Error())
	}
	return nil
}
