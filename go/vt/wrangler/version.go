// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var getVersionFromTabletDebugVars = func(tabletAddr string) (string, error) {
	resp, err := http.Get("http://" + tabletAddr + "/debug/vars")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var vars struct {
		BuildHost      string
		BuildUser      string
		BuildTimestamp int64
		BuildGitRev    string
	}
	err = json.Unmarshal(body, &vars)
	if err != nil {
		return "", err
	}

	version := fmt.Sprintf("%v", vars)
	return version, nil
}

var getVersionFromTablet = getVersionFromTabletDebugVars

// ResetDebugVarsGetVersion is used by tests to reset the
// getVersionFromTablet variable to the default one. That way we can
// run the unit tests in testlib/ even when another implementation of
// getVersionFromTablet is used.
func ResetDebugVarsGetVersion() {
	getVersionFromTablet = getVersionFromTabletDebugVars
}

// GetVersion returns the version string from a tablet
func (wr *Wrangler) GetVersion(ctx context.Context, tabletAlias *topodatapb.TabletAlias) (string, error) {
	tablet, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return "", err
	}

	version, err := getVersionFromTablet(tablet.Addr())
	if err != nil {
		return "", err
	}
	log.Infof("Tablet %v is running version '%v'", topoproto.TabletAliasString(tabletAlias), version)
	return version, err
}

// helper method to asynchronously get and diff a version
func (wr *Wrangler) diffVersion(ctx context.Context, masterVersion string, masterAlias *topodatapb.TabletAlias, alias *topodatapb.TabletAlias, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering version for %v", topoproto.TabletAliasString(alias))
	slaveVersion, err := wr.GetVersion(ctx, alias)
	if err != nil {
		er.RecordError(err)
		return
	}

	if masterVersion != slaveVersion {
		er.RecordError(fmt.Errorf("Master %v version %v is different than slave %v version %v", topoproto.TabletAliasString(masterAlias), masterVersion, topoproto.TabletAliasString(alias), slaveVersion))
	}
}

// ValidateVersionShard validates all versions are the same in all
// tablets in a shard
func (wr *Wrangler) ValidateVersionShard(ctx context.Context, keyspace, shard string) error {
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// get version from the master, or error
	if !si.HasMaster() {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shard)
	}
	log.Infof("Gathering version for master %v", topoproto.TabletAliasString(si.MasterAlias))
	masterVersion, err := wr.GetVersion(ctx, si.MasterAlias)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// then diff with all slaves
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if topoproto.TabletAliasEqual(alias, si.MasterAlias) {
			continue
		}

		wg.Add(1)
		go wr.diffVersion(ctx, masterVersion, si.MasterAlias, alias, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Version diffs: %v", er.Error().Error())
	}
	return nil
}

// ValidateVersionKeyspace validates all versions are the same in all
// tablets in a keyspace
func (wr *Wrangler) ValidateVersionKeyspace(ctx context.Context, keyspace string) error {
	// find all the shards
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("No shards in keyspace %v", keyspace)
	}
	sort.Strings(shards)
	if len(shards) == 1 {
		return wr.ValidateVersionShard(ctx, keyspace, shards[0])
	}

	// find the reference version using the first shard's master
	si, err := wr.ts.GetShard(ctx, keyspace, shards[0])
	if err != nil {
		return err
	}
	if !si.HasMaster() {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shards[0])
	}
	referenceAlias := si.MasterAlias
	log.Infof("Gathering version for reference master %v", topoproto.TabletAliasString(referenceAlias))
	referenceVersion, err := wr.GetVersion(ctx, referenceAlias)
	if err != nil {
		return err
	}

	// then diff with all tablets but master 0
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, shard := range shards {
		aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
		if err != nil {
			er.RecordError(err)
			continue
		}

		for _, alias := range aliases {
			if topoproto.TabletAliasEqual(alias, si.MasterAlias) {
				continue
			}

			wg.Add(1)
			go wr.diffVersion(ctx, referenceVersion, referenceAlias, alias, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Version diffs: %v", er.Error().Error())
	}
	return nil
}
