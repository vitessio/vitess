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

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

var getVersionFromTablet = func(tabletAddr string) (string, error) {
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

func (wr *Wrangler) GetVersion(tabletAlias topo.TabletAlias) (string, error) {
	tablet, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return "", err
	}

	version, err := getVersionFromTablet(tablet.Addr())
	if err != nil {
		return "", err
	}
	log.Infof("Tablet %v is running version '%v'", tabletAlias, version)
	return version, err
}

// helper method to asynchronously get and diff a version
func (wr *Wrangler) diffVersion(masterVersion string, masterAlias topo.TabletAlias, alias topo.TabletAlias, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering version for %v", alias)
	slaveVersion, err := wr.GetVersion(alias)
	if err != nil {
		er.RecordError(err)
		return
	}

	if masterVersion != slaveVersion {
		er.RecordError(fmt.Errorf("Master %v version %v is different than slave %v version %v", masterAlias, masterVersion, alias, slaveVersion))
	}
}

func (wr *Wrangler) ValidateVersionShard(keyspace, shard string) error {
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// get version from the master, or error
	if si.MasterAlias.Uid == topo.NO_TABLET {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shard)
	}
	log.Infof("Gathering version for master %v", si.MasterAlias)
	masterVersion, err := wr.GetVersion(si.MasterAlias)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := topo.FindAllTabletAliasesInShard(context.TODO(), wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	// then diff with all slaves
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if alias == si.MasterAlias {
			continue
		}

		wg.Add(1)
		go wr.diffVersion(masterVersion, si.MasterAlias, alias, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Version diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) ValidateVersionKeyspace(keyspace string) error {
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
		return wr.ValidateVersionShard(keyspace, shards[0])
	}

	// find the reference version using the first shard's master
	si, err := wr.ts.GetShard(keyspace, shards[0])
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == topo.NO_TABLET {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shards[0])
	}
	referenceAlias := si.MasterAlias
	log.Infof("Gathering version for reference master %v", referenceAlias)
	referenceVersion, err := wr.GetVersion(referenceAlias)
	if err != nil {
		return err
	}

	// then diff with all tablets but master 0
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, shard := range shards {
		aliases, err := topo.FindAllTabletAliasesInShard(context.TODO(), wr.ts, keyspace, shard)
		if err != nil {
			er.RecordError(err)
			continue
		}

		for _, alias := range aliases {
			if alias == si.MasterAlias {
				continue
			}

			wg.Add(1)
			go wr.diffVersion(referenceVersion, referenceAlias, alias, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Version diffs:\n%v", er.Error().Error())
	}
	return nil
}
