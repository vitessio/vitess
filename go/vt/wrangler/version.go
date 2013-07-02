// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

type debugVars struct {
	Version string
}

func (wr *Wrangler) GetVersion(zkTabletPath string) (string, error) {
	// read the tablet from ZK to get the address to connect to
	tablet, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return "", err
	}

	// build the url, get debug/vars
	resp, err := http.Get("http://" + tablet.Addr + "/debug/vars")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// convert json
	vars := debugVars{}
	err = json.Unmarshal(body, &vars)
	if err != nil {
		return "", err
	}

	// split the version into date and md5
	parts := strings.Split(vars.Version, " ")
	if len(parts) != 2 {
		// can't understand this, oh well
		return vars.Version, nil
	}
	version := parts[1]

	relog.Info("Tablet %v is running version '%v'", zkTabletPath, version)
	return version, nil
}

// helper method to asynchronously get and diff a version
func (wr *Wrangler) diffVersion(masterVersion string, zkMasterTabletPath string, alias naming.TabletAlias, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	zkTabletPath := tm.TabletPathForAlias(alias)
	relog.Info("Gathering version for %v", zkTabletPath)
	slaveVersion, err := wr.GetVersion(zkTabletPath)
	if err != nil {
		er.RecordError(err)
		return
	}

	if masterVersion != slaveVersion {
		er.RecordError(fmt.Errorf("Master %v version %v is different than slave %v version %v", zkMasterTabletPath, masterVersion, zkTabletPath, slaveVersion))
	}
}

func (wr *Wrangler) ValidateVersionShard(zkShardPath string) error {
	si, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	// get version from the master, or error
	if si.MasterAlias.Uid == naming.NO_TABLET {
		return fmt.Errorf("No master in shard " + zkShardPath)
	}
	zkMasterTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	relog.Info("Gathering version for master %v", zkMasterTabletPath)
	masterVersion, err := wr.GetVersion(zkMasterTabletPath)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, si.ShardPath())
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
		go wr.diffVersion(masterVersion, zkMasterTabletPath, alias, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Version diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) ValidateVersionKeyspace(zkKeyspacePath string) error {
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
		return wr.ValidateVersionShard(referenceShardPath)
	}

	// find the reference version using the first shard's master
	si, err := tm.ReadShard(wr.zconn, referenceShardPath)
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == naming.NO_TABLET {
		return fmt.Errorf("No master in shard " + referenceShardPath)
	}
	zkReferenceTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	relog.Info("Gathering version for reference master %v", zkReferenceTabletPath)
	referenceVersion, err := wr.GetVersion(zkReferenceTabletPath)
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
			go wr.diffVersion(referenceVersion, zkReferenceTabletPath, alias, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Version diffs:\n%v", er.Error().Error())
	}
	return nil
}
