/*
Copyright 2019 The Vitess Authors.

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

package wrangler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var getVersionFromTabletDebugVars = func(tabletAddr string) (string, error) {
	resp, err := http.Get("http://" + tabletAddr + "/debug/vars")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
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
	resp, err := wr.VtctldServer().GetVersion(ctx, &vtctldatapb.GetVersionRequest{
		TabletAlias: tabletAlias,
	})
	log.Infof("Tablet %v is running version '%v'", topoproto.TabletAliasString(tabletAlias), resp.Version)
	return resp.Version, err
}

// ValidateVersionShard validates all versions are the same in all
// tablets in a shard
func (wr *Wrangler) ValidateVersionShard(ctx context.Context, keyspace, shard string) error {
	res, err := wr.VtctldServer().ValidateVersionShard(ctx, &vtctldatapb.ValidateVersionShardRequest{
		Keyspace: keyspace,
		Shard:    shard,
	})

	if len(res.Results) > 0 {
		return fmt.Errorf("version diffs: %v", res.Results)
	}
	return err
}

// ValidateVersionKeyspace validates all versions are the same in all
// tablets in a keyspace
func (wr *Wrangler) ValidateVersionKeyspace(ctx context.Context, keyspace string) error {
	res, err := wr.VtctldServer().ValidateVersionKeyspace(ctx, &vtctldatapb.ValidateVersionKeyspaceRequest{
		Keyspace: keyspace,
	})

	if len(res.Results) > 0 {
		return fmt.Errorf("version diffs: %v", res.Results)
	}
	return err
}
