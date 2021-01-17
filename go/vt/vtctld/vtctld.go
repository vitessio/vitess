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

// Package vtctld contains all the code to expose a vtctld server
// based on the provided topo.Server.
package vtctld

import (
	"flag"
	"net/http"
	"strings"
	"time"

	rice "github.com/GeertJohan/go.rice"

	"context"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	enableRealtimeStats = flag.Bool("enable_realtime_stats", false, "Required for the Realtime Stats view. If set, vtctld will maintain a streaming RPC to each tablet (in all cells) to gather the realtime health stats.")

	_ = flag.String("web_dir", "", "NOT USED, here for backward compatibility")
	_ = flag.String("web_dir2", "", "NOT USED, here for backward compatibility")
)

const (
	appPrefix = "/app/"
)

// InitVtctld initializes all the vtctld functionality.
func InitVtctld(ts *topo.Server) {
	actionRepo := NewActionRepository(ts)

	// keyspace actions
	actionRepo.RegisterKeyspaceAction("ValidateKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string) (string, error) {
			return "", wr.ValidateKeyspace(ctx, keyspace, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateSchemaKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string) (string, error) {
			return "", wr.ValidateSchemaKeyspace(ctx, keyspace, nil, false, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateVersionKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string) (string, error) {
			return "", wr.ValidateVersionKeyspace(ctx, keyspace)
		})

	actionRepo.RegisterKeyspaceAction("ValidatePermissionsKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string) (string, error) {
			return "", wr.ValidatePermissionsKeyspace(ctx, keyspace)
		})

	// shard actions
	actionRepo.RegisterShardAction("ValidateShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string) (string, error) {
			return "", wr.ValidateShard(ctx, keyspace, shard, false)
		})

	actionRepo.RegisterShardAction("ValidateSchemaShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string) (string, error) {
			return "", wr.ValidateSchemaShard(ctx, keyspace, shard, nil, false)
		})

	actionRepo.RegisterShardAction("ValidateVersionShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string) (string, error) {
			return "", wr.ValidateVersionShard(ctx, keyspace, shard)
		})

	actionRepo.RegisterShardAction("ValidatePermissionsShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string) (string, error) {
			return "", wr.ValidatePermissionsShard(ctx, keyspace, shard)
		})

	// tablet actions
	actionRepo.RegisterTabletAction("Ping", "",
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias) (string, error) {
			ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
			if err != nil {
				return "", err
			}
			return "", wr.TabletManagerClient().Ping(ctx, ti.Tablet)
		})

	actionRepo.RegisterTabletAction("RefreshState", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias) (string, error) {
			ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
			if err != nil {
				return "", err
			}
			return "", wr.TabletManagerClient().RefreshState(ctx, ti.Tablet)
		})

	actionRepo.RegisterTabletAction("DeleteTablet", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias) (string, error) {
			return "", wr.DeleteTablet(ctx, tabletAlias, false)
		})

	actionRepo.RegisterTabletAction("ReloadSchema", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias) (string, error) {
			return "", wr.ReloadSchema(ctx, tabletAlias)
		})

	// Anything unrecognized gets redirected to the main app page.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, appPrefix, http.StatusFound)
	})

	// Serve the static files for the vtctld2 web app
	http.HandleFunc(appPrefix, func(w http.ResponseWriter, r *http.Request) {
		// Strip the prefix.
		parts := strings.SplitN(r.URL.Path, "/", 3)
		if len(parts) != 3 {
			http.NotFound(w, r)
			return
		}
		rest := parts[2]
		if rest == "" {
			rest = "index.html"
		}

		riceBox, err := rice.FindBox("../../../web/vtctld2/app")
		if err != nil {
			log.Errorf("Unable to open rice box %s", err)
			http.NotFound(w, r)
		}
		fileToServe, err := riceBox.Open(rest)
		if err != nil {
			if !strings.ContainsAny(rest, "/.") {
				//This is a virtual route so pass index.html
				fileToServe, err = riceBox.Open("index.html")
			}
			if err != nil {
				log.Errorf("Unable to open file from rice box %s : %s", rest, err)
				http.NotFound(w, r)
			}
		}
		if fileToServe != nil {
			http.ServeContent(w, r, rest, time.Now(), fileToServe)
		}
	})

	var realtimeStats *realtimeStats
	if *enableRealtimeStats {
		var err error
		realtimeStats, err = newRealtimeStats(ts)
		if err != nil {
			log.Errorf("Failed to instantiate RealtimeStats at startup: %v", err)
		}
	}

	// Serve the REST API for the vtctld web app.
	initAPI(context.Background(), ts, actionRepo, realtimeStats)

	// Init redirects for explorers
	initExplorer(ts)

	// Init workflow manager.
	initWorkflowManager(ts)

	// Init online DDL schema manager
	initSchemaManager(ts)

	// Setup reverse proxy for all vttablets through /vttablet/.
	initVTTabletRedirection(ts)
}
