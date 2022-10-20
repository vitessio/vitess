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
	"context"
	"net/http"
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/wrangler"
	"vitess.io/vitess/web/vtctld2"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	enableRealtimeStats = false
	enableUI            = true
	durabilityPolicy    = "none"
	sanitizeLogMessages = false
	webDir              string
	webDir2             string
)

const (
	appPrefix = "/app/"
)

func init() {
	for _, cmd := range []string{"vtcombo", "vtctld"} {
		servenv.OnParseFor(cmd, registerVtctldFlags)
	}
}

func registerVtctldFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&enableRealtimeStats, "enable_realtime_stats", enableRealtimeStats, "Required for the Realtime Stats view. If set, vtctld will maintain a streaming RPC to each tablet (in all cells) to gather the realtime health stats.")
	fs.MarkDeprecated("enable_realtime_stats", "It is used by old vtctl UI that is already deprecated.")
	fs.BoolVar(&enableUI, "enable_vtctld_ui", enableUI, "If true, the vtctld web interface will be enabled. Default is true.")
	fs.MarkDeprecated("enable_vtctld_ui", "It is used by old vtctl UI that is already deprecated.")
	fs.StringVar(&durabilityPolicy, "durability_policy", durabilityPolicy, "type of durability to enforce. Default is none. Other values are dictated by registered plugins")
	fs.BoolVar(&sanitizeLogMessages, "vtctld_sanitize_log_messages", sanitizeLogMessages, "When true, vtctld sanitizes logging.")
	fs.StringVar(&webDir, "web_dir", webDir, "NOT USED, here for backward compatibility")
	fs.MarkDeprecated("web_dir", "it will be removed in a future releases.")
	fs.StringVar(&webDir2, "web_dir2", webDir2, "NOT USED, here for backward compatibility")
	fs.MarkDeprecated("web_dir2", "it will be removed in a future releases.")
}

// InitVtctld initializes all the vtctld functionality.
func InitVtctld(ts *topo.Server) error {
	actionRepo := NewActionRepository(ts)

	// keyspace actions
	actionRepo.RegisterKeyspaceAction("ValidateKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string) (string, error) {
			return "", wr.ValidateKeyspace(ctx, keyspace, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateSchemaKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string) (string, error) {
			return "", wr.ValidateSchemaKeyspace(ctx, keyspace, nil /*excludeTables*/, false /*includeViews*/, false /*skipNoPrimary*/, false /*includeVSchema*/)
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
			return "", wr.ValidateSchemaShard(ctx, keyspace, shard, nil, false, false /*includeVSchema*/)
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
			_, err := wr.VtctldServer().ReloadSchema(ctx, &vtctldatapb.ReloadSchemaRequest{
				TabletAlias: tabletAlias,
			})
			return "", err
		})

	// Anything unrecognized gets redirected to the main app page.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, appPrefix, http.StatusFound)
	})

	http.Handle(appPrefix, staticContentHandler(enableUI))

	var healthCheck discovery.HealthCheck
	if enableRealtimeStats {
		ctx := context.Background()
		cells, err := ts.GetKnownCells(ctx)
		if err != nil {
			log.Errorf("Failed to get the list of known cells, failed to instantiate the healthcheck at startup: %v", err)
		} else {
			healthCheck = discovery.NewHealthCheck(ctx, *vtctl.HealthcheckRetryDelay, *vtctl.HealthCheckTimeout, ts, localCell, strings.Join(cells, ","))
		}
	}

	// Serve the REST API for the vtctld web app.
	initAPI(context.Background(), ts, actionRepo, healthCheck)

	// Init redirects for explorers
	initExplorer(ts)

	// Init workflow manager.
	initWorkflowManager(ts)

	// Setup reverse proxy for all vttablets through /vttablet/.
	initVTTabletRedirection(ts)

	return nil
}

func staticContentHandler(enabled bool) http.Handler {
	if enabled {
		return http.FileServer(http.FS(vtctld2.Content))
	}

	fn := func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}
	return http.HandlerFunc(fn)
}
