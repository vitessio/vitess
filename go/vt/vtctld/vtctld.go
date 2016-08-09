// Package vtctld contains all the code to expose a vtctld server
// based on the provided topo.Server.
package vtctld

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	webDir = flag.String("web_dir", "", "directory from which to serve vtctld web interface resources")
	// webDir2 is a temporary additional dir for a new, in-development UI.
	webDir2             = flag.String("web_dir2", "", "directory from which to serve vtctld2 web interface resources")
	enableRealtimeStats = flag.Bool("enable_realtime_stats", false, "Required for the Realtime Stats view. If set, vtctld will maintain a streaming RPC to each tablet (in all cells) to gather the realtime health stats.")
)

const (
	appPrefix = "/app/"
	// appPrefix2 is a temporary additional path for a new, in-development UI.
	appPrefix2 = "/app2/"
)

// InitVtctld initializes all the vtctld functionnality.
func InitVtctld(ts topo.Server) {
	actionRepo := NewActionRepository(ts)

	// keyspace actions
	actionRepo.RegisterKeyspaceAction("ValidateKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateKeyspace(ctx, keyspace, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateSchemaKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateSchemaKeyspace(ctx, keyspace, nil, false)
		})

	actionRepo.RegisterKeyspaceAction("ValidateVersionKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidateVersionKeyspace(ctx, keyspace)
		})

	actionRepo.RegisterKeyspaceAction("ValidatePermissionsKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "", wr.ValidatePermissionsKeyspace(ctx, keyspace)
		})

	actionRepo.RegisterKeyspaceAction("CreateKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			shardingColumnName := r.FormValue("shardingColumnName")
			shardingColumnType := r.FormValue("shardingColumnType")
			kit, err := key.ParseKeyspaceIDType(shardingColumnType)
			if err != nil {
				return "", err
			}
			ki := &topodatapb.Keyspace{
				ShardingColumnName: shardingColumnName,
				ShardingColumnType: kit,
			}
			return "", wr.TopoServer().CreateKeyspace(ctx, keyspace, ki)
		})

	actionRepo.RegisterKeyspaceAction("EditKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			shardingColumnName := r.FormValue("shardingColumnName")
			shardingColumnType := r.FormValue("shardingColumnType")
			force := r.FormValue("force") == "true"

			kit, err := key.ParseKeyspaceIDType(shardingColumnType)
			if err != nil {
				return "", err
			}

			keyspaceIDTypeSet := kit != topodatapb.KeyspaceIdType_UNSET
			columnNameSet := shardingColumnName != ""
			if (keyspaceIDTypeSet && !columnNameSet) || (!keyspaceIDTypeSet && columnNameSet) {
				return "", fmt.Errorf("Both <column name> and <column type> must be set, or both must be unset.")
			}
			return "", wr.SetKeyspaceShardingInfo(ctx, keyspace, shardingColumnName, kit, force)
		})

	actionRepo.RegisterKeyspaceAction("DeleteKeyspace",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			recursive := r.FormValue("recursive") == "true"
			return "", wr.DeleteKeyspace(ctx, keyspace, recursive)
		})

	// shard actions
	actionRepo.RegisterShardAction("ValidateShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidateShard(ctx, keyspace, shard, false)
		})

	actionRepo.RegisterShardAction("ValidateSchemaShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidateSchemaShard(ctx, keyspace, shard, nil, false)
		})

	actionRepo.RegisterShardAction("ValidateVersionShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidateVersionShard(ctx, keyspace, shard)
		})

	actionRepo.RegisterShardAction("ValidatePermissionsShard",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "", wr.ValidatePermissionsShard(ctx, keyspace, shard)
		})

	// tablet actions
	actionRepo.RegisterTabletAction("Ping", "",
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (string, error) {
			ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
			if err != nil {
				return "", err
			}
			return "", wr.TabletManagerClient().Ping(ctx, ti.Tablet)
		})

	actionRepo.RegisterTabletAction("RefreshState", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (string, error) {
			ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
			if err != nil {
				return "", err
			}
			return "", wr.TabletManagerClient().RefreshState(ctx, ti.Tablet)
		})

	actionRepo.RegisterTabletAction("DeleteTablet", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (string, error) {
			return "", wr.DeleteTablet(ctx, tabletAlias, false)
		})

	actionRepo.RegisterTabletAction("ReloadSchema", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (string, error) {
			return "", wr.ReloadSchema(ctx, tabletAlias)
		})

	// Anything unrecognized gets redirected to the main app page.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, appPrefix, http.StatusFound)
	})

	// Serve the static files for the vtctld web app.
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
		http.ServeFile(w, r, path.Join(*webDir, rest))
	})

	// Serve the static files for the vtctld2 web app.
	// This is a temporary additional URL for serving the new,
	// in-development UI side-by-side with the current one.
	http.HandleFunc(appPrefix2, func(w http.ResponseWriter, r *http.Request) {
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
		filePath := path.Join(*webDir2, rest)
		// If the requested file doesn't exist, serve index.html.
		if _, err := os.Stat(filePath); err != nil {
			filePath = path.Join(*webDir2, "index.html")
		}
		http.ServeFile(w, r, filePath)
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
}
