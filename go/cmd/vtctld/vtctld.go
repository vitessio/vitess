package main

import (
	"flag"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/schemamanager"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	webDir                    = flag.String("web_dir", "", "directory from which to serve vtctld web interface resources")
	debug                     = flag.Bool("debug", false, "recompile templates for every request")
	schemaChangeDir           = flag.String("schema_change_dir", "", "directory contains schema changes for all keyspaces. Each keyspace has its own directory and schema changes are expected to live in '$KEYSPACE/input' dir. e.g. test_keyspace/input/*sql, each sql file represents a schema change")
	schemaChangeController    = flag.String("schema_change_controller", "", "schema change controller is responsible for finding schema changes and responsing schema change events")
	schemaChangeCheckInterval = flag.Int("schema_change_check_interval", 60, "this value decides how often we check schema change dir, in seconds")
	schemaChangeUser          = flag.String("schema_change_user", "", "The user who submits this schema change.")

	_ = flag.String("templates", "", "<deprecated>")

	appPrefix = "/app/"
)

func init() {
	servenv.RegisterDefaultFlags()
}

func httpErrorf(w http.ResponseWriter, r *http.Request, format string, args ...interface{}) {
	errMsg := fmt.Sprintf(format, args...)
	log.Errorf("HTTP error on %v: %v, request: %#v", r.URL.Path, errMsg, r)
	http.Error(w, errMsg, http.StatusInternalServerError)
}

// used at runtime by plug-ins
var (
	actionRepo *ActionRepository
	ts         topo.Server
)

func main() {
	flag.Parse()
	servenv.Init()
	defer servenv.Close()

	ts = topo.GetServer()
	defer topo.CloseServers()

	actionRepo = NewActionRepository(ts)

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
			return "", wr.TabletManagerClient().Ping(ctx, ti)
		})

	actionRepo.RegisterTabletAction("RefreshState", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (string, error) {
			ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
			if err != nil {
				return "", err
			}
			return "", wr.TabletManagerClient().RefreshState(ctx, ti)
		})

	actionRepo.RegisterTabletAction("DeleteTablet", acl.ADMIN,
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (string, error) {
			return "", wr.DeleteTablet(ctx, tabletAlias, false, false)
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

	// Serve the REST API for the vtctld web app.
	initAPI(context.Background(), ts, actionRepo)

	// redirects for explorers
	http.HandleFunc("/explorers/redirect", func(w http.ResponseWriter, r *http.Request) {
		if explorer == nil {
			http.Error(w, "no explorer configured", http.StatusInternalServerError)
			return
		}
		if err := r.ParseForm(); err != nil {
			httpErrorf(w, r, "cannot parse form: %s", err)
			return
		}

		target, err := handleExplorerRedirect(context.Background(), ts, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		http.Redirect(w, r, target, http.StatusFound)
	})

	// Start schema manager service.
	if *schemaChangeDir != "" {
		interval := 60
		if *schemaChangeCheckInterval > 0 {
			interval = *schemaChangeCheckInterval
		}
		timer := timer.NewTimer(time.Duration(interval) * time.Second)
		controllerFactory, err :=
			schemamanager.GetControllerFactory(*schemaChangeController)
		if err != nil {
			log.Fatalf("unable to get a controller factory, error: %v", err)
		}

		timer.Start(func() {
			controller, err := controllerFactory(map[string]string{
				schemamanager.SchemaChangeDirName: *schemaChangeDir,
				schemamanager.SchemaChangeUser:    *schemaChangeUser,
			})
			if err != nil {
				log.Errorf("failed to get controller, error: %v", err)
				return
			}
			ctx := context.Background()
			err = schemamanager.Run(
				ctx,
				controller,
				schemamanager.NewTabletExecutor(
					tmclient.NewTabletManagerClient(), ts),
			)
			if err != nil {
				log.Errorf("Schema change failed, error: %v", err)
			}
		})
		servenv.OnClose(func() { timer.Stop() })
	}

	servenv.RunDefault()
}
