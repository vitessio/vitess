package main

import (
	"flag"
	"html/template"
	"net/http"
	"net/url"
	"strings"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

var (
	actionTimeout = flag.Duration("action_timeout", wrangler.DefaultActionTimeout, "time to wait for an action before resorting to force")
	lockTimeout   = flag.Duration("lock_timeout", actionnode.DefaultLockTimeout, "lock time for wrangler/topo operations")
)

// ActionResult contains the result of an action. If Error, the aciton failed.
type ActionResult struct {
	Name       string
	Parameters string
	Output     string
	Error      bool
}

func (ar *ActionResult) error(text string) {
	ar.Error = true
	ar.Output = text
}

// action{Keyspace,Shard,Tablet}Method is a function that performs
// some action on a Topology object. It should return a message for
// the user or an empty string in case there's nothing interesting to
// be communicated.
type actionKeyspaceMethod func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (output string, err error)

type actionShardMethod func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (output string, err error)

type actionTabletMethod func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (output string, err error)

type actionTabletRecord struct {
	role   string
	method actionTabletMethod
}

// ActionRepository is a repository of actions that can be performed
// on a {Keyspace,Shard,Tablet}.
type ActionRepository struct {
	keyspaceActions map[string]actionKeyspaceMethod
	shardActions    map[string]actionShardMethod
	tabletActions   map[string]actionTabletRecord
	ts              topo.Server
}

// NewActionRepository creates and returns a new ActionRepository,
// with no actions.
func NewActionRepository(ts topo.Server) *ActionRepository {
	return &ActionRepository{
		keyspaceActions: make(map[string]actionKeyspaceMethod),
		shardActions:    make(map[string]actionShardMethod),
		tabletActions:   make(map[string]actionTabletRecord),
		ts:              ts,
	}
}

// RegisterKeyspaceAction registers a new action on a keyspace.
func (ar *ActionRepository) RegisterKeyspaceAction(name string, method actionKeyspaceMethod) {
	ar.keyspaceActions[name] = method
}

// RegisterShardAction registers a new action on a shard.
func (ar *ActionRepository) RegisterShardAction(name string, method actionShardMethod) {
	ar.shardActions[name] = method
}

// RegisterTabletAction registers a new action on a tablet.
func (ar *ActionRepository) RegisterTabletAction(name, role string, method actionTabletMethod) {
	ar.tabletActions[name] = actionTabletRecord{
		role:   role,
		method: method,
	}
}

// ApplyKeyspaceAction applies the provided action to the keyspace.
func (ar *ActionRepository) ApplyKeyspaceAction(actionName, keyspace string, r *http.Request) *ActionResult {
	result := &ActionResult{Name: actionName, Parameters: keyspace}

	action, ok := ar.keyspaceActions[actionName]
	if !ok {
		result.error("Unknown keyspace action")
		return result
	}

	// FIXME(alainjobart) copy web context info
	ctx, cancel := context.WithTimeout(context.TODO(), *actionTimeout)
	wr := wrangler.New(logutil.NewConsoleLogger(), ar.ts, *actionTimeout, *lockTimeout)
	output, err := action(ctx, wr, keyspace, r)
	cancel()
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}

// ApplyShardAction applies the provided action to the shard.
func (ar *ActionRepository) ApplyShardAction(actionName, keyspace, shard string, r *http.Request) *ActionResult {
	// if the shard name contains a '-', we assume it's the
	// name for a ranged based shard, so we lower case it.
	if strings.Contains(shard, "-") {
		shard = strings.ToLower(shard)
	}
	result := &ActionResult{Name: actionName, Parameters: keyspace + "/" + shard}

	action, ok := ar.shardActions[actionName]
	if !ok {
		result.error("Unknown shard action")
		return result
	}

	// FIXME(alainjobart) copy web context info
	ctx, cancel := context.WithTimeout(context.TODO(), *actionTimeout)
	wr := wrangler.New(logutil.NewConsoleLogger(), ar.ts, *actionTimeout, *lockTimeout)
	output, err := action(ctx, wr, keyspace, shard, r)
	cancel()
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}

// ApplyTabletAction applies the provided action to the tablet.
func (ar *ActionRepository) ApplyTabletAction(actionName string, tabletAlias topo.TabletAlias, r *http.Request) *ActionResult {
	result := &ActionResult{Name: actionName, Parameters: tabletAlias.String()}

	action, ok := ar.tabletActions[actionName]
	if !ok {
		result.error("Unknown tablet action")
		return result
	}

	// check the role
	if action.role != "" {
		if err := acl.CheckAccessHTTP(r, action.role); err != nil {
			result.error("Access denied")
			return result
		}
	}

	// run the action
	// FIXME(alainjobart) copy web context info
	ctx, cancel := context.WithTimeout(context.TODO(), *actionTimeout)
	wr := wrangler.New(logutil.NewConsoleLogger(), ar.ts, *actionTimeout, *lockTimeout)
	output, err := action.method(ctx, wr, tabletAlias, r)
	cancel()
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}

// PopulateKeyspaceActions populates result with actions that can be
// performed on the keyspace.
func (ar ActionRepository) PopulateKeyspaceActions(actions map[string]template.URL, keyspace string) {
	for name := range ar.keyspaceActions {
		values := url.Values{}
		values.Set("action", name)
		values.Set("keyspace", keyspace)
		actions[name] = template.URL("/keyspace_actions?" + values.Encode())
	}
}

// PopulateShardActions populates result with actions that can be
// performed on the shard.
func (ar ActionRepository) PopulateShardActions(actions map[string]template.URL, keyspace, shard string) {
	for name := range ar.shardActions {
		values := url.Values{}
		values.Set("action", name)
		values.Set("keyspace", keyspace)
		values.Set("shard", shard)
		actions[name] = template.URL("/shard_actions?" + values.Encode())
	}
}

// PopulateTabletActions populates result with actions that can be
// performed on the tablet.
func (ar ActionRepository) PopulateTabletActions(actions map[string]template.URL, tabletAlias string, r *http.Request) {
	for name, value := range ar.tabletActions {
		// check we are authorized for the role we need
		if value.role != "" {
			if err := acl.CheckAccessHTTP(r, value.role); err != nil {
				continue
			}
		}

		// and populate the entry
		values := url.Values{}
		values.Set("action", name)
		values.Set("alias", tabletAlias)
		actions[name] = template.URL("/tablet_actions?" + values.Encode())
	}
}
