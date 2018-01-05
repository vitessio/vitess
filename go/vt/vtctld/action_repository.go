/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctld

import (
	"flag"
	"net/http"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	actionTimeout = flag.Duration("action_timeout", wrangler.DefaultActionTimeout, "time to wait for an action before resorting to force")
)

// ActionResult contains the result of an action. If Error, the action failed.
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

type actionTabletMethod func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (output string, err error)

type actionTabletRecord struct {
	role   string
	method actionTabletMethod
}

// ActionRepository is a repository of actions that can be performed
// on a {Keyspace,Shard,Tablet}.
// Note that the registered action methods will be passed an *http.Request
// on which ParseForm() has already succeeded.
type ActionRepository struct {
	keyspaceActions map[string]actionKeyspaceMethod
	shardActions    map[string]actionShardMethod
	tabletActions   map[string]actionTabletRecord
	ts              *topo.Server
}

// NewActionRepository creates and returns a new ActionRepository,
// with no actions.
func NewActionRepository(ts *topo.Server) *ActionRepository {
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
func (ar *ActionRepository) ApplyKeyspaceAction(ctx context.Context, actionName, keyspace string, r *http.Request) *ActionResult {
	result := &ActionResult{Name: actionName, Parameters: keyspace}

	action, ok := ar.keyspaceActions[actionName]
	if !ok {
		result.error("Unknown keyspace action")
		return result
	}

	ctx, cancel := context.WithTimeout(ctx, *actionTimeout)
	wr := wrangler.New(logutil.NewConsoleLogger(), ar.ts, tmclient.NewTabletManagerClient())
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
func (ar *ActionRepository) ApplyShardAction(ctx context.Context, actionName, keyspace, shard string, r *http.Request) *ActionResult {
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

	ctx, cancel := context.WithTimeout(ctx, *actionTimeout)
	wr := wrangler.New(logutil.NewConsoleLogger(), ar.ts, tmclient.NewTabletManagerClient())
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
func (ar *ActionRepository) ApplyTabletAction(ctx context.Context, actionName string, tabletAlias *topodatapb.TabletAlias, r *http.Request) *ActionResult {
	result := &ActionResult{
		Name:       actionName,
		Parameters: topoproto.TabletAliasString(tabletAlias),
	}

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
	ctx, cancel := context.WithTimeout(ctx, *actionTimeout)
	wr := wrangler.New(logutil.NewConsoleLogger(), ar.ts, tmclient.NewTabletManagerClient())
	output, err := action.method(ctx, wr, tabletAlias, r)
	cancel()
	if err != nil {
		result.error(err.Error())
		return result
	}
	result.Output = output
	return result
}
