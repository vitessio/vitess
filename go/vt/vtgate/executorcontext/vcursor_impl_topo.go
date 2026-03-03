/*
Copyright 2026 The Vitess Authors.

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

package executorcontext

import (
	"context"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
)

func (vc *VCursorImpl) ExecuteVSchema(ctx context.Context, keyspace string, vschemaDDL *sqlparser.AlterVschema) error {
	srvVschema := vc.vm.GetCurrentSrvVschema()
	if srvVschema == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
	}

	user := callerid.ImmediateCallerIDFromContext(ctx)
	allowed := vschemaacl.Authorized(user)
	if !allowed {
		return vterrors.NewErrorf(vtrpcpb.Code_PERMISSION_DENIED, vterrors.AccessDeniedError, "User '%s' is not authorized to perform vschema operations", user.GetUsername())
	}

	// Resolve the keyspace either from the table qualifier or the target keyspace
	var (
		ksName string
		err    error
	)
	if !vschemaDDL.Table.IsEmpty() {
		ksName = vschemaDDL.Table.Qualifier.String()
	}
	if ksName == "" {
		ksName = keyspace
	}
	if ksName == "" {
		return ErrNoKeyspace
	}

	ksvs, err := topotools.ApplyVSchemaDDL(ctx, ksName, vc.topoServer, vschemaDDL)
	if err != nil {
		return err
	}

	srvVschema.Keyspaces[ksName] = ksvs.Keyspace

	return vc.vm.UpdateVSchema(ctx, ksvs, srvVschema)
}

func (vc *VCursorImpl) MessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, tableName string, callback func(*sqltypes.Result) error) error {
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(len(rss)))
	return vc.executor.ExecuteMessageStream(ctx, rss, tableName, callback)
}

func (vc *VCursorImpl) VStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	return vc.executor.ExecuteVStream(ctx, rss, filter, gtid, callback)
}

func (vc *VCursorImpl) ShowExec(ctx context.Context, command sqlparser.ShowCommandType, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	switch command {
	case sqlparser.VitessReplicationStatus:
		return vc.executor.ShowVitessReplicationStatus(ctx, filter)
	case sqlparser.VitessShards:
		return vc.executor.ShowShards(ctx, filter, vc.tabletType)
	case sqlparser.VitessTablets:
		return vc.executor.ShowTablets(filter)
	case sqlparser.VitessVariables:
		return vc.executor.ShowVitessMetadata(ctx, filter)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "bug: unexpected show command: %v", command)
	}
}

func (vc *VCursorImpl) SetExec(ctx context.Context, name string, value string) error {
	return vc.executor.SetVitessMetadata(ctx, name, value)
}

func (vc *VCursorImpl) ThrottleApp(ctx context.Context, throttledAppRule *topodatapb.ThrottledAppRule) (err error) {
	if throttledAppRule == nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ThrottleApp: nil rule")
	}
	if throttledAppRule.Name == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ThrottleApp: app name is empty")
	}
	// We don't strictly have to construct a UpdateThrottlerConfigRequest here, because we only populate it
	// with a couple variables; we could do without it. However, constructing the request makes the remaining code
	// consistent with vtctldclient/command/throttler.go and we prefer this consistency
	req := &vtctldatapb.UpdateThrottlerConfigRequest{
		Keyspace:     vc.keyspace,
		ThrottledApp: throttledAppRule,
	}

	update := func(throttlerConfig *topodatapb.ThrottlerConfig) *topodatapb.ThrottlerConfig {
		if throttlerConfig == nil {
			throttlerConfig = &topodatapb.ThrottlerConfig{}
		}
		if throttlerConfig.ThrottledApps == nil {
			throttlerConfig.ThrottledApps = make(map[string]*topodatapb.ThrottledAppRule)
		}
		if req.ThrottledApp != nil && req.ThrottledApp.Name != "" {
			timeNow := time.Now()
			if protoutil.TimeFromProto(req.ThrottledApp.ExpiresAt).After(timeNow) {
				throttlerConfig.ThrottledApps[req.ThrottledApp.Name] = req.ThrottledApp
			} else {
				delete(throttlerConfig.ThrottledApps, req.ThrottledApp.Name)
			}
		}
		return throttlerConfig
	}

	ctx, unlock, lockErr := vc.topoServer.LockKeyspace(ctx, req.Keyspace, "UpdateThrottlerConfig")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	ki, err := vc.topoServer.GetKeyspace(ctx, req.Keyspace)
	if err != nil {
		return err
	}

	ki.ThrottlerConfig = update(ki.ThrottlerConfig)

	err = vc.topoServer.UpdateKeyspace(ctx, ki)
	if err != nil {
		return err
	}

	_, err = vc.topoServer.UpdateSrvKeyspaceThrottlerConfig(ctx, req.Keyspace, []string{}, update)

	return err
}

func (vc *VCursorImpl) ReleaseLock(ctx context.Context) error {
	return vc.executor.ReleaseLock(ctx, vc.SafeSession)
}

func (vc *VCursorImpl) ReadTransaction(ctx context.Context, transactionID string) (*querypb.TransactionMetadata, error) {
	return vc.executor.ReadTransaction(ctx, transactionID)
}

// UnresolvedTransactions gets the unresolved transactions for the given keyspace. If the keyspace is not given,
// then we use the default keyspace.
func (vc *VCursorImpl) UnresolvedTransactions(ctx context.Context, keyspace string) ([]*querypb.TransactionMetadata, error) {
	if keyspace == "" {
		keyspace = vc.GetKeyspace()
	}
	rss, _, err := vc.ResolveDestinations(ctx, keyspace, nil, []key.ShardDestination{key.DestinationAllShards{}})
	if err != nil {
		return nil, err
	}
	var targets []*querypb.Target
	for _, rs := range rss {
		targets = append(targets, rs.Target)
	}
	return vc.executor.UnresolvedTransactions(ctx, targets)
}
