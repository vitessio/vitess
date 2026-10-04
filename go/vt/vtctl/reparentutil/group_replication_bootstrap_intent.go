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

package reparentutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// GroupReplicationBootstrapIntentFence is how long a bootstrap intent keeps another bootstrap of the
// shard's group, on another tablet, from starting. A bootstrap whose reply was lost may still be
// running: MySQL keeps running a START GROUP_REPLICATION whose client left, and it can wait behind
// the member's own leave of its previous group for tens of seconds. The tablet trusts a group it
// bootstrapped itself for a minute before the incarnation is recorded; the fence outlasts it. A
// bootstrap on the intent's target itself is not fenced: the tablet stops a START still in progress
// before it starts again, and a member that is already active refuses the bootstrap, which then
// leads to the adoption of its group.
var GroupReplicationBootstrapIntentFence = 2 * time.Minute

// GroupReplicationAdoptionStatusTimeout bounds the read of the intent target's status when VTOrc
// checks whether to adopt its group. It runs under the shard lock, often right after the bootstrap
// RPC failed because the target was cut off again: a target that does not answer quickly is
// checked again on a later pass, rather than holding the lock that other recoveries need.
var GroupReplicationAdoptionStatusTimeout = 5 * time.Second

// GroupReplicationBootstrapIntentClockSkew is how much earlier than its bootstrap intent a group
// may have been created, by MySQL's clock, for the intent to adopt it: the intent's time is VTOrc's
// clock (see adoptableGroupIncarnation).
var GroupReplicationBootstrapIntentClockSkew = 30 * time.Second

// WriteGroupReplicationBootstrapIntent records in the shard record that the caller is about to
// bootstrap the shard's group on target, and returns the intent. The caller must hold the shard
// lock, which is re-checked first, and must have read expectedIncarnation from the shard record
// before it decided to bootstrap; the write is a compare-and-swap against it.
//
// The intent is a fence: while an intent for another target is younger than
// GroupReplicationBootstrapIntentFence, and the incarnation it was recorded for is still the
// shard's, the write fails with FAILED_PRECONDITION. VTOrc checks before a bootstrap that every
// voter is reachable and none is an active member, but a bootstrap whose reply was lost may still
// be running on the other target, its member not active yet: a second bootstrap would create a
// second group. The intent of the same target is replaced.
func WriteGroupReplicationBootstrapIntent(ctx context.Context, ts *topo.Server, keyspace, shard string, target *topodatapb.TabletAlias, expectedIncarnation string, now time.Time) (*topodatapb.GroupReplicationBootstrapIntent, error) {
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return nil, vterrors.Wrap(err, lostTopologyLockMsg)
	}
	token, err := newBootstrapIntentToken(now)
	if err != nil {
		return nil, err
	}
	intent := &topodatapb.GroupReplicationBootstrapIntent{
		Target:              target,
		Time:                protoutil.TimeToProto(now),
		PreviousIncarnation: expectedIncarnation,
		Token:               token,
	}
	_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		if si.GroupReplicationIncarnation != expectedIncarnation {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard record of %s/%s lists the group replication incarnation %q instead of %q: it changed concurrently, not bootstrapping the group",
				keyspace, shard, si.GroupReplicationIncarnation, expectedIncarnation)
		}
		if existing := LiveGroupReplicationBootstrapIntent(si.Shard, now); existing != nil && !topoproto.TabletAliasEqual(existing.Target, target) {
			started := protoutil.TimeFromProto(existing.Time)
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "a bootstrap of the replication group of %s/%s on %s started at %s and may still run: not bootstrapping it on %s before %s",
				keyspace, shard, topoproto.TabletAliasString(existing.Target), started.UTC().Format(time.RFC3339Nano),
				topoproto.TabletAliasString(target), started.Add(GroupReplicationBootstrapIntentFence).UTC().Format(time.RFC3339Nano))
		}
		si.GroupReplicationBootstrapIntent = intent
		return nil
	})
	if err != nil {
		if vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION {
			return nil, err
		}
		return nil, vterrors.Wrapf(err, "failed to store the group replication bootstrap intent of shard %s/%s", keyspace, shard)
	}
	return intent, nil
}

// newBootstrapIntentToken returns a token that identifies a bootstrap intent.
func newBootstrapIntentToken(now time.Time) (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", vterrors.Wrapf(err, "failed to generate a bootstrap intent token")
	}
	return fmt.Sprintf("%d-%s", now.UnixNano(), hex.EncodeToString(random)), nil
}

// CurrentGroupReplicationBootstrapIntent returns the shard's bootstrap intent if it still applies:
// it was recorded for the incarnation that the shard record lists now. An intent for another
// incarnation was superseded, for example by a component that does not know intents and recorded
// an incarnation without clearing it.
func CurrentGroupReplicationBootstrapIntent(shard *topodatapb.Shard) *topodatapb.GroupReplicationBootstrapIntent {
	intent := shard.GetGroupReplicationBootstrapIntent()
	if intent == nil || intent.GetTarget() == nil || intent.GetPreviousIncarnation() != shard.GetGroupReplicationIncarnation() {
		return nil
	}
	return intent
}

// LiveGroupReplicationBootstrapIntent returns the shard's current bootstrap intent (see
// CurrentGroupReplicationBootstrapIntent) while it is younger than
// GroupReplicationBootstrapIntentFence, nil otherwise.
func LiveGroupReplicationBootstrapIntent(shard *topodatapb.Shard, now time.Time) *topodatapb.GroupReplicationBootstrapIntent {
	intent := CurrentGroupReplicationBootstrapIntent(shard)
	if intent == nil || now.Sub(protoutil.TimeFromProto(intent.GetTime())) >= GroupReplicationBootstrapIntentFence {
		return nil
	}
	return intent
}

// adoptableGroupIncarnation returns the incarnation of the group that the intent's target reports
// in status, if that group may be adopted as the shard's group for the intent, which the shard
// record still lists with recordedIncarnation. It returns a FAILED_PRECONDITION error otherwise.
//
// A group is adopted only when all of these hold:
//   - The intent still applies: the shard record lists the incarnation it was recorded for.
//   - The group's primary is the intent's target: its MySQL is the ONLINE primary, with quorum,
//     of the shard's group.
//   - Its incarnation is new: it differs from the incarnation the intent was recorded for.
//   - It was created after the intent. VTOrc records the intent under the shard lock only after a
//     fresh read of every voter found none of them in a group, so any group in which the target is
//     the primary afterwards was formed after it; the time that MySQL encodes in the incarnation
//     must not contradict that, by more than GroupReplicationBootstrapIntentClockSkew.
//
// A failed join can leave a member alone in a group of its own, a stray incarnation that has
// quorum in its view of one. Such a group is never adopted unless its member is the intent's
// target, the voter that VTOrc found to hold every other voter's transactions; for the target,
// a group of its own after the intent is what the bootstrap creates.
func adoptableGroupIncarnation(intent *topodatapb.GroupReplicationBootstrapIntent, recordedIncarnation string, target *topodatapb.Tablet, status *replicationdatapb.FullStatus) (string, error) {
	targetAlias := topoproto.TabletAliasString(target.GetAlias())
	if intent == nil || intent.GetTarget() == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard record of %s/%s holds no bootstrap intent", target.GetKeyspace(), target.GetShard())
	}
	if !topoproto.TabletAliasEqual(intent.GetTarget(), target.GetAlias()) {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the bootstrap intent of %s/%s is for %s, not %s",
			target.GetKeyspace(), target.GetShard(), topoproto.TabletAliasString(intent.GetTarget()), targetAlias)
	}
	if intent.GetPreviousIncarnation() != recordedIncarnation {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the bootstrap intent of %s/%s was recorded for the incarnation %q, but the shard record lists %q",
			target.GetKeyspace(), target.GetShard(), intent.GetPreviousIncarnation(), recordedIncarnation)
	}
	gs := status.GetGroupReplicationStatus()
	if !mysql.IsGroupPrimary(gs) || gs.GetGroupName() != policy.GroupName(target.GetKeyspace(), target.GetShard()) {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of %s is not the primary of the shard's group with quorum (group %s, member %s %s, quorum %t)",
			targetAlias, gs.GetGroupName(), gs.GetMemberState(), gs.GetMemberRole(), gs.GetHasQuorum())
	}
	if gs.GetPrimaryUuid() != "" && status.GetServerUuid() != "" && gs.GetPrimaryUuid() != status.GetServerUuid() {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the primary of the group of %s is %s, not its MySQL %s", targetAlias, gs.GetPrimaryUuid(), status.GetServerUuid())
	}
	incarnation := policy.GroupIncarnation(gs.GetViewId())
	if incarnation == "" || incarnation == intent.GetPreviousIncarnation() {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the group of %s has the incarnation %q, which the bootstrap intent was recorded for, not a new one", targetAlias, incarnation)
	}
	intentTime := protoutil.TimeFromProto(intent.GetTime())
	if created, ok := policy.GroupIncarnationTime(incarnation); ok && created.Before(intentTime.Add(-GroupReplicationBootstrapIntentClockSkew)) {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the group of %s, incarnation %s, was created at %s, before the bootstrap intent of %s",
			targetAlias, incarnation, created.Format(time.RFC3339Nano), intentTime.UTC().Format(time.RFC3339Nano))
	}
	return incarnation, nil
}

// AdoptGroupReplicationBootstrap records the incarnation of the group that the bootstrap intent's
// target created, when the bootstrap's reply was lost: the RPC failed or timed out, or the
// component that asked for it stopped before it recorded the incarnation. It reads the target's
// status now and checks that its group may be adopted (see adoptableGroupIncarnation), then records
// its incarnation with a compare-and-swap against the incarnation the intent was recorded for, as
// long as the shard record still holds the intent; that clears the intent. The caller holds the
// shard lock, and passes the shard record's incarnation and intent as it read them under it. It
// returns the recorded incarnation.
func AdoptGroupReplicationBootstrap(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, keyspace, shard, recordedIncarnation string, intent *topodatapb.GroupReplicationBootstrapIntent, target *topodatapb.Tablet) (string, error) {
	res := fetchFullStatus(ctx, tmc, target, GroupReplicationAdoptionStatusTimeout)
	if res.err != nil {
		return "", vterrors.Wrapf(res.err, "cannot read the status of %s, the target of the bootstrap intent of %s/%s", topoproto.TabletAliasString(target.GetAlias()), keyspace, shard)
	}
	incarnation, err := adoptableGroupIncarnation(intent, recordedIncarnation, target, res.status)
	if err != nil {
		return "", err
	}
	if err := writeGroupReplicationIncarnation(ctx, ts, keyspace, shard, intent.GetPreviousIncarnation(), incarnation, intent.GetToken()); err != nil {
		return "", err
	}
	return incarnation, nil
}

// RecordGroupReplicationBootstrap records the incarnation of the group that the caller bootstrapped
// for the intent, as the bootstrap's reply reported it: a compare-and-swap against the incarnation
// the intent was recorded for, as long as the shard record still holds the intent. It clears the
// intent.
func RecordGroupReplicationBootstrap(ctx context.Context, ts *topo.Server, keyspace, shard string, intent *topodatapb.GroupReplicationBootstrapIntent, incarnation string) error {
	if incarnation == "" || incarnation == intent.GetPreviousIncarnation() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the bootstrapped group of %s/%s reports the incarnation %q, not a new one", keyspace, shard, incarnation)
	}
	return writeGroupReplicationIncarnation(ctx, ts, keyspace, shard, intent.GetPreviousIncarnation(), incarnation, intent.GetToken())
}

// WithdrawGroupReplicationBootstrapIntent removes the caller's bootstrap intent from the shard
// record, once the intent's target refused the bootstrap definitively (see
// tmclient.GroupBootstrapRefusedError): the tablet proved, under its action lock, that the RPC did
// not start MySQL's bootstrap and never will, and no other RPC carries the intent's token. The
// intent then fences nothing, and the caller can bootstrap the group on another voter on its next
// pass, instead of waiting GroupReplicationBootstrapIntentFence for it to expire.
//
// The caller must hold the shard lock, which is re-checked first, as for the other writes of the
// intent. The write is a compare-and-swap: it removes the intent only while the shard record still
// holds it, the same token, for the incarnation it was recorded for. A newer intent, written by
// another VTOrc that took the shard lock after the caller's lease expired, and an incarnation
// recorded since, are left as they are. It returns whether it removed the intent.
func WithdrawGroupReplicationBootstrapIntent(ctx context.Context, ts *topo.Server, keyspace, shard string, intent *topodatapb.GroupReplicationBootstrapIntent) (bool, error) {
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return false, vterrors.Wrap(err, lostTopologyLockMsg)
	}
	if intent.GetToken() == "" {
		return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot withdraw a bootstrap intent of %s/%s without a token", keyspace, shard)
	}
	withdrawn := false
	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		withdrawn = false
		current := si.GroupReplicationBootstrapIntent
		if current.GetToken() != intent.GetToken() || si.GroupReplicationIncarnation != intent.GetPreviousIncarnation() {
			return topo.NewError(topo.NoUpdateNeeded, keyspace+"/"+shard)
		}
		si.GroupReplicationBootstrapIntent = nil
		withdrawn = true
		return nil
	})
	if err != nil {
		return false, vterrors.Wrapf(err, "failed to withdraw the group replication bootstrap intent %s of shard %s/%s", intent.GetToken(), keyspace, shard)
	}
	return withdrawn, nil
}
