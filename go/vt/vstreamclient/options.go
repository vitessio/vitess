/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"math"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqlescape"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// DefaultMinFlushDuration is the default minimum duration between flushes, used if not explicitly
	// set using WithMinFlushDuration. This can be safely modified if needed before calling New.
	DefaultMinFlushDuration = 30 * time.Second

	// DefaultMaxRowsPerFlush is the default number of rows to buffer per table, used if not explicitly
	// set in the table configuration. This same number is also used to chunk rows when calling flush.
	// This can be safely modified if needed before calling New.
	DefaultMaxRowsPerFlush = 1000

	// DefaultGracefulShutdownWaitDur is the default duration to wait for the stream to gracefully shutdown after
	// receiving a shutdown signal, used if not explicitly set using WithGracefulShutdownChan or
	// WithGracefulShutdownSignals. This can be safely modified if needed before calling New.
	DefaultGracefulShutdownWaitDur = 5 * time.Second

	// DefaultStartupTimeout is how long the client waits for the first event after Run starts before it
	// shuts itself down with ErrStartupTimeout. This can be safely modified if needed before calling New.
	DefaultStartupTimeout = 5 * time.Minute

	// DefaultHeartbeatTimeoutMultiplier controls the liveness window as a multiple of the heartbeat
	// interval: once the first event has been received, if no event (including heartbeats) is received
	// for that window, the client shuts itself down with ErrHeartbeatTimeout. This can be safely
	// modified if needed before calling New.
	DefaultHeartbeatTimeoutMultiplier = 2
)

// Option is a function that can be used to configure a VStreamClient
type Option func(v *VStreamClient) error

// WithMinFlushDuration sets the minimum duration between flushes. This is useful for ensuring that data
// isn't flushed too often, which can be inefficient. The default is 30 seconds.
func WithMinFlushDuration(d time.Duration) Option {
	return func(v *VStreamClient) error {
		if d <= 0 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: minimum flush duration must be positive, got %s", d.String())
		}

		v.cfg.minFlushDuration = d
		return nil
	}
}

func WithHeartbeatSeconds(seconds int) Option {
	return func(v *VStreamClient) error {
		if seconds <= 0 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: heartbeat seconds must be positive, got %d", seconds)
		}

		if uint64(seconds) > math.MaxUint32 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: heartbeat seconds must be %d or less, got %d", uint64(math.MaxUint32), seconds)
		}

		v.cfg.heartbeatSeconds = seconds
		return nil
	}
}

func WithTimeLocation(loc *time.Location) Option {
	return func(v *VStreamClient) error {
		if loc == nil {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: time location cannot be nil")
		}

		v.cfg.timeLocation = loc
		return nil
	}
}

// WithTabletType overrides the tablet type used when opening the VStream.
// The default is REPLICA.
func WithTabletType(tabletType topodatapb.TabletType) Option {
	return func(v *VStreamClient) error {
		if tabletType == topodatapb.TabletType_UNKNOWN {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: tablet type cannot be UNKNOWN")
		}

		v.cfg.tabletType = tabletType
		return nil
	}
}

// WithGracefulShutdownChan triggers GracefulShutdown when the provided channel is closed.
// A common pattern is to pass a channel derived from signal.Notify or signal.NotifyContext.
func WithGracefulShutdownChan(ch <-chan struct{}, wait time.Duration) Option {
	return func(v *VStreamClient) error {
		if ch == nil {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: graceful shutdown channel is required")
		}
		if wait <= 0 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: graceful shutdown wait must be positive, got %s", wait)
		}

		v.cfg.gracefulShutdownChan = ch
		v.cfg.gracefulShutdownWaitDur = wait
		return nil
	}
}

// WithGracefulShutdownSignals triggers GracefulShutdown when one of the provided
// OS signals is received. A common pattern is to pass os.Interrupt and syscall.SIGTERM.
func WithGracefulShutdownSignals(wait time.Duration, signals ...os.Signal) Option {
	return func(v *VStreamClient) error {
		if len(signals) == 0 {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: graceful shutdown signals are required")
		}
		if wait <= 0 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: graceful shutdown wait must be positive, got %s", wait)
		}

		v.cfg.gracefulShutdownSignals = append([]os.Signal(nil), signals...)
		v.cfg.gracefulShutdownWaitDur = wait
		return nil
	}
}

func WithStateTable(keyspace, table string) Option {
	return func(v *VStreamClient) error {
		if table == "" {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: state table name is required")
		}

		shards, ok := v.shardsByKeyspace[keyspace]
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: keyspace %s not found", keyspace)
		}

		// unsharded keyspaces always have exactly one shard named "0". A sharded keyspace can
		// currently have a single shard (named "-") and can gain shards through resharding, so
		// checking the shard count alone is not enough.
		if len(shards) != 1 || shards[0] != "0" {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: keyspace %s is sharded, only unsharded keyspaces are supported", keyspace)
		}

		// checkpoint writes are themselves transactions: if the state keyspace is also a source
		// keyspace, every checkpoint advances the stream position and schedules another
		// checkpoint, creating a self-sustaining write loop on an otherwise idle stream
		for _, tbl := range v.tables {
			if tbl.Keyspace == keyspace {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: state keyspace %s is also a streamed source keyspace; store checkpoints in a keyspace that is not being streamed", keyspace)
			}
		}

		v.cfg.vgtidStateKeyspace = sqlescape.EscapeID(keyspace)
		v.cfg.vgtidStateTable = sqlescape.EscapeID(table)
		return nil
	}
}

// DefaultFlags returns a default set of flags for a VStreamClient, safe to use in most cases, but can be customized
func DefaultFlags() *vtgatepb.VStreamFlags {
	return &vtgatepb.VStreamFlags{
		HeartbeatInterval: 1,
		// Keep keyspace in TableName so multiple-keyspace streams disambiguate tables.
		ExcludeKeyspaceFromTableName: false,
	}
}

// WithFlags lets you manually control all the flag options, instead of using helper functions
func WithFlags(flags *vtgatepb.VStreamFlags) Option {
	return func(v *VStreamClient) error {
		if flags == nil {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: flags cannot be nil")
		}
		if flags.HeartbeatInterval == 0 {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: HeartbeatInterval must be positive")
		}
		if flags.StreamKeyspaceHeartbeats {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: StreamKeyspaceHeartbeats is not supported: it streams internal sidecar heartbeat table events that have no TableConfig")
		}

		// clone so later caller mutations can't change stream behavior or bypass the validation above
		v.cfg.flags = proto.Clone(flags).(*vtgatepb.VStreamFlags)
		return nil
	}
}

// WithEventFunc provides for custom event handling functions for specific event types. Only one function
// can be registered per event type, and it is called before the default event handling function. Returning
// an error from the custom function will exit the stream before the default function is called.
func WithEventFunc(fn EventFunc, eventTypes ...binlogdatapb.VEventType) Option {
	return func(v *VStreamClient) error {
		if len(eventTypes) == 0 {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: no event types provided")
		}

		if fn == nil {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: event func cannot be nil")
		}

		if v.cfg.eventFuncs == nil {
			v.cfg.eventFuncs = make(map[binlogdatapb.VEventType]EventFunc)
		}

		for _, eventType := range eventTypes {
			if _, ok := v.cfg.eventFuncs[eventType]; ok {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: event type %s already has a function", eventType.String())
			}

			v.cfg.eventFuncs[eventType] = fn
		}

		return nil
	}
}

// WithStartingVGtid sets the starting VGtid for the VStreamClient. This is useful for resuming a stream from a
// specific point, vs what might be stored in the state table.
//
// The position is persisted with copy_completed=true and becomes the durable restart point, so every
// shard gtid must be a concrete position: symbolic positions like "current" are resolved afresh by
// VTGate on every run, which would silently skip rows delivered between a crash and the restart.
func WithStartingVGtid(vgtid *binlogdatapb.VGtid) Option {
	return func(v *VStreamClient) error {
		if vgtid == nil || len(vgtid.ShardGtids) == 0 {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: starting vgtid must include at least one shard gtid")
		}

		for _, shardGtid := range vgtid.ShardGtids {
			if shardGtid == nil || shardGtid.Keyspace == "" || shardGtid.Shard == "" {
				return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: every starting shard gtid must name a keyspace and shard")
			}
			if shardGtid.Gtid == "" || strings.EqualFold(shardGtid.Gtid, "current") {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: starting gtid for %s/%s must be a concrete position, got %q: symbolic or empty positions cannot be persisted as a restart point", shardGtid.Keyspace, shardGtid.Shard, shardGtid.Gtid)
			}
		}

		// clone so later caller mutations can't change or race with client state
		v.latestVgtid = proto.Clone(vgtid).(*binlogdatapb.VGtid)
		return nil
	}
}
