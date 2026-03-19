package vstreamclient

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"vitess.io/vitess/go/sqlescape"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
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
)

// Option is a function that can be used to configure a VStreamClient
type Option func(v *VStreamClient) error

// WithMinFlushDuration sets the minimum duration between flushes. This is useful for ensuring that data
// isn't flushed too often, which can be inefficient. The default is 30 seconds.
func WithMinFlushDuration(d time.Duration) Option {
	return func(v *VStreamClient) error {
		if d <= 0 {
			return fmt.Errorf("vstreamclient: minimum flush duration must be positive, got %s", d.String())
		}

		v.cfg.minFlushDuration = d
		return nil
	}
}

func WithHeartbeatSeconds(seconds int) Option {
	return func(v *VStreamClient) error {
		if seconds <= 0 {
			return fmt.Errorf("vstreamclient: heartbeat seconds must be positive, got %d", seconds)
		}

		if uint64(seconds) > math.MaxUint32 {
			return fmt.Errorf("vstreamclient: heartbeat seconds must be %d or less, got %d", uint64(math.MaxUint32), seconds)
		}

		v.cfg.heartbeatSeconds = seconds
		return nil
	}
}

func WithTimeLocation(loc *time.Location) Option {
	return func(v *VStreamClient) error {
		if loc == nil {
			return errors.New("vstreamclient: time location cannot be nil")
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
			return errors.New("vstreamclient: tablet type cannot be UNKNOWN")
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
			return errors.New("vstreamclient: graceful shutdown channel is required")
		}
		if wait <= 0 {
			return fmt.Errorf("vstreamclient: graceful shutdown wait must be positive, got %s", wait)
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
			return errors.New("vstreamclient: graceful shutdown signals are required")
		}
		if wait <= 0 {
			return fmt.Errorf("vstreamclient: graceful shutdown wait must be positive, got %s", wait)
		}

		v.cfg.gracefulShutdownSignals = append([]os.Signal(nil), signals...)
		v.cfg.gracefulShutdownWaitDur = wait
		return nil
	}
}

func WithStateTable(keyspace, table string) Option {
	return func(v *VStreamClient) error {
		shards, ok := v.shardsByKeyspace[keyspace]
		if !ok {
			return fmt.Errorf("vstreamclient: keyspace %s not found", keyspace)
		}

		// this could allow for shard pinning, but we can support that if it becomes useful
		if len(shards) > 1 {
			return fmt.Errorf("vstreamclient: keyspace %s is sharded, only unsharded keyspaces are supported", keyspace)
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
			return errors.New("vstreamclient: flags cannot be nil")
		}
		if flags.HeartbeatInterval == 0 {
			return errors.New("vstreamclient: HeartbeatInterval must be positive")
		}
		v.cfg.flags = flags
		return nil
	}
}

// WithEventFunc provides for custom event handling functions for specific event types. Only one function
// can be registered per event type, and it is called before the default event handling function. Returning
// an error from the custom function will exit the stream before the default function is called.
func WithEventFunc(fn EventFunc, eventTypes ...binlogdatapb.VEventType) Option {
	return func(v *VStreamClient) error {
		if len(eventTypes) == 0 {
			return errors.New("vstreamclient: no event types provided")
		}

		if fn == nil {
			return errors.New("vstreamclient: event func cannot be nil")
		}

		if v.cfg.eventFuncs == nil {
			v.cfg.eventFuncs = make(map[binlogdatapb.VEventType]EventFunc)
		}

		for _, eventType := range eventTypes {
			if _, ok := v.cfg.eventFuncs[eventType]; ok {
				return fmt.Errorf("vstreamclient: event type %s already has a function", eventType.String())
			}

			v.cfg.eventFuncs[eventType] = fn
		}

		return nil
	}
}

// WithStartingVGtid sets the starting VGtid for the VStreamClient. This is useful for resuming a stream from a
// specific point, vs what might be stored in the state table.
func WithStartingVGtid(vgtid *binlogdatapb.VGtid) Option {
	return func(v *VStreamClient) error {
		v.latestVgtid = vgtid
		return nil
	}
}
