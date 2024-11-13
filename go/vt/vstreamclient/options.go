package vstreamclient

import (
	"fmt"
	"time"

	"vitess.io/vitess/go/sqlescape"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var (
	// DefaultMinFlushDuration is the default minimum duration between flushes, used if not explicitly
	// set using WithMinFlushDuration. This can be safely modified if needed before calling New.
	DefaultMinFlushDuration = 5 * time.Second

	// DefaultMaxRowsPerFlush is the default number of rows to buffer per table, used if not explicitly
	// set in the table configuration. This same number is also used to chunk rows when calling flush.
	// This can be safely modified if needed before calling New.
	DefaultMaxRowsPerFlush = 1000
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

		v.minFlushDuration = d
		return nil
	}
}

func WithHeartbeatSeconds(seconds int) Option {
	return func(v *VStreamClient) error {
		if seconds <= 0 {
			return fmt.Errorf("vstreamclient: heartbeat seconds must be positive, got %d", seconds)
		}

		v.heartbeatSeconds = seconds
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

		v.vgtidStateKeyspace = sqlescape.EscapeID(keyspace)
		v.vgtidStateTable = sqlescape.EscapeID(table)
		return nil
	}
}

// DefaultFlags returns a default set of flags for a VStreamClient, safe to use in most cases, but can be customized
func DefaultFlags() *vtgatepb.VStreamFlags {
	return &vtgatepb.VStreamFlags{
		HeartbeatInterval: 1,
	}
}

// WithFlags lets you manually control all the flag options, instead of using helper functions
func WithFlags(flags *vtgatepb.VStreamFlags) Option {
	return func(v *VStreamClient) error {
		v.flags = flags
		return nil
	}
}

// WithEventFunc provides for custom event handling functions for specific event types. Only one function
// can be registered per event type, and it is called before the default event handling function. Returning
// an error from the custom function will exit the stream before the default function is called.
func WithEventFunc(fn EventFunc, eventTypes ...binlogdatapb.VEventType) Option {
	return func(v *VStreamClient) error {
		if len(eventTypes) == 0 {
			return fmt.Errorf("vstreamclient: no event types provided")
		}

		if v.eventFuncs == nil {
			v.eventFuncs = make(map[binlogdatapb.VEventType]EventFunc)
		}

		for _, eventType := range eventTypes {
			if _, ok := v.eventFuncs[eventType]; ok {
				return fmt.Errorf("vstreamclient: event type %s already has a function", eventType.String())
			}

			v.eventFuncs[eventType] = fn
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
