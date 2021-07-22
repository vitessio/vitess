package topo

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/protoutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"
)

// WatchTopoEventData is the data returned on each watch event from
// WatchTopoEvents. It contains the topology events that are currently
// happening on the cluster.
type WatchTopoEventData struct {
	// ActiveEvents is a map of all the events that are currently happening
	// on the cluster, keyed by their UUID.
	ActiveEvents map[string]*topodatapb.TopoEvent

	// Error is the last error that happened while watching for topology events
	Err error
}

func (ts *Server) createTopoEventLog(ctx context.Context, entries ...*topodatapb.TopoEvent) error {
	active := &topodatapb.ActiveTopoEvents{}

	if len(entries) > 0 {
		active.Events = make(map[string]*topodatapb.TopoEvent)
		for _, ev := range entries {
			active.Events[ev.Uuid] = ev
		}
	}

	newData, err := proto.Marshal(active)
	if err != nil {
		return vterrors.Wrapf(err, "TopoEventLog marshal failed: %v", newData)
	}

	_, err = ts.globalCell.Create(ctx, TopoEventFile, newData)
	return err
}

// WatchTopoEvents watches the topology event log for topology change events
// in real time. If there's no topology event log in the cluster, a new empty
// one will be created.
func (ts *Server) WatchTopoEvents(ctx context.Context) (*WatchTopoEventData, <-chan *WatchTopoEventData, CancelFunc) {
	current, wdChannel, cancel := ts.globalCell.Watch(ctx, TopoEventFile)
	if current.Err != nil {
		if IsErrType(current.Err, NoNode) {
			current.Err = ts.createTopoEventLog(ctx)
			if current.Err == nil || IsErrType(current.Err, NodeExists) {
				return ts.WatchTopoEvents(ctx)
			}
		}
		return &WatchTopoEventData{Err: current.Err}, nil, nil
	}
	value := &topodatapb.ActiveTopoEvents{}
	if err := proto.Unmarshal(current.Contents, value); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return &WatchTopoEventData{Err: vterrors.Wrapf(err, "error unpacking initial TopoEvent object")}, nil, nil
	}

	changes := make(chan *WatchTopoEventData, 10)

	// The background routine reads any event from the watch channel,
	// translates it, and sends it to the caller.
	// If cancel() is called, the underlying Watch() code will
	// send an ErrInterrupted and then close the channel. We'll
	// just propagate that back to our caller.
	go func() {
		defer close(changes)

		for wd := range wdChannel {
			if wd.Err != nil {
				// Last error value, we're done.
				// wdChannel will be closed right after
				// this, no need to do anything.
				changes <- &WatchTopoEventData{Err: wd.Err}
				return
			}

			value := &topodatapb.ActiveTopoEvents{}
			if err := proto.Unmarshal(wd.Contents, value); err != nil {
				cancel()
				for range wdChannel {
				}
				changes <- &WatchTopoEventData{Err: vterrors.Wrapf(err, "error unpacking TopoEvent object")}
				return
			}

			changes <- &WatchTopoEventData{
				ActiveEvents: value.Events,
			}
		}
	}()

	return &WatchTopoEventData{ActiveEvents: value.Events}, changes, cancel
}

const MaximumTopoLogDuration = 24 * time.Hour

func (ts *Server) atomicUpdateTopoEvent(ctx context.Context, update func(map[string]*topodatapb.TopoEvent) error) error {
	nodePath := TopoEventFile
	for {
		data, version, err := ts.globalCell.Get(ctx, nodePath)
		if err != nil {
			return err
		}

		active := &topodatapb.ActiveTopoEvents{}
		if err := proto.Unmarshal(data, active); err != nil {
			return vterrors.Wrapf(err, "TopoEventLog unmarshal failed: %v", data)
		}

		var condensed = make(map[string]*topodatapb.TopoEvent)
		var now = ts.now()
		for _, ev := range active.Events {
			if ev.FinishedAt != nil {
				started := protoutil.TimeFromProto(ev.StartedAt)
				if now.Sub(started) > MaximumTopoLogDuration {
					continue
				}
			}

			condensed[ev.Uuid] = ev
		}

		if err := update(condensed); err != nil {
			return vterrors.Wrapf(err, "failed to update ActiveTopoEvent")
		}

		active.Events = condensed
		updatedData, err := proto.Marshal(active)
		if err != nil {
			return vterrors.Wrapf(err, "TopoEventLog marshal failed: %v", data)
		}

		_, err = ts.globalCell.Update(ctx, nodePath, updatedData, version)
		if err != nil {
			if IsErrType(err, BadVersion) {
				continue
			}
			return vterrors.Wrapf(err, "TopoEventLog update failed: %v", data)
		}
		return nil
	}
}

// AppendTopoEvent appends the given event to the topology event log.
// The log is always kept consistently sorted by StartTime for all events.
func (ts *Server) AppendTopoEvent(ctx context.Context, newEvent *topodatapb.TopoEvent) error {
	err := ts.atomicUpdateTopoEvent(ctx, func(active map[string]*topodatapb.TopoEvent) error {
		if _, found := active[newEvent.Uuid]; found {
			return fmt.Errorf("duplicate event UUID: %q", newEvent.Uuid)
		}
		active[newEvent.Uuid] = newEvent
		return nil
	})

	if err != nil {
		if IsErrType(err, NoNode) {
			err = ts.createTopoEventLog(ctx, newEvent)
			if err != nil {
				if IsErrType(err, NodeExists) {
					return ts.AppendTopoEvent(ctx, newEvent)
				}
			}
		}
		return vterrors.Wrapf(err, "failed to update ActiveTopoEvents in place")
	}

	return nil
}

// ResolveTopoEvent marks a specific event in the topology events log as resolved
func (ts *Server) ResolveTopoEvent(ctx context.Context, uuid string) error {
	err := ts.atomicUpdateTopoEvent(ctx, func(active map[string]*topodatapb.TopoEvent) error {
		ev := active[uuid]
		if ev == nil {
			return fmt.Errorf("cannot resolve topo event %q (not found)", uuid)
		}
		if ev.FinishedAt == nil {
			ev.FinishedAt = protoutil.TimeToProto(ts.now())
		}
		return nil
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to update ActiveTopoEvents in place")
	}
	return nil
}
