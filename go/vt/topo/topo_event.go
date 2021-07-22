package topo

import (
	"context"
	"sort"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/protoutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"
)

// WatchTopoEventData is the data returned on each watch event from
// WatchTopoEventLog. It contains the topology events that have happened
// on the cluster since the last time this method was called.
type WatchTopoEventData struct {
	// NewLogEntries are the topology events that have happened on the cluster
	// since the last time we watched the topology server
	NewLogEntries []*topodatapb.TopoEvent

	// FullLog is the full list of topology events in the cluster as of the last
	// watch event, and it contains events that have potentially been seen before
	FullLog []*topodatapb.TopoEvent

	// Error is the last error that happened while watching for topology events
	Err error
}

func (ts *Server) createTopoEventLog(ctx context.Context, entries []*topodatapb.TopoEvent) error {
	topoEvLog := &topodatapb.TopoEventLog{
		Log: entries,
	}

	newData, err := proto.Marshal(topoEvLog)
	if err != nil {
		return vterrors.Wrapf(err, "TopoEventLog marshal failed: %v", newData)
	}

	_, err = ts.globalCell.Create(ctx, TopoEventFile, newData)
	return err
}

// WatchTopoEventLog watches the topology event log for topology change events
// in real time. If there's no topology event log in the cluster, a new empty
// one will be created.
func (ts *Server) WatchTopoEventLog(ctx context.Context) (*WatchTopoEventData, <-chan *WatchTopoEventData, CancelFunc) {
	current, wdChannel, cancel := ts.globalCell.Watch(ctx, TopoEventFile)
	if current.Err != nil {
		if IsErrType(current.Err, NoNode) {
			err := ts.createTopoEventLog(ctx, nil)
			if err == nil || IsErrType(err, NodeExists) {
				return ts.WatchTopoEventLog(ctx)
			}
			return &WatchTopoEventData{Err: err}, nil, nil
		}
		return &WatchTopoEventData{Err: current.Err}, nil, nil
	}
	value := &topodatapb.TopoEventLog{}
	if err := proto.Unmarshal(current.Contents, value); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return &WatchTopoEventData{Err: vterrors.Wrapf(err, "error unpacking initial TopoEvent object")}, nil, nil
	}

	changes := make(chan *WatchTopoEventData, 10)
	seen := make(map[string]struct{})

	for _, ev := range value.Log {
		seen[ev.Uuid] = struct{}{}
	}

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

			value := &topodatapb.TopoEventLog{}
			if err := proto.Unmarshal(wd.Contents, value); err != nil {
				cancel()
				for range wdChannel {
				}
				changes <- &WatchTopoEventData{Err: vterrors.Wrapf(err, "error unpacking TopoEvent object")}
				return
			}

			data := &WatchTopoEventData{
				FullLog: value.Log,
			}
			for _, ev := range value.Log {
				if _, ok := seen[ev.Uuid]; !ok {
					seen[ev.Uuid] = struct{}{}
					data.NewLogEntries = append(data.NewLogEntries, ev)
				}
			}

			changes <- data
		}
	}()

	return &WatchTopoEventData{NewLogEntries: value.Log, FullLog: value.Log}, changes, cancel
}

// UpdateTopoEventLog appends the given event to the topology event log.
// The log is always kept consistently sorted by StartTime for all events.
func (ts *Server) UpdateTopoEventLog(ctx context.Context, newEvent *topodatapb.TopoEvent) error {
	nodePath := TopoEventFile
	for {
		data, version, err := ts.globalCell.Get(ctx, nodePath)
		if err != nil {
			if IsErrType(err, NoNode) {
				err = ts.createTopoEventLog(ctx, []*topodatapb.TopoEvent{newEvent})
				if err != nil {
					if IsErrType(err, NodeExists) {
						continue
					}
					return err
				}
			}
			return vterrors.Wrapf(err, "failed to update TopoEventLog in place")
		}

		topoEvLog := &topodatapb.TopoEventLog{}
		if err := proto.Unmarshal(data, topoEvLog); err != nil {
			return vterrors.Wrapf(err, "TopoEventLog unmarshal failed: %v", data)
		}

		topoEvLog.Log = append(topoEvLog.Log, newEvent)
		sort.Slice(topoEvLog.Log, func(i, j int) bool {
			a := protoutil.TimeFromProto(topoEvLog.Log[i].StartedAt)
			b := protoutil.TimeFromProto(topoEvLog.Log[j].StartedAt)
			return a.Before(b)
		})

		updatedData, err := proto.Marshal(topoEvLog)
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
