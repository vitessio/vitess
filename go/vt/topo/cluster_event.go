package topo

import (
	"context"
	"sort"

	"google.golang.org/protobuf/proto"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"
)

type WatchTopoEventData struct {
	NewLogEntries []*topodatapb.TopoEvent
	Err           error
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

			data := &WatchTopoEventData{}
			for _, ev := range value.Log {
				if _, ok := seen[ev.Uuid]; !ok {
					seen[ev.Uuid] = struct{}{}
					data.NewLogEntries = append(data.NewLogEntries, ev)
				}
			}

			changes <- data
		}
	}()

	return &WatchTopoEventData{NewLogEntries: value.Log}, changes, cancel
}

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
			a := topoEvLog.Log[i].StartedAt
			b := topoEvLog.Log[j].StartedAt
			return a.Seconds < b.Seconds && a.Nanoseconds < b.Nanoseconds
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
