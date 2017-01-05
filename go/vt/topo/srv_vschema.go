package topo

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// This file contains the utility methods to manage SrvVSchema objects.

// WatchSrvVSchemaData is returned / streamed by WatchSrvVSchema.
// The WatchSrvVSchema API guarantees exactly one of Value or Err will be set.
type WatchSrvVSchemaData struct {
	Value *vschemapb.SrvVSchema
	Err   error
}

// WatchSrvVSchema will set a watch on the SrvVSchema object.
// It has the same contract as Backend.Watch, but it also unpacks the
// contents into a SrvVSchema object.
func (ts Server) WatchSrvVSchema(ctx context.Context, cell string) (*WatchSrvVSchemaData, <-chan *WatchSrvVSchemaData, CancelFunc) {
	current, wdChannel, cancel := ts.Watch(ctx, cell, SrvVSchemaFile)
	if current.Err != nil {
		return &WatchSrvVSchemaData{Err: current.Err}, nil, nil
	}
	value := &vschemapb.SrvVSchema{}
	if err := proto.Unmarshal(current.Contents, value); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return &WatchSrvVSchemaData{Err: fmt.Errorf("error unpacking initial SrvVSchema object: %v", err)}, nil, nil
	}

	changes := make(chan *WatchSrvVSchemaData, 10)

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
				changes <- &WatchSrvVSchemaData{Err: wd.Err}
				return
			}

			value := &vschemapb.SrvVSchema{}
			if err := proto.Unmarshal(wd.Contents, value); err != nil {
				cancel()
				for range wdChannel {
				}
				changes <- &WatchSrvVSchemaData{Err: fmt.Errorf("error unpacking SrvVSchema object: %v", err)}
				return
			}
			changes <- &WatchSrvVSchemaData{Value: value}
		}
	}()

	return &WatchSrvVSchemaData{Value: value}, changes, cancel
}
