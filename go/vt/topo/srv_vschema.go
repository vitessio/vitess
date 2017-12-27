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
// It has the same contract as Conn.Watch, but it also unpacks the
// contents into a SrvVSchema object.
func (ts *Server) WatchSrvVSchema(ctx context.Context, cell string) (*WatchSrvVSchemaData, <-chan *WatchSrvVSchemaData, CancelFunc) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return &WatchSrvVSchemaData{Err: err}, nil, nil
	}

	current, wdChannel, cancel := conn.Watch(ctx, SrvVSchemaFile)
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

// UpdateSrvVSchema updates the SrvVSchema file for a cell.
func (ts *Server) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := SrvVSchemaFile
	data, err := proto.Marshal(srvVSchema)
	if err != nil {
		return err
	}
	_, err = conn.Update(ctx, nodePath, data, nil)
	return err
}

// GetSrvVSchema returns the SrvVSchema for a cell.
func (ts *Server) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	nodePath := SrvVSchemaFile
	data, _, err := conn.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}
	srvVSchema := &vschemapb.SrvVSchema{}
	if err := proto.Unmarshal(data, srvVSchema); err != nil {
		return nil, fmt.Errorf("SrvVSchema unmarshal failed: %v %v", data, err)
	}
	return srvVSchema, nil
}

// DeleteSrvVSchema deletes the SrvVSchema file for a cell.
func (ts *Server) DeleteSrvVSchema(ctx context.Context, cell string) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := SrvVSchemaFile
	return conn.Delete(ctx, nodePath, nil)
}
