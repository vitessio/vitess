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
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the utility methods to manage SrvKeyspace objects.

func srvKeyspaceFileName(keyspace string) string {
	return path.Join(KeyspacesPath, keyspace, SrvKeyspaceFile)
}

// WatchSrvKeyspaceData is returned / streamed by WatchSrvKeyspace.
// The WatchSrvKeyspace API guarantees exactly one of Value or Err will be set.
type WatchSrvKeyspaceData struct {
	Value *topodatapb.SrvKeyspace
	Err   error
}

// WatchSrvKeyspace will set a watch on the SrvKeyspace object.
// It has the same contract as Conn.Watch, but it also unpacks the
// contents into a SrvKeyspace object.
func (ts *Server) WatchSrvKeyspace(ctx context.Context, cell, keyspace string) (*WatchSrvKeyspaceData, <-chan *WatchSrvKeyspaceData, CancelFunc) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return &WatchSrvKeyspaceData{Err: err}, nil, nil
	}

	filePath := srvKeyspaceFileName(keyspace)
	current, wdChannel, cancel := conn.Watch(ctx, filePath)
	if current.Err != nil {
		return &WatchSrvKeyspaceData{Err: current.Err}, nil, nil
	}
	value := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(current.Contents, value); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return &WatchSrvKeyspaceData{Err: vterrors.Wrapf(err, "error unpacking initial SrvKeyspace object")}, nil, nil
	}

	changes := make(chan *WatchSrvKeyspaceData, 10)

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
				changes <- &WatchSrvKeyspaceData{Err: wd.Err}
				return
			}

			value := &topodatapb.SrvKeyspace{}
			if err := proto.Unmarshal(wd.Contents, value); err != nil {
				cancel()
				for range wdChannel {
				}
				changes <- &WatchSrvKeyspaceData{Err: vterrors.Wrapf(err, "error unpacking SrvKeyspace object")}
				return
			}

			changes <- &WatchSrvKeyspaceData{Value: value}
		}
	}()

	return &WatchSrvKeyspaceData{Value: value}, changes, cancel
}

// GetSrvKeyspaceNames returns the SrvKeyspace objects for a cell.
func (ts *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	children, err := conn.ListDir(ctx, KeyspacesPath, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return nil, nil
	default:
		return nil, err
	}
}

// UpdateSrvKeyspace saves a new SrvKeyspace. It is a blind write.
func (ts *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	data, err := proto.Marshal(srvKeyspace)
	if err != nil {
		return err
	}
	_, err = conn.Update(ctx, nodePath, data, nil)
	return err
}

// DeleteSrvKeyspace deletes a SrvKeyspace.
func (ts *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	return conn.Delete(ctx, nodePath, nil)
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
func (ts *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	data, _, err := conn.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(data, srvKeyspace); err != nil {
		return nil, vterrors.Wrapf(err, "SrvKeyspace unmarshal failed: %v", data)
	}
	return srvKeyspace, nil
}
