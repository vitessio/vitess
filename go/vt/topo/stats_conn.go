/*
Copyright 2019 The Vitess Authors.

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

package topo

import (
	"context"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Conn = (*StatsConn)(nil)

var (
	topoStatsConnTimings = stats.NewMultiTimings(
		"TopologyConnOperations",
		"TopologyConnOperations timings",
		[]string{"Operation", "Cell"})

	topoStatsConnErrors = stats.NewCountersWithMultiLabels(
		"TopologyConnErrors",
		"TopologyConnErrors errors per operation",
		[]string{"Operation", "Cell"})
)

const readOnlyErrorStrFormat = "cannot perform %s on %s as the topology server connection is read-only"

// The StatsConn is a wrapper for a Conn that emits stats for every operation
type StatsConn struct {
	cell     string
	conn     Conn
	readOnly bool
}

// NewStatsConn returns a StatsConn
func NewStatsConn(cell string, conn Conn) *StatsConn {
	return &StatsConn{
		cell:     cell,
		conn:     conn,
		readOnly: false,
	}
}

// ListDir is part of the Conn interface
func (st *StatsConn) ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error) {
	startTime := time.Now()
	statsKey := []string{"ListDir", st.cell}
	defer topoStatsConnTimings.Record(statsKey, startTime)
	res, err := st.conn.ListDir(ctx, dirPath, full)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return res, err
	}
	return res, err
}

// Create is part of the Conn interface
func (st *StatsConn) Create(ctx context.Context, filePath string, contents []byte) (Version, error) {
	statsKey := []string{"Create", st.cell}
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, readOnlyErrorStrFormat, statsKey[0], filePath)
	}
	startTime := time.Now()
	defer topoStatsConnTimings.Record(statsKey, startTime)
	res, err := st.conn.Create(ctx, filePath, contents)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return res, err
	}
	return res, err
}

// Update is part of the Conn interface
func (st *StatsConn) Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error) {
	statsKey := []string{"Update", st.cell}
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, readOnlyErrorStrFormat, statsKey[0], filePath)
	}
	startTime := time.Now()
	defer topoStatsConnTimings.Record(statsKey, startTime)
	res, err := st.conn.Update(ctx, filePath, contents, version)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return res, err
	}
	return res, err
}

// Get is part of the Conn interface
func (st *StatsConn) Get(ctx context.Context, filePath string) ([]byte, Version, error) {
	startTime := time.Now()
	statsKey := []string{"Get", st.cell}
	defer topoStatsConnTimings.Record(statsKey, startTime)
	bytes, version, err := st.conn.Get(ctx, filePath)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return bytes, version, err
	}
	return bytes, version, err
}

// List is part of the Conn interface
func (st *StatsConn) List(ctx context.Context, filePathPrefix string) ([]KVInfo, error) {
	startTime := time.Now()
	statsKey := []string{"List", st.cell}
	defer topoStatsConnTimings.Record(statsKey, startTime)
	bytes, err := st.conn.List(ctx, filePathPrefix)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return bytes, err
	}
	return bytes, err
}

// Delete is part of the Conn interface
func (st *StatsConn) Delete(ctx context.Context, filePath string, version Version) error {
	statsKey := []string{"Delete", st.cell}
	if st.readOnly {
		return vterrors.Errorf(vtrpc.Code_READ_ONLY, readOnlyErrorStrFormat, statsKey[0], filePath)
	}
	startTime := time.Now()
	defer topoStatsConnTimings.Record(statsKey, startTime)
	err := st.conn.Delete(ctx, filePath, version)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return err
	}
	return err
}

// Lock is part of the Conn interface
func (st *StatsConn) Lock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	statsKey := []string{"Lock", st.cell}
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, readOnlyErrorStrFormat, statsKey[0], dirPath)
	}
	startTime := time.Now()
	defer topoStatsConnTimings.Record(statsKey, startTime)
	res, err := st.conn.Lock(ctx, dirPath, contents)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return res, err
	}
	return res, err
}

// Watch is part of the Conn interface
func (st *StatsConn) Watch(ctx context.Context, filePath string) (current *WatchData, changes <-chan *WatchData, err error) {
	startTime := time.Now()
	statsKey := []string{"Watch", st.cell}
	defer topoStatsConnTimings.Record(statsKey, startTime)
	return st.conn.Watch(ctx, filePath)
}

func (st *StatsConn) WatchRecursive(ctx context.Context, path string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
	startTime := time.Now()
	statsKey := []string{"WatchRecursive", st.cell}
	defer topoStatsConnTimings.Record(statsKey, startTime)
	return st.conn.WatchRecursive(ctx, path)
}

// NewLeaderParticipation is part of the Conn interface
func (st *StatsConn) NewLeaderParticipation(name, id string) (LeaderParticipation, error) {
	startTime := time.Now()
	statsKey := []string{"NewLeaderParticipation", st.cell}
	defer topoStatsConnTimings.Record(statsKey, startTime)
	res, err := st.conn.NewLeaderParticipation(name, id)
	if err != nil {
		topoStatsConnErrors.Add(statsKey, int64(1))
		return res, err
	}
	return res, err
}

// Close is part of the Conn interface
func (st *StatsConn) Close() {
	startTime := time.Now()
	statsKey := []string{"Close", st.cell}
	defer topoStatsConnTimings.Record(statsKey, startTime)
	st.conn.Close()
}

// SetReadOnly with true prevents any write operations from being made on the topo connection
func (st *StatsConn) SetReadOnly(readOnly bool) {
	st.readOnly = readOnly
}

// IsReadOnly allows you to check the access type for the topo connection
func (st *StatsConn) IsReadOnly() bool {
	return st.readOnly
}
