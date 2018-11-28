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
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/stats"
)

var _ Conn = (*StatsConn)(nil)

// The StatsConn is a wrapper for a Conn that emits stats for every operation
type StatsConn struct {
	cell string
	conn Conn

	timings *stats.MultiTimings
	counts  *stats.CountersWithMultiLabels
	errors  *stats.CountersWithMultiLabels
}

// NewStatsConn returns a StatsConn
func NewStatsConn(cell string, conn Conn) *StatsConn {
	return &StatsConn{
		cell: cell,
		conn: conn,
		timings: stats.NewMultiTimings(
			"TopologyConn",
			"TopologyConn timings",
			[]string{"Operation", "Cell"}),
		counts: stats.NewCountersWithMultiLabels(
			"TopologyConnCounts",
			"Operation counts to topology cells",
			[]string{"Operation", "Cell"}),
		errors: stats.NewCountersWithMultiLabels(
			"TopologyConnErrors",
			"Operation errors to topology cells",
			[]string{"Operation", "Cell"}),
	}
}

// ListDir is part of the Conn interface
func (st *StatsConn) ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error) {
	startTime := time.Now()
	statsKey := []string{"ListDir", st.cell}
	defer st.timings.Record(statsKey, startTime)
	res, err := st.ListDir(ctx, dirPath, full)
	if err != nil {
		st.errors.Add(statsKey, int64(1))
		return res, err
	}
	st.counts.Add(statsKey, int64(1))
	return res, err
}

// Create is part of the Conn interface
func (st *StatsConn) Create(ctx context.Context, filePath string, contents []byte) (Version, error) {
	startTime := time.Now()
	statsKey := []string{"Create", st.cell}
	defer st.timings.Record(statsKey, startTime)
	res, err := st.Create(ctx, filePath, contents)
	if err != nil {
		st.errors.Add(statsKey, int64(1))
		return res, err
	}
	st.counts.Add(statsKey, int64(1))
	return res, err
}

// Update is part of the Conn interface
func (st *StatsConn) Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error) {
	startTime := time.Now()
	statsKey := []string{"Update", st.cell}
	defer st.timings.Record(statsKey, startTime)
	res, err := st.Update(ctx, filePath, contents, version)
	if err != nil {
		st.errors.Add(statsKey, int64(1))
		return res, err
	}
	st.counts.Add(statsKey, int64(1))
	return res, err
}

// Get is part of the Conn interface
func (st *StatsConn) Get(ctx context.Context, filePath string) ([]byte, Version, error) {
	startTime := time.Now()
	statsKey := []string{"Get", st.cell}
	defer st.timings.Record(statsKey, startTime)
	bytes, version, err := st.Get(ctx, filePath)
	if err != nil {
		st.errors.Add(statsKey, int64(1))
		return bytes, version, err
	}
	st.counts.Add(statsKey, int64(1))
	return bytes, version, err
}

// Delete is part of the Conn interface
func (st *StatsConn) Delete(ctx context.Context, filePath string, version Version) error {
	startTime := time.Now()
	statsKey := []string{"Delete", st.cell}
	defer st.timings.Record(statsKey, startTime)
	err := st.Delete(ctx, filePath, version)
	if err != nil {
		st.errors.Add(statsKey, int64(1))
		return err
	}
	st.counts.Add(statsKey, int64(1))
	return err
}

// Lock is part of the Conn interface
func (st *StatsConn) Lock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	startTime := time.Now()
	statsKey := []string{"Lock", st.cell}
	defer st.timings.Record(statsKey, startTime)
	res, err := st.Lock(ctx, dirPath, contents)
	if err != nil {
		st.errors.Add(statsKey, int64(1))
		return res, err
	}
	st.counts.Add(statsKey, int64(1))
	return res, err
}

// Watch is part of the Conn interface
func (st *StatsConn) Watch(ctx context.Context, filePath string) (current *WatchData, changes <-chan *WatchData, cancel CancelFunc) {
	startTime := time.Now()
	statsKey := []string{"Watch", st.cell}
	defer st.timings.Record(statsKey, startTime)
	st.counts.Add(statsKey, int64(1))
	return st.Watch(ctx, filePath)
}

// NewMasterParticipation is part of the Conn interface
func (st *StatsConn) NewMasterParticipation(name, id string) (MasterParticipation, error) {
	startTime := time.Now()
	statsKey := []string{"NewMasterParticipation", st.cell}
	defer st.timings.Record(statsKey, startTime)
	res, err := st.NewMasterParticipation(name, id)
	if err != nil {
		st.errors.Add(statsKey, int64(1))
		return res, err
	}
	st.counts.Add(statsKey, int64(1))
	return res, err
}

// Close is part of the Conn interface
func (st *StatsConn) Close() {
	startTime := time.Now()
	statsKey := []string{"Close", st.cell}
	defer st.timings.Record(statsKey, startTime)
	st.counts.Add(statsKey, int64(1))
	st.Close()
}
