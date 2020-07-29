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

package vstreamer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestStreamResults(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	oldPacketSize := *PacketSize
	defer func() {
		*PacketSize = oldPacketSize
	}()
	*PacketSize = 1
	engine.resultStreamerNumPackets.Reset()
	engine.resultStreamerNumRows.Reset()

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, 'aaa'), (2, 'bbb')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	query := "select id, val from t1 order by id"
	wantStream := []string{
		`rows:<lengths:1 lengths:3 values:"1aaa" > `,
		`rows:<lengths:1 lengths:3 values:"2bbb" > `,
	}
	i := 0
	ch := make(chan error)
	// We don't want to report errors inside callback functions because
	// line numbers come out wrong.
	go func() {
		first := true
		defer close(ch)
		err := engine.StreamResults(context.Background(), query, func(rows *binlogdatapb.VStreamResultsResponse) error {
			if first {
				first = false
				if rows.Gtid == "" {
					ch <- fmt.Errorf("stream gtid is empty")
				}
				return nil
			}
			if i >= len(wantStream) {
				ch <- fmt.Errorf("unexpected stream rows: %v", rows)
				return nil
			}
			srows := fmt.Sprintf("%v", rows)
			if srows != wantStream[i] {
				ch <- fmt.Errorf("stream %d:\n%s, want\n%s", i, srows, wantStream[i])
			}
			i++
			return nil
		})
		if err != nil {
			ch <- err
		}
	}()
	for err := range ch {
		t.Error(err)
	}
	require.Equal(t, int64(2), engine.resultStreamerNumPackets.Get())
	require.Equal(t, int64(2), engine.resultStreamerNumRows.Get())
}
