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
	"encoding/json"
	"testing"
	"time"

	"golang.org/x/net/context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var shardedVSchema = `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        }
      ]
    }
  }
}`

func TestUpdateVSchema(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	defer env.SetVSchema("{}")

	// We have to start at least one stream to start the vschema watcher.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}

	_ = startStream(ctx, t, filter, "")
	cancel()

	startCount := expectUpdateCount(t, 1)

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	expectUpdateCount(t, startCount+1)

	want := `{
  "sharded": true,
  "tables": {
    "t1": {
      "name": "t1",
      "column_vindexes": [
        {
          "columns": [
            "id1"
          ],
          "type": "hash",
          "name": "hash",
          "vindex": {}
        }
      ],
      "ordered": [
        {
          "columns": [
            "id1"
          ],
          "type": "hash",
          "name": "hash",
          "vindex": {}
        }
      ]
    }
  },
  "vindexes": {
    "hash": {}
  }
}`
	b, err := json.MarshalIndent(engine.vschema(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != want {
		t.Errorf("vschema:\n%s, want:\n%s", got, want)
	}
}

func expectUpdateCount(t *testing.T, wantCount int64) int64 {
	for i := 0; i < 10; i++ {
		gotCount := vschemaUpdates.Get()
		if gotCount >= wantCount {
			return gotCount
		}
		if i == 9 {
			t.Fatalf("update count: %d, want %d", gotCount, wantCount)
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("unreachable")
}
