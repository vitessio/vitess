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
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

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
	defer setVSchema("{}")

	// We have to start at least one stream to start the vschema watcher.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}

	_ = startStream(ctx, t, filter)
	cancel()

	startCount := framework.FetchInt(framework.DebugVars(), "VSchemaUpdates")
	if startCount == 0 {
		t.Errorf("startCount: %d, want non-zero", startCount)
	}

	if err := setVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}

	// Looks like memorytopo instantly transmits the watch.
	// No need to wait for vschema to propagate.
	endCount := framework.FetchInt(framework.DebugVars(), "VSchemaUpdates")
	if endCount != startCount+1 {
		t.Errorf("endCount: %d, want %d", endCount, startCount+1)
	}

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
	if got := framework.FetchURL("/debug/vschema"); got != want {
		t.Errorf("vschema:\n%s, want:\n%s", got, want)
	}
}
