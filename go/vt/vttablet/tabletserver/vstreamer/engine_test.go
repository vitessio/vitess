package vstreamer

import (
	"encoding/json"
	"testing"
	"time"

	"context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	shardedVSchema = `{
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

	multicolumnVSchema = `{
  "sharded": true,
  "vindexes": {
    "region_vdx": {
      "type": "region_experimental",
			"params": {
				"region_bytes": "1"
			}
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "columns": [
						"region",
						"id"
					],
          "name": "region_vdx"
        }
      ]
    }
  }
}`
)

func TestUpdateVSchema(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	defer env.SetVSchema("{}")

	// We have to start at least one stream to start the vschema watcher.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	// Stream should terminate immediately due to canceled context.
	_ = engine.Stream(ctx, "current", nil, filter, func(_ []*binlogdatapb.VEvent) error {
		return nil
	})

	startCount := expectUpdateCount(t, 1)

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	expectUpdateCount(t, startCount+1)

	want := `{
  "routing_rules": {},
  "keyspaces": {
    "vttest": {
      "sharded": true,
      "tables": {
        "dual": {
          "type": "reference",
          "name": "dual"
        },
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
    }
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
		gotCount := engine.vschemaUpdates.Get()
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
