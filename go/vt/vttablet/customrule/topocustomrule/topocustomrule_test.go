package topocustomrule

import (
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

var customRule1 = `
[
  {
    "Name": "r1",
    "Description": "disallow bindvar 'asdfg'",
    "BindVarConds":[{
      "Name": "asdfg",
      "OnAbsent": false,
      "Operator": ""
    }]
  }
]`

var customRule2 = `
[
  {
    "Name": "r2",
    "Description": "disallow insert on table test",
    "TableNames" : ["test"],
    "Query" : "(insert)|(INSERT)"
  }
]`

func waitForValue(t *testing.T, qsc *tabletservermock.Controller, expected *rules.Rules) {
	start := time.Now()
	for {
		val := qsc.GetQueryRules(topoCustomRuleSource)
		if val != nil {
			if val.Equal(expected) {
				return
			}
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("timeout: value in topo was not propagated in time")
		}
		t.Logf("sleeping for 10ms waiting for value %v (current=%v)", expected, val)
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUpdate(t *testing.T) {
	custom1 := rules.New()
	if err := custom1.UnmarshalJSON([]byte(customRule1)); err != nil {
		t.Fatalf("error unmarshaling customRule1: %v", err)
	}
	custom2 := rules.New()
	if err := custom2.UnmarshalJSON([]byte(customRule2)); err != nil {
		t.Fatalf("error unmarshaling customRule2: %v", err)
	}

	cell := "cell1"
	filePath := "/keyspaces/ks1/configs/CustomRules"
	ts := memorytopo.NewServer(cell)
	qsc := tabletservermock.NewController()
	qsc.TS = ts
	sleepDuringTopoFailure = time.Millisecond
	ctx := context.Background()

	cr, err := newTopoCustomRule(qsc, cell, filePath)
	if err != nil {
		t.Fatalf("newTopoCustomRule failed: %v", err)
	}
	cr.start()
	defer cr.stop()

	// Set a value, wait until we get it.
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		t.Fatalf("ConnForCell failed: %v", err)
	}
	if _, err := conn.Create(ctx, filePath, []byte(customRule1)); err != nil {
		t.Fatalf("conn.Create failed: %v", err)
	}
	waitForValue(t, qsc, custom1)

	// update the value, wait until we get it.
	if _, err := conn.Update(ctx, filePath, []byte(customRule2), nil); err != nil {
		t.Fatalf("conn.Update failed: %v", err)
	}
	waitForValue(t, qsc, custom2)
}
