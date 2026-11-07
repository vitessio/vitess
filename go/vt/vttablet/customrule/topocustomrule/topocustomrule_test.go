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

package topocustomrule

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
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
			require.Fail(t, "timeout: value in topo was not propagated in time")
		}
		t.Logf("sleeping for 10ms waiting for value %v (current=%v)", expected, val)
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUpdate(t *testing.T) {
	custom1 := rules.New()
	if err := custom1.UnmarshalJSON([]byte(customRule1)); err != nil {
		require.NoError(t, err)
	}
	custom2 := rules.New()
	if err := custom2.UnmarshalJSON([]byte(customRule2)); err != nil {
		require.NoError(t, err)
	}

	cell := "cell1"
	filePath := "/keyspaces/ks1/configs/CustomRules"
	ctx := t.Context()

	ts := memorytopo.NewServer(ctx, cell)
	qsc := tabletservermock.NewController()
	qsc.TS = ts
	sleepDuringTopoFailure = time.Millisecond

	logs := logtest.Capture(t, slog.LevelInfo)
	cr, err := newTopoCustomRule(qsc, cell, filePath)
	require.NoError(t, err)
	cr.start()
	defer cr.stop()

	// The file doesn't exist yet, so the watch fails and is retried.
	record := logs.Wait(t, "Background watch of topo custom rule failed")
	assert.Equal(t, filePath, record["path"])
	assert.Equal(t, "node doesn't exist: "+filePath, record["error"])
	assert.Equal(t, "UNKNOWN", record["error_code"])
	record = logs.Wait(t, "Sleeping before trying again")
	assert.Equal(t, filePath, record["path"])
	assert.EqualValues(t, time.Millisecond, record["retry_delay"])

	// Set a value, wait until we get it.
	conn, err := ts.ConnForCell(ctx, cell)
	require.NoError(t, err)
	version1, err := conn.Create(ctx, filePath, []byte(customRule1))
	require.NoError(t, err)
	waitForValue(t, qsc, custom1)

	// update the value, wait until we get it.
	version2, err := conn.Update(ctx, filePath, []byte(customRule2), nil)
	require.NoError(t, err)
	waitForValue(t, qsc, custom2)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var applied []map[string]any
		for _, record := range logs.Records(c) {
			if record["msg"] == "Custom rule version fetched from topo and applied to vttablet" {
				applied = append(applied, record)
			}
		}
		if !assert.Len(c, applied, 2) {
			return
		}
		assert.Equal(c, filePath, applied[0]["path"])
		assert.Equal(c, version1.String(), applied[0]["topo_version"])
		assert.Equal(c, filePath, applied[1]["path"])
		assert.Equal(c, version2.String(), applied[1]["topo_version"])
	}, 10*time.Second, 10*time.Millisecond)
}
