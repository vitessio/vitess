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

package vreplication

import (
	"bytes"
	"html/template"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/proto/binlogdata"
)

var wantOut = `
VReplication state: Open</br>
<table>
  <tr>
    <th>Index</th>
    <th>Source</th>
    <th>Source Tablet</th>
    <th>State</th>
    <th>Stop Position</th>
    <th>Last Position</th>
    <th>Seconds Behind Master</th>
    <th>Counts</th>
    <th>Rates</th>
    <th>Last Message</th>
  </tr>
  <tr>
      <td>1</td>
      <td>keyspace:&#34;ks&#34; shard:&#34;0&#34; </td>
      <td>src1</td>
      <td>Running</td>
      <td>MariaDB/1-2-4</td>
      <td>1-2-3</td>
      <td>2</td>
      <td><b>All</b>: 0<br></td>
      <td></td>
      <td>Test Message2<br>Test Message1<br></td>
    </tr><tr>
      <td>2</td>
      <td>keyspace:&#34;ks&#34; shard:&#34;1&#34; </td>
      <td>src2</td>
      <td>Stopped</td>
      <td>MariaDB/1-2-5</td>
      <td>1-2-3</td>
      <td>2</td>
      <td><b>All</b>: 0<br></td>
      <td></td>
      <td>Test Message2<br>Test Message1<br></td>
    </tr>
</table>
`

func TestStatusHtml(t *testing.T) {
	pos, err := mysql.DecodePosition("MariaDB/1-2-3")
	if err != nil {
		t.Fatal(err)
	}

	blpStats := binlogplayer.NewStats()
	blpStats.SetLastPosition(pos)
	blpStats.SecondsBehindMaster.Set(2)
	blpStats.History.Add(&binlogplayer.StatsHistoryRecord{Time: time.Now(), Message: "Test Message1"})
	blpStats.History.Add(&binlogplayer.StatsHistoryRecord{Time: time.Now(), Message: "Test Message2"})

	testStats := &vrStats{}
	testStats.isOpen = true
	testStats.controllers = map[int]*controller{
		1: {
			id: 1,
			source: binlogdata.BinlogSource{
				Keyspace: "ks",
				Shard:    "0",
			},
			stopPos:  "MariaDB/1-2-4",
			blpStats: blpStats,
			done:     make(chan struct{}),
		},
		2: {
			id: 2,
			source: binlogdata.BinlogSource{
				Keyspace: "ks",
				Shard:    "1",
			},
			stopPos:  "MariaDB/1-2-5",
			blpStats: blpStats,
			done:     make(chan struct{}),
		},
	}
	testStats.controllers[1].sourceTablet.Set("src1")
	testStats.controllers[2].sourceTablet.Set("src2")
	close(testStats.controllers[2].done)

	tpl := template.Must(template.New("test").Parse(vreplicationTemplate))
	buf := bytes.NewBuffer(nil)
	tpl.Execute(buf, testStats.status())
	if strings.Contains(buf.String(), wantOut) {
		t.Errorf("output: %v, want %v", buf, wantOut)
	}
}

func TestVReplicationStats(t *testing.T) {
	blpStats := binlogplayer.NewStats()

	testStats := &vrStats{}
	testStats.isOpen = true
	testStats.controllers = map[int]*controller{
		1: {
			id: 1,
			source: binlogdata.BinlogSource{
				Keyspace: "ks",
				Shard:    "0",
			},
			blpStats: blpStats,
			done:     make(chan struct{}),
		},
	}
	testStats.controllers[1].sourceTablet.Set("src1")

	sleepTime := 1 * time.Millisecond
	record := func(phase string) {
		defer blpStats.PhaseTimings.Record(phase, time.Now())
		time.Sleep(sleepTime)
	}
	want := int64(1.2 * float64(sleepTime)) //allow 10% overhead for recording timing

	record("fastforward")
	require.Greater(t, want, testStats.status().Controllers[0].PhaseTimings["fastforward"])
	record("catchup")
	require.Greater(t, want, testStats.status().Controllers[0].PhaseTimings["catchup"])
	record("copy")
	require.Greater(t, want, testStats.status().Controllers[0].PhaseTimings["copy"])

	blpStats.QueryCount.Add("replicate", 11)
	blpStats.QueryCount.Add("fastforward", 23)
	require.Equal(t, int64(11), testStats.status().Controllers[0].QueryCounts["replicate"])
	require.Equal(t, int64(23), testStats.status().Controllers[0].QueryCounts["fastforward"])

	blpStats.CopyLoopCount.Add(100)
	blpStats.CopyRowCount.Add(200)
	require.Equal(t, int64(100), testStats.status().Controllers[0].CopyLoopCount)
	require.Equal(t, int64(200), testStats.status().Controllers[0].CopyRowCount)

	var tm int64 = 1234567890
	blpStats.RecordHeartbeat(tm)
	require.Equal(t, tm, blpStats.Heartbeat())
}
