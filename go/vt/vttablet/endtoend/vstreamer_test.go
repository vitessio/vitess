/*
Copyright 2020 The Vitess Authors.

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

package endtoend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

type test struct {
	query  string
	output []string
}

// the schema version tests can get events related to the creation of the schema version table depending on
// whether the table already exists or not. To avoid different behaviour when tests are run together
// this function adds to events expected if table is not present
func getSchemaVersionTableCreationEvents() []string {
	tableCreationEvents := []string{"gtid", "other", "gtid", "other", "gtid", "other"}
	client := framework.NewClient()
	_, err := client.Execute("describe _vt.schema_version", nil)
	if err != nil {
		log.Errorf("_vt.schema_version not found, will expect its table creation events")
		return tableCreationEvents
	}
	return nil
}

func TestSchemaVersioning(t *testing.T) {
	// Let's disable the already running tracker to prevent it from
	// picking events from the previous test, and then re-enable it at the end.
	tsv := framework.Server
	tsv.EnableHistorian(false)
	tsv.SetTracking(false)
	tsv.EnableHeartbeat(false)
	tsv.EnableThrottler(false)
	defer tsv.EnableThrottler(true)
	defer tsv.EnableHeartbeat(true)
	defer tsv.EnableHistorian(true)
	defer tsv.SetTracking(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsv.EnableHistorian(true)
	tsv.SetTracking(true)

	target := &querypb.Target{
		Keyspace:   "vttest",
		Shard:      "0",
		TabletType: tabletpb.TabletType_MASTER,
		Cell:       "",
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}

	var cases = []test{
		{
			query: "create table vitess_version (\n\tid1 int,\n\tid2 int\n)",
			output: append(append([]string{
				`gtid`, //gtid+other => vstream current pos
				`other`,
				`gtid`, //gtid+ddl => actual query
				`type:DDL statement:"create table vitess_version (\n\tid1 int,\n\tid2 int\n)" `},
				getSchemaVersionTableCreationEvents()...),
				`version`,
				`gtid`,
			),
		},
		{
			query: "insert into vitess_version values(1, 10)",
			output: []string{
				`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > > `,
				`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 values:"110" > > > `,
				`gtid`,
			},
		}, {
			query: "alter table vitess_version add column id3 int",
			output: []string{
				`gtid`,
				`type:DDL statement:"alter table vitess_version add column id3 int" `,
				`version`,
				`gtid`,
			},
		}, {
			query: "insert into vitess_version values(2, 20, 200)",
			output: []string{
				`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:11 charset:63 > > `,
				`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 values:"220200" > > > `,
				`gtid`,
			},
		}, {
			query: "alter table vitess_version modify column id3 varbinary(16)",
			output: []string{
				`gtid`,
				`type:DDL statement:"alter table vitess_version modify column id3 varbinary(16)" `,
				`version`,
				`gtid`,
			},
		}, {
			query: "insert into vitess_version values(3, 30, 'TTT')",
			output: []string{
				`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:16 charset:63 > > `,
				`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 values:"330TTT" > > > `,
				`gtid`,
			},
		},
	}
	eventCh := make(chan []*binlogdatapb.VEvent)
	var startPos string
	send := func(events []*binlogdatapb.VEvent) error {
		var evs []*binlogdatapb.VEvent
		for _, event := range events {
			if event.Type == binlogdatapb.VEventType_GTID {
				if startPos == "" {
					startPos = event.Gtid
				}
			}
			if event.Type == binlogdatapb.VEventType_HEARTBEAT {
				continue
			}
			log.Infof("Received event %v", event)
			evs = append(evs, event)
		}
		select {
		case eventCh <- evs:
		case <-ctx.Done():
			return nil
		}
		return nil
	}
	go func() {
		defer close(eventCh)
		if err := tsv.VStream(ctx, target, "current", nil, filter, send); err != nil {
			fmt.Printf("Error in tsv.VStream: %v", err)
			t.Error(err)
		}
	}()
	log.Infof("\n\n\n=============================================== CURRENT EVENTS START HERE ======================\n\n\n")
	runCases(ctx, t, cases, eventCh)

	tsv.SetTracking(false)
	cases = []test{
		{
			//comment prefix required so we don't look for ddl in schema_version
			query: "/**/alter table vitess_version add column id4 varbinary(16)",
			output: []string{
				`gtid`, //no tracker, so no insert into schema_version or version event
				`type:DDL statement:"/**/alter table vitess_version add column id4 varbinary(16)" `,
			},
		}, {
			query: "insert into vitess_version values(4, 40, 'FFF', 'GGGG' )",
			output: []string{
				`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:16 charset:63 > fields:<name:"id4" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id4" column_length:16 charset:63 > > `,
				`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 lengths:4 values:"440FFFGGGG" > > > `,
				`gtid`,
			},
		},
	}
	runCases(ctx, t, cases, eventCh)
	cancel()

	log.Infof("\n\n\n=============================================== PAST EVENTS WITH TRACK VERSIONS START HERE ======================\n\n\n")
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	eventCh = make(chan []*binlogdatapb.VEvent)
	send = func(events []*binlogdatapb.VEvent) error {
		var evs []*binlogdatapb.VEvent
		for _, event := range events {
			if event.Type == binlogdatapb.VEventType_HEARTBEAT {
				continue
			}
			log.Infof("Received event %v", event)
			evs = append(evs, event)
		}
		select {
		case eventCh <- evs:
		case <-ctx.Done():
			t.Fatal("Context Done() in send")
		}
		return nil
	}
	go func() {
		defer close(eventCh)
		if err := tsv.VStream(ctx, target, startPos, nil, filter, send); err != nil {
			fmt.Printf("Error in tsv.VStream: %v", err)
			t.Error(err)
		}
	}()

	// playing events from the past: same events as original since historian is providing the latest schema
	output := append(append([]string{
		`gtid`,
		`type:DDL statement:"create table vitess_version (\n\tid1 int,\n\tid2 int\n)" `},
		getSchemaVersionTableCreationEvents()...),
		`version`,
		`gtid`,
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 values:"110" > > > `,
		`gtid`,
		`gtid`,
		`type:DDL statement:"alter table vitess_version add column id3 int" `,
		`version`,
		`gtid`,
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:11 charset:63 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 values:"220200" > > > `,
		`gtid`,
		`gtid`,
		`type:DDL statement:"alter table vitess_version modify column id3 varbinary(16)" `,
		`version`,
		`gtid`,
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:16 charset:63 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 values:"330TTT" > > > `,
		`gtid`,
		`gtid`,
		`type:DDL statement:"/**/alter table vitess_version add column id4 varbinary(16)" `,
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:16 charset:63 > fields:<name:"id4" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id4" column_length:16 charset:63 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 lengths:4 values:"440FFFGGGG" > > > `,
		`gtid`,
	)

	expectLogs(ctx, t, "Past stream", eventCh, output)

	cancel()

	log.Infof("\n\n\n=============================================== PAST EVENTS WITHOUT TRACK VERSIONS START HERE ======================\n\n\n")
	tsv.EnableHistorian(false)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	eventCh = make(chan []*binlogdatapb.VEvent)
	send = func(events []*binlogdatapb.VEvent) error {
		var evs []*binlogdatapb.VEvent
		for _, event := range events {
			if event.Type == binlogdatapb.VEventType_HEARTBEAT {
				continue
			}
			log.Infof("Received event %v", event)
			evs = append(evs, event)
		}
		select {
		case eventCh <- evs:
		case <-ctx.Done():
			t.Fatal("Context Done() in send")
		}
		return nil
	}
	go func() {
		defer close(eventCh)
		if err := tsv.VStream(ctx, target, startPos, nil, filter, send); err != nil {
			fmt.Printf("Error in tsv.VStream: %v", err)
			t.Error(err)
		}
	}()

	// playing events from the past: same as earlier except one below, see comments
	output = append(append([]string{
		`gtid`,
		`type:DDL statement:"create table vitess_version (\n\tid1 int,\n\tid2 int\n)" `},
		getSchemaVersionTableCreationEvents()...),
		`version`,
		`gtid`,
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 values:"110" > > > `,
		`gtid`,
		`gtid`,
		`type:DDL statement:"alter table vitess_version add column id3 int" `,
		`version`,
		`gtid`,
		/*at this point we only have latest schema so we have types (int32, int32, varbinary, varbinary) so the types don't match. Hence the @ fieldnames*/
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"@1" type:INT32 > fields:<name:"@2" type:INT32 > fields:<name:"@3" type:INT32 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 values:"220200" > > > `,
		`gtid`,
		`gtid`,
		`type:DDL statement:"alter table vitess_version modify column id3 varbinary(16)" `,
		`version`,
		`gtid`,
		/*at this point we only have latest schema so we have types (int32, int32, varbinary, varbinary),
		  but the three fields below match the first three types in the latest, so the field names are correct*/
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:16 charset:63 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 values:"330TTT" > > > `,
		`gtid`,
		`gtid`,
		`type:DDL statement:"/**/alter table vitess_version add column id4 varbinary(16)" `,
		`type:FIELD field_event:<table_name:"vitess_version" fields:<name:"id1" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id1" column_length:11 charset:63 > fields:<name:"id2" type:INT32 table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id2" column_length:11 charset:63 > fields:<name:"id3" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id3" column_length:16 charset:63 > fields:<name:"id4" type:VARBINARY table:"vitess_version" org_table:"vitess_version" database:"vttest" org_name:"id4" column_length:16 charset:63 > > `,
		`type:ROW row_event:<table_name:"vitess_version" row_changes:<after:<lengths:1 lengths:2 lengths:3 lengths:4 values:"440FFFGGGG" > > > `,
		`gtid`,
	)

	expectLogs(ctx, t, "Past stream", eventCh, output)
	cancel()

	client := framework.NewClient()
	client.Execute("drop table vitess_version", nil)
	client.Execute("drop table _vt.schema_version", nil)

	log.Info("=== END OF TEST")
}

func runCases(ctx context.Context, t *testing.T, tests []test, eventCh chan []*binlogdatapb.VEvent) {
	t.Helper()
	client := framework.NewClient()

	for _, test := range tests {
		query := test.query
		client.Execute(query, nil)
		if len(test.output) > 0 {
			expectLogs(ctx, t, query, eventCh, test.output)
		}
		if strings.HasPrefix(query, "create") || strings.HasPrefix(query, "alter") || strings.HasPrefix(query, "drop") {
			ok, err := waitForVersionInsert(client, query)
			if err != nil || !ok {
				t.Fatalf("Query %s never got inserted into the schema_version table", query)
			}
			framework.Server.SchemaEngine().Reload(ctx)

		}
	}
}

func expectLogs(ctx context.Context, t *testing.T, query string, eventCh chan []*binlogdatapb.VEvent, output []string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var evs []*binlogdatapb.VEvent
	log.Infof("In expectLogs for query %s, output len %d", query, len(output))
	for {
		select {
		case allevs, ok := <-eventCh:
			if !ok {
				t.Fatal("expectLogs: not ok, stream ended early")
			}
			for _, ev := range allevs {
				// Ignore spurious heartbeats that can happen on slow machines.
				if ev.Type == binlogdatapb.VEventType_HEARTBEAT {
					continue
				}
				// Also ignore begin/commit to reduce list of events to expect, for readability ...
				if ev.Type == binlogdatapb.VEventType_BEGIN {
					continue
				}
				if ev.Type == binlogdatapb.VEventType_COMMIT {
					continue
				}

				evs = append(evs, ev)
			}
			log.Infof("In expectLogs, have got %d events, want %d", len(evs), len(output))
		case <-ctx.Done():
			t.Fatalf("expectLog: Done(), stream ended early")
		case <-timer.C:
			t.Fatalf("expectLog: timed out waiting for events: %v: evs\n%v, want\n%v, >> got length %d, wanted length %d", query, evs, output, len(evs), len(output))
		}
		if len(evs) >= len(output) {
			break
		}
	}
	if len(evs) > len(output) {
		t.Fatalf("expectLog: got too many events: %v: evs\n%v, want\n%v, >> got length %d, wanted length %d", query, evs, output, len(evs), len(output))
	}
	for i, want := range output {
		// CurrentTime is not testable.
		evs[i].CurrentTime = 0
		switch want {
		case "begin":
			if evs[i].Type != binlogdatapb.VEventType_BEGIN {
				t.Fatalf("%v (%d): event: %v, want begin", query, i, evs[i])
			}
		case "gtid":
			if evs[i].Type != binlogdatapb.VEventType_GTID {
				t.Fatalf("%v (%d): event: %v, want gtid", query, i, evs[i])
			}
		case "commit":
			if evs[i].Type != binlogdatapb.VEventType_COMMIT {
				t.Fatalf("%v (%d): event: %v, want commit", query, i, evs[i])
			}
		case "other":
			if evs[i].Type != binlogdatapb.VEventType_OTHER {
				t.Fatalf("%v (%d): event: %v, want other", query, i, evs[i])
			}
		case "version":
			if evs[i].Type != binlogdatapb.VEventType_VERSION {
				t.Fatalf("%v (%d): event: %v, want version", query, i, evs[i])
			}
		default:
			evs[i].Timestamp = 0
			if evs[i].Type == binlogdatapb.VEventType_FIELD {
				for j := range evs[i].FieldEvent.Fields {
					evs[i].FieldEvent.Fields[j].Flags = 0
				}
			}
			if got := fmt.Sprintf("%v", evs[i]); got != want {
				t.Fatalf("%v (%d): event:\n%q, want\n%q", query, i, got, want)
			}
		}
	}
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

func validateSchemaInserted(client *framework.QueryClient, ddl string) (bool, error) {
	qr, _ := client.Execute(fmt.Sprintf("select * from _vt.schema_version where ddl = %s", encodeString(ddl)), nil)
	if len(qr.Rows) == 1 {
		log.Infof("Found ddl in schema_version: %s", ddl)
		return true, nil
	}
	return false, fmt.Errorf("Found %d rows for gtid %s", len(qr.Rows), ddl)
}

// To avoid races between ddls and the historian refreshing its cache explicitly wait for tracker's insert to be visible
func waitForVersionInsert(client *framework.QueryClient, ddl string) (bool, error) {
	timeout := time.After(1000 * time.Millisecond)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return false, errors.New("waitForVersionInsert timed out")
		case <-tick:
			ok, err := validateSchemaInserted(client, ddl)
			if err != nil {
				return false, err
			} else if ok {
				log.Infof("Found version insert for %s", ddl)
				return true, nil
			}
		}
	}
}
