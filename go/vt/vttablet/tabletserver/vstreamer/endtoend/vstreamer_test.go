/*
Copyright 2018 The Vitess Authors.

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
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestVStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := startStream(ctx, t, &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	})

	testcases := []struct {
		input  interface{}
		output [][]string
	}{{
		input: []string{
			"insert into stream1 values (1, 'aaa')",
			"update stream1 set val='bbb' where id = 1",
		},
		output: [][]string{{
			`gtid`,
			`begin`,
			`type:ROW row_event:<table_name:"stream1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"stream1" row_changes:<before:<lengths:1 lengths:3 values:"1aaa" > after:<lengths:1 lengths:3 values:"1bbb" > > > `,
			`commit`,
		}},
	}}

	client := framework.NewClient()

	for _, tcase := range testcases {
		switch input := tcase.input.(type) {
		case []string:
			execTransaction(t, client, input)
		default:
			t.Fatalf("unexpected input: %#v", input)
		}
		for _, wantset := range tcase.output {
			evs := <-ch
			if len(wantset) != len(evs) {
				t.Fatalf("evs\n%v, want\n%v", evs, wantset)
			}
			for i, want := range wantset {
				switch want {
				case "gtid":
					if evs[i].Type != binlogdatapb.VEventType_GTID {
						t.Fatalf("event: %v, want gtid", evs[i])
					}
				case "begin":
					if evs[i].Type != binlogdatapb.VEventType_BEGIN {
						t.Fatalf("event: %v, want begin", evs[i])
					}
				case "commit":
					if evs[i].Type != binlogdatapb.VEventType_COMMIT {
						t.Fatalf("event: %v, want commit", evs[i])
					}
				default:
					if got := fmt.Sprintf("%v", evs[i]); got != want {
						t.Fatalf("event:\n%q, want\n%q", got, want)
					}
				}
			}
		}
	}

	//execTransaction(t, client, []string{
	//	"insert into stream1 values (1, 'aaa')",
	//	"update stream1 set val='bbb' where id = 1",
	//})
	//execStatement(t, client, "/* c1 */ alter table stream1 change column val val varbinary(128)")
	//execTransaction(t, client, []string{
	//	"set timestamp=1",
	//	"insert into stream1 values (2, 'bbb')",
	//	"insert into stream2 values (1, 'aaa')",
	//	"update stream1 set val='ccc'",
	//})
	//if err := framework.Mysqld.ExecuteSuperQueryList(ctx, []string{
	//	"/* c1 */ begin /* c2 */",
	//	"insert into stream1 values(3, 'ccc')",
	//	"/* c3 */ commit /* c4 */",
	//}); err != nil {
	//	t.Fatal(err)
	//}
	//execStatement(t, client, "set timestamp=1")
	//execStatement(t, client, "delete from stream1")
	//execStatement(t, client, "describe stream2")
	//execStatement(t, client, "repair table stream2")
	//execStatement(t, client, "optimize table stream2")
	//execStatement(t, client, "analyze table stream2")
}

func startStream(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter) <-chan []*binlogdatapb.VEvent {
	pos, err := framework.Mysqld.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan []*binlogdatapb.VEvent)

	go func() {
		err := framework.Server.VStream(
			ctx,
			&framework.Target,
			pos,
			&binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match: "/.*/",
				}},
			},
			func(evs []*binlogdatapb.VEvent) error {
				t.Logf("evs: %v\n", evs)
				ch <- evs
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		close(ch)
	}()
	return ch
}

func execTransaction(t *testing.T, client *framework.QueryClient, queries []string) {
	t.Helper()
	if err := client.Begin(false); err != nil {
		t.Fatal(err)
	}
	for _, query := range queries {
		if _, err := client.Execute(query, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Commit(); err != nil {
		t.Fatal(err)
	}
}

func execStatement(t *testing.T, client *framework.QueryClient, query string) {
	t.Helper()
	if err := framework.Mysqld.ExecuteSuperQuery(context.Background(), query); err != nil {
		t.Fatal(err)
	}
}
