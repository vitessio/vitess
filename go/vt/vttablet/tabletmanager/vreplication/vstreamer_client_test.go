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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestTabletVStreamerClientOpen(t *testing.T) {
	tablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(tablet)

	type fields struct {
		mu             sync.Mutex
		isOpen         bool
		tablet         *topodatapb.Tablet
		target         *querypb.Target
		tsQueryService queryservice.QueryService
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		err    string
	}{
		{
			name: "initializes streamer client",
			fields: fields{
				tablet: tablet,
			},
			args: args{
				ctx: context.Background(),
			},
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			vsClient := &TabletVStreamerClient{
				tablet: tcase.fields.tablet,
			}

			err := vsClient.Open(tcase.args.ctx)

			if err != nil {
				if !strings.Contains(err.Error(), tcase.err) {
					t.Errorf("TabletVStreamerClient.Open() error:\n%v, want\n%v", err, tcase.err)
				}
				return
			}

			if tcase.err != "" {
				t.Errorf("TabletVStreamerClient.Open() error:\n%v, want\n%v", err, tcase.err)
			}

			if !vsClient.isOpen {
				t.Errorf("TabletVStreamerClient.Open() isOpen set to false, expected true")
			}

			if vsClient.tablet == nil {
				t.Errorf("TabletVStreamerClient.Open() expected sourceSe to be set")
			}
		})
	}
}

func TestTabletVStreamerClientClose(t *testing.T) {
	tablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(tablet)

	type fields struct {
		mu             sync.Mutex
		isOpen         bool
		tablet         *topodatapb.Tablet
		target         *querypb.Target
		tsQueryService queryservice.QueryService
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		err    string
	}{
		{
			name: "closes engine correctly",
			fields: fields{
				tablet: tablet,
			},
			args: args{
				ctx: context.Background(),
			},
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			vsClient := &TabletVStreamerClient{
				tablet: tcase.fields.tablet,
			}

			err := vsClient.Open(tcase.args.ctx)
			if err != nil {
				t.Errorf("Failed to Open vsClient")
				return
			}

			err = vsClient.Close(tcase.args.ctx)

			if tcase.err != "" {
				t.Errorf("MySQLVStreamerClient.Close() error:\n%v, want\n%v", err, tcase.err)
			}

			if vsClient.isOpen {
				t.Errorf("MySQLVStreamerClient.Close() isOpen set to true, expected false")
			}
		})
	}
}

func TestTabletVStreamerClientVStream(t *testing.T) {
	tablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(tablet)

	vsClient := &TabletVStreamerClient{
		tablet: tablet,
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	eventsChan := make(chan *binlogdatapb.VEvent, 1000)
	send := func(events []*binlogdatapb.VEvent) error {
		fmt.Println(events)
		fmt.Println(len(events))
		for _, e := range events {
			eventsChan <- e
		}
		return nil
	}

	execStatements(t, []string{
		"create table t1(id int, ts timestamp, dt datetime)",
		fmt.Sprintf("create table %s.t1(id int, ts timestamp, dt datetime)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

	ctx := context.Background()
	err := vsClient.Open(ctx)
	if err != nil {
		t.Errorf("Failed to Open vsClient")
		return
	}

	defer vsClient.Close(ctx)

	pos := masterPosition(t)
	// This asserts that events are flowing through the VStream when using mysql client
	go vsClient.VStream(ctx, pos, filter, send)

	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), "select now()")
	if err != nil {
		t.Fatal(err)
	}
	want := qr.Rows[0][0].ToString()
	execStatements(t, []string{
		fmt.Sprintf("insert into t1 values(1, '%s', '%s')", want, want),
	})

	select {
	case got := <-eventsChan:
		if got.Type != binlogdatapb.VEventType_GTID {
			t.Errorf("Did not get expected events: want: %v, got: %v", binlogdatapb.VEventType_GTID, got.Type)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("no events received")
	}
}

func TestTabletVStreamerClientVStreamRows(t *testing.T) {
	tablet := addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(tablet)

	vsClient := &TabletVStreamerClient{
		tablet: tablet,
	}

	eventsChan := make(chan *querypb.Row, 1000)
	send := func(streamerResponse *binlogdatapb.VStreamRowsResponse) error {
		for _, row := range streamerResponse.Rows {
			eventsChan <- row
		}
		return nil
	}

	execStatements(t, []string{
		"create table t1(id int, ts timestamp, dt datetime)",
		fmt.Sprintf("create table %s.t1(id int, ts timestamp, dt datetime)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), "select now()")
	if err != nil {
		t.Fatal(err)
	}
	want := qr.Rows[0][0].ToString()
	ctx := context.Background()
	err = vsClient.Open(ctx)
	if err != nil {
		t.Errorf("Failed to Open vsClient")
		return
	}

	defer vsClient.Close(ctx)

	// This asserts that events are flowing through the VStream when using mysql client
	go vsClient.VStreamRows(ctx, "select * from t1", nil, send)

	execStatements(t, []string{
		fmt.Sprintf("insert into t1 values(1, '%s', '%s')", want, want),
	})

	select {
	case <-eventsChan:
		// Success got expected
	case <-time.After(5 * time.Second):
		t.Errorf("no events received")
	}
}

func TestNewMySQLVStreamerClient(t *testing.T) {
	type args struct {
		sourceConnParams *mysql.ConnParams
	}
	tests := []struct {
		name string
		args args
		want *MySQLVStreamerClient
	}{
		{
			name: "sets conn params for MySQLVStreamerClient ",
			args: args{
				sourceConnParams: &mysql.ConnParams{
					Host: "testhost",
					Port: 3306,
				},
			},
			want: &MySQLVStreamerClient{
				sourceConnParams: &mysql.ConnParams{
					Host: "testhost",
					Port: 3306,
				},
			},
		},
	}
	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			if got := NewMySQLVStreamerClient(tcase.args.sourceConnParams); !reflect.DeepEqual(got, tcase.want) {
				t.Errorf("NewMySQLVStreamerClient() = %v, want %v", got, tcase.want)
			}
		})
	}
}

func TestMySQLVStreamerClientOpen(t *testing.T) {
	type fields struct {
		isOpen           bool
		sourceConnParams *mysql.ConnParams
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		err    string
	}{
		{
			name: "initializes streamer correctly",
			fields: fields{
				sourceConnParams: env.Dbcfgs.ExternalReplWithDB(),
			},
			args: args{
				ctx: context.Background(),
			},
		},
		{
			name: "returns error when invalid conn params are provided",
			fields: fields{
				sourceConnParams: &mysql.ConnParams{
					Host: "invalidhost",
					Port: 3306,
				},
			},
			args: args{
				ctx: context.Background(),
			},
			err: "failed: dial tcp: lookup invalidhost",
		},
	}
	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			vsClient := &MySQLVStreamerClient{
				sourceConnParams: tcase.fields.sourceConnParams,
			}

			err := vsClient.Open(tcase.args.ctx)

			if err != nil {
				if !strings.Contains(err.Error(), tcase.err) {
					t.Errorf("MySQLVStreamerClient.Open() error:\n%v, want\n%v", err, tcase.err)
				}
				return
			}

			if tcase.err != "" {
				t.Errorf("MySQLVStreamerClient.Open() error:\n%v, want\n%v", err, tcase.err)
			}

			if !vsClient.isOpen {
				t.Errorf("MySQLVStreamerClient.Open() isOpen set to false, expected true")
			}

			if !vsClient.sourceSe.IsOpen() {
				t.Errorf("MySQLVStreamerClient.Open() expected sourceSe to be opened")
			}

			if !vsClient.vsEngine.IsOpen() {
				t.Errorf("MySQLVStreamerClient.Open() expected vsEngine to be opened")
			}
		})
	}
}

func TestMySQLVStreamerClientClose(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		isOpen           bool
		sourceConnParams *mysql.ConnParams
		vsEngine         *vstreamer.Engine
		sourceSe         *schema.Engine
	}
	type args struct {
		ctx context.Context
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		err    string
	}{
		{
			name: "closes engine correctly",
			fields: fields{
				sourceConnParams: env.Dbcfgs.ExternalReplWithDB(),
			},
			args: args{
				ctx: context.Background(),
			},
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			vsClient := &MySQLVStreamerClient{
				isOpen:           tcase.fields.isOpen,
				sourceConnParams: tcase.fields.sourceConnParams,
			}

			err := vsClient.Open(tcase.args.ctx)
			if err != nil {
				t.Errorf("Failed to Open vsClient")
				return
			}

			err = vsClient.Close(tcase.args.ctx)

			if tcase.err != "" {
				t.Errorf("MySQLVStreamerClient.Close() error:\n%v, want\n%v", err, tcase.err)
			}

			if vsClient.isOpen {
				t.Errorf("MySQLVStreamerClient.Close() isOpen set to true, expected false")
			}

			if vsClient.sourceSe.IsOpen() {
				t.Errorf("MySQLVStreamerClient.Close() expected sourceSe to be closed")
			}

			if vsClient.vsEngine.IsOpen() {
				t.Errorf("MySQLVStreamerClient.Close() expected vsEngine to be closed")
			}
		})
	}
}

func TestMySQLVStreamerClientVStream(t *testing.T) {
	vsClient := &MySQLVStreamerClient{
		sourceConnParams: env.Dbcfgs.ExternalReplWithDB(),
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	eventsChan := make(chan *binlogdatapb.VEvent, 1000)
	send := func(events []*binlogdatapb.VEvent) error {
		fmt.Println(events)
		fmt.Println(len(events))
		for _, e := range events {
			eventsChan <- e
		}
		return nil
	}

	execStatements(t, []string{
		"create table t1(id int, ts timestamp, dt datetime)",
		fmt.Sprintf("create table %s.t1(id int, ts timestamp, dt datetime)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

	ctx := context.Background()
	err := vsClient.Open(ctx)
	if err != nil {
		t.Errorf("Failed to Open vsClient")
		return
	}

	defer vsClient.Close(ctx)

	pos := masterPosition(t)
	// This asserts that events are flowing through the VStream when using mysql client
	go vsClient.VStream(ctx, pos, filter, send)

	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), "select now()")
	if err != nil {
		t.Fatal(err)
	}
	want := qr.Rows[0][0].ToString()
	execStatements(t, []string{
		fmt.Sprintf("insert into t1 values(1, '%s', '%s')", want, want),
	})

	select {
	case got := <-eventsChan:
		if got.Type != binlogdatapb.VEventType_GTID {
			t.Errorf("Did not get expected events: want: %v, got: %v", binlogdatapb.VEventType_GTID, got.Type)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("no events received")
	}
}

func TestMySQLVStreamerClientVStreamRows(t *testing.T) {
	vsClient := &MySQLVStreamerClient{
		sourceConnParams: env.Dbcfgs.ExternalReplWithDB(),
	}

	eventsChan := make(chan *querypb.Row, 1000)
	send := func(streamerResponse *binlogdatapb.VStreamRowsResponse) error {
		for _, row := range streamerResponse.Rows {
			eventsChan <- row
		}
		return nil
	}

	execStatements(t, []string{
		"create table t1(id int, ts timestamp, dt datetime)",
		fmt.Sprintf("create table %s.t1(id int, ts timestamp, dt datetime)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), "select now()")
	if err != nil {
		t.Fatal(err)
	}
	want := qr.Rows[0][0].ToString()

	ctx := context.Background()
	err = vsClient.Open(ctx)
	if err != nil {
		t.Errorf("Failed to Open vsClient")
		return
	}

	defer vsClient.Close(ctx)

	// This asserts that events are flowing through the VStream when using mysql client
	go vsClient.VStreamRows(ctx, "select * from t1", nil, send)

	execStatements(t, []string{
		fmt.Sprintf("insert into t1 values(1, '%s', '%s')", want, want),
	})

	select {
	case <-eventsChan:
		// Success got expected
	case <-time.After(5 * time.Second):
		t.Errorf("no events received")
	}
}
