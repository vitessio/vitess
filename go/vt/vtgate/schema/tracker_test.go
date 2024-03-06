/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    `http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

var (
	keyspace = "ks"
	cell     = "aa"
)

func TestMain(m *testing.M) {
	exitCode := func() int {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ts := memorytopo.NewServer(ctx, cell)
		ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{})
		_, created := sidecardb.NewIdentifierCache(func(ctx context.Context, keyspace string) (string, error) {
			ki, err := ts.GetKeyspace(ctx, keyspace)
			if err != nil {
				return "", err
			}
			return ki.SidecarDbName, nil
		})
		if !created {
			log.Error("Failed to create a new sidecar database identifier cache as one already existed!")
			return 1
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestTrackingUnHealthyTablet tests that the tracker is sending GetSchema calls only when the tablet is healthy.
func TestTrackingUnHealthyTablet(t *testing.T) {
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      "-80",
		TabletType: topodatapb.TabletType_PRIMARY,
		Cell:       cell,
	}
	tablet := &topodatapb.Tablet{
		Keyspace: target.Keyspace,
		Shard:    target.Shard,
		Type:     target.TabletType,
	}

	sbc := sandboxconn.NewSandboxConn(tablet)
	ch := make(chan *discovery.TabletHealth)
	tracker := NewTracker(ch, false, sqlparser.NewTestParser())
	tracker.consumeDelay = 1 * time.Millisecond
	tracker.Start()
	defer tracker.Stop()

	// the test are written in a way that it expects 3 signals to be sent from the tracker to the subscriber.
	wg := sync.WaitGroup{}
	wg.Add(3)
	tracker.RegisterSignalReceiver(func() {
		wg.Done()
	})

	tcases := []struct {
		name          string
		serving       bool
		expectedQuery string
		updatedTbls   []string
	}{
		{
			name:    "initial load",
			serving: true,
		},
		{
			name:        "initial load",
			serving:     true,
			updatedTbls: []string{"a"},
		},
		{
			name:    "non serving tablet",
			serving: false,
		},
		{
			name:    "now serving tablet",
			serving: true,
		},
	}

	for _, tcase := range tcases {
		ch <- &discovery.TabletHealth{
			Conn:    sbc,
			Tablet:  tablet,
			Target:  target,
			Serving: tcase.serving,
			Stats:   &querypb.RealtimeStats{TableSchemaChanged: tcase.updatedTbls},
		}
		time.Sleep(5 * time.Millisecond)
	}

	require.False(t, waitTimeout(&wg, 5*time.Second), "schema was updated but received no signal")
	require.EqualValues(t, 3, sbc.GetSchemaCount.Load())
}

// TestTrackerGetKeyspaceUpdateController tests table update controller initialization.
func TestTrackerGetKeyspaceUpdateController(t *testing.T) {
	ks3 := &updateController{}
	tracker := Tracker{
		tracked: map[keyspaceStr]*updateController{
			"ks3": ks3,
		},
	}

	th1 := &discovery.TabletHealth{
		Target: &querypb.Target{Keyspace: "ks1"},
	}
	ks1 := tracker.getKeyspaceUpdateController(th1)

	th2 := &discovery.TabletHealth{
		Target: &querypb.Target{Keyspace: "ks2"},
	}
	ks2 := tracker.getKeyspaceUpdateController(th2)

	th3 := &discovery.TabletHealth{
		Target: &querypb.Target{Keyspace: "ks3"},
	}

	assert.NotEqual(t, ks1, ks2, "ks1 and ks2 should not be equal, belongs to different keyspace")
	assert.Equal(t, ks1, tracker.getKeyspaceUpdateController(th1), "received different updateController")
	assert.Equal(t, ks2, tracker.getKeyspaceUpdateController(th2), "received different updateController")
	assert.Equal(t, ks3, tracker.getKeyspaceUpdateController(th3), "received different updateController")

	assert.NotNil(t, ks1.reloadKeyspace, "ks1 needs to be initialized")
	assert.NotNil(t, ks2.reloadKeyspace, "ks2 needs to be initialized")
	assert.Nil(t, ks3.reloadKeyspace, "ks3 already initialized")
}

// TestTableTracking tests that the tracker is able to track table schema changes.
func TestTableTracking(t *testing.T) {
	schemaDefResult := []map[string]string{{
		"prior": "create table prior(id int primary key)",
	}, {
		// initial load of view - kept empty
	}, {
		"t1": "create table t1(id bigint primary key, name varchar(50), email varchar(50) not null default 'a@b.com')",
		"T1": "create table T1(id varchar(50) primary key)",
	}, {
		"T1": "create table T1(id varchar(50) primary key, name varchar(50))",
		"t3": "create table t3(id datetime primary key)",
	}, {
		"t4": "create table t4(name varchar(50) primary key)",
	}}

	testcases := []testCases{{
		testName: "initial table load",
		expTbl: map[string][]vindexes.Column{
			"prior": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT32, CollationName: "binary", Nullable: true}},
		},
	}, {
		testName: "new tables",
		updTbl:   []string{"t1", "T1"},
		expTbl: map[string][]vindexes.Column{
			"prior": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT32, CollationName: "binary", Nullable: true}},
			"t1":    {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: true}, {Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}, {Name: sqlparser.NewIdentifierCI("email"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: false, Default: &sqlparser.Literal{Val: "a@b.com"}}},
			"T1":    {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}},
		},
	}, {
		testName: "delete prior, updated T1 and new t3",
		updTbl:   []string{"prior", "T1", "t3"},
		expTbl: map[string][]vindexes.Column{
			"t1": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: true}, {Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}, {Name: sqlparser.NewIdentifierCI("email"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: false, Default: &sqlparser.Literal{Val: "a@b.com"}}},
			"T1": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}, {Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}},
			"t3": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_DATETIME, CollationName: "binary", Size: 0, Nullable: true}},
		},
	}, {
		testName: "new t4",
		updTbl:   []string{"t4"},
		expTbl: map[string][]vindexes.Column{
			"t1": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: true}, {Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}, {Name: sqlparser.NewIdentifierCI("email"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: false, Default: &sqlparser.Literal{Val: "a@b.com"}}},
			"T1": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}, {Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}},
			"t3": {{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_DATETIME, CollationName: "binary", Size: 0, Nullable: true}},
			"t4": {{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, Nullable: true}},
		},
	}}

	testTracker(t, schemaDefResult, testcases)
}

// TestViewsTracking tests that the tracker is able to track views.
func TestViewsTracking(t *testing.T) {
	schemaDefResult := []map[string]string{{
		// initial load of table - kept empty
	}, {
		"prior": "create view prior as select 1 from tbl",
	}, {
		"t1": "create view t1 as select 1 from tbl1",
		"V1": "create view V1 as select 1 from tbl2",
	}, {
		"V1": "create view V1 as select 1,2 from tbl2",
		"t3": "create view t3 as select 1 from tbl3",
	}, {
		"t4": "create view t4 as select 1 from tbl4",
	}}

	testcases := []testCases{{
		testName: "initial view load",
		expView: map[string]string{
			"prior": "select 1 from tbl"},
	}, {
		testName: "new view t1, V1",
		updView:  []string{"t1", "V1"},
		expView: map[string]string{
			"t1":    "select 1 from tbl1",
			"V1":    "select 1 from tbl2",
			"prior": "select 1 from tbl"},
	}, {
		testName: "delete prior, updated V1 and new t3",
		updView:  []string{"prior", "V1", "t3"},
		expView: map[string]string{
			"t1": "select 1 from tbl1",
			"V1": "select 1, 2 from tbl2",
			"t3": "select 1 from tbl3"},
	}, {
		testName: "new t4",
		updView:  []string{"t4"},
		expView: map[string]string{
			"t1": "select 1 from tbl1",
			"V1": "select 1, 2 from tbl2",
			"t3": "select 1 from tbl3",
			"t4": "select 1 from tbl4"},
	}}

	testTracker(t, schemaDefResult, testcases)
}

// TestFKInfoRetrieval tests that the tracker is able to retrieve required foreign key information from ddl statement.
func TestFKInfoRetrieval(t *testing.T) {
	schemaDefResult := []map[string]string{{
		"my_tbl": "CREATE TABLE `my_tbl` (" +
			"`id` bigint NOT NULL AUTO_INCREMENT," +
			"`name` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL," +
			"`email` varbinary(100) DEFAULT NULL," +
			"PRIMARY KEY (`id`)," +
			"KEY `id` (`id`,`name`)) " +
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	}, {
		// initial load of view - kept empty
	}, {
		"my_child_tbl": "CREATE TABLE `my_child_tbl` (" +
			"`id` bigint NOT NULL AUTO_INCREMENT," +
			"`name` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL," +
			"`code` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL," +
			"`my_id` bigint DEFAULT NULL," +
			"PRIMARY KEY (`id`)," +
			"KEY `my_id` (`my_id`,`name`)," +
			"CONSTRAINT `my_child_tbl_ibfk_1` FOREIGN KEY (`my_id`, `name`) REFERENCES `my_tbl` (`id`, `name`) ON DELETE CASCADE) " +
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	}}

	testcases := []testCases{{
		testName: "initial table load",
		expTbl: map[string][]vindexes.Column{
			"my_tbl": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: false},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, CollationName: "latin1_swedish_ci", Nullable: true, Default: &sqlparser.NullVal{}},
				{Name: sqlparser.NewIdentifierCI("email"), Type: querypb.Type_VARBINARY, Size: 100, CollationName: "binary", Nullable: true, Default: &sqlparser.NullVal{}},
			},
		},
	}, {
		testName: "new tables",
		updTbl:   []string{"my_child_tbl"},
		expTbl: map[string][]vindexes.Column{
			"my_tbl": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: false},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, CollationName: "latin1_swedish_ci", Nullable: true, Default: &sqlparser.NullVal{}},
				{Name: sqlparser.NewIdentifierCI("email"), Type: querypb.Type_VARBINARY, Size: 100, CollationName: "binary", Nullable: true, Default: &sqlparser.NullVal{}},
			},
			"my_child_tbl": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: false},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, Size: 50, CollationName: "latin1_swedish_ci", Nullable: true, Default: &sqlparser.NullVal{}},
				{Name: sqlparser.NewIdentifierCI("code"), Type: querypb.Type_VARCHAR, Size: 6, CollationName: "utf8mb4_0900_ai_ci", Nullable: true, Default: &sqlparser.NullVal{}},
				{Name: sqlparser.NewIdentifierCI("my_id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: true, Default: &sqlparser.NullVal{}},
			},
		},
		expFk: map[string]string{
			"my_tbl":       "",
			"my_child_tbl": "foreign key (my_id, `name`) references my_tbl (id, `name`) on delete cascade",
		},
	}}

	testTracker(t, schemaDefResult, testcases)
}

// TestIndexInfoRetrieval tests that the tracker is able to retrieve required index information from ddl statement.
func TestIndexInfoRetrieval(t *testing.T) {
	schemaDefResult := []map[string]string{{
		"my_tbl": "CREATE TABLE `my_tbl` (" +
			"`id` bigint NOT NULL AUTO_INCREMENT," +
			"`name` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL," +
			"`email` varbinary(100) DEFAULT NULL," +
			"PRIMARY KEY (`id`)," +
			"KEY `id` (`id`,`name`)) " +
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	}, {
		// initial load of view - kept empty
	}, {
		"my_tbl": "CREATE TABLE `my_tbl` (" +
			"`id` bigint NOT NULL AUTO_INCREMENT," +
			"`name` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL," +
			"`email` varbinary(100) DEFAULT NULL," +
			"PRIMARY KEY (`id`)," +
			"KEY `id` (`id`,`name`), " +
			"UNIQUE KEY `email` (`email`)) " +
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	}}

	testcases := []testCases{{
		testName: "initial table load",
		expTbl: map[string][]vindexes.Column{
			"my_tbl": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: false},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, CollationName: "latin1_swedish_ci", Size: 50, Nullable: true, Default: &sqlparser.NullVal{}},
				{Name: sqlparser.NewIdentifierCI("email"), Type: querypb.Type_VARBINARY, CollationName: "binary", Size: 100, Nullable: true, Default: &sqlparser.NullVal{}},
			},
		},
		expIdx: map[string][]string{
			"my_tbl": {
				"primary key (id)",
				"key id (id, `name`)",
			},
		},
	}, {
		testName: "next load",
		updTbl:   []string{"my_tbl"},
		expTbl: map[string][]vindexes.Column{
			"my_tbl": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64, CollationName: "binary", Nullable: false},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, CollationName: "latin1_swedish_ci", Size: 50, Nullable: true, Default: &sqlparser.NullVal{}},
				{Name: sqlparser.NewIdentifierCI("email"), Type: querypb.Type_VARBINARY, CollationName: "binary", Size: 100, Nullable: true, Default: &sqlparser.NullVal{}},
			},
		},
		expIdx: map[string][]string{
			"my_tbl": {
				"primary key (id)",
				"key id (id, `name`)",
				"unique key email (email)",
			},
		},
	}}

	testTracker(t, schemaDefResult, testcases)
}

type testCases struct {
	testName string

	updTbl []string
	expTbl map[string][]vindexes.Column
	expFk  map[string]string
	expIdx map[string][]string

	updView []string
	expView map[string]string
}

func testTracker(t *testing.T, schemaDefResult []map[string]string, tcases []testCases) {
	ch := make(chan *discovery.TabletHealth)
	tracker := NewTracker(ch, true, sqlparser.NewTestParser())
	tracker.consumeDelay = 1 * time.Millisecond
	tracker.Start()
	defer tracker.Stop()

	wg := sync.WaitGroup{}
	tracker.RegisterSignalReceiver(func() {
		wg.Done()
	})

	target := &querypb.Target{Cell: cell, Keyspace: keyspace, Shard: "-80", TabletType: topodatapb.TabletType_PRIMARY}
	tablet := &topodatapb.Tablet{Keyspace: target.Keyspace, Shard: target.Shard, Type: target.TabletType}

	sbc := sandboxconn.NewSandboxConn(tablet)
	sbc.SetSchemaResult(schemaDefResult)

	for count, tcase := range tcases {
		t.Run(tcase.testName, func(t *testing.T) {
			wg.Add(1)
			ch <- &discovery.TabletHealth{
				Conn:    sbc,
				Tablet:  tablet,
				Target:  target,
				Serving: true,
				Stats:   &querypb.RealtimeStats{TableSchemaChanged: tcase.updTbl, ViewSchemaChanged: tcase.updView},
			}

			require.False(t, waitTimeout(&wg, time.Second), "schema was updated but received no signal")
			require.EqualValues(t, count+2, sbc.GetSchemaCount.Load())

			_, keyspacePresent := tracker.tracked[target.Keyspace]
			require.Equal(t, true, keyspacePresent)

			for k, v := range tcase.expTbl {
				utils.MustMatch(t, v, tracker.GetColumns(keyspace, k), "mismatch columns for table: ", k)
				if len(tcase.expFk[k]) > 0 {
					fks := tracker.GetForeignKeys(keyspace, k)
					for _, fk := range fks {
						utils.MustMatch(t, tcase.expFk[k], sqlparser.String(fk), "mismatch foreign keys for table: ", k)
					}
				}
				expIndexes := tcase.expIdx[k]
				if len(expIndexes) > 0 {
					idxs := tracker.GetIndexes(keyspace, k)
					if len(expIndexes) != len(idxs) {
						t.Fatalf("mismatch index for table: %s", k)
					}
					for i, idx := range idxs {
						utils.MustMatch(t, expIndexes[i], sqlparser.String(idx), "mismatch index for table: ", k)
					}
				}
			}

			for k, v := range tcase.expView {
				utils.MustMatch(t, v, sqlparser.String(tracker.GetViews(keyspace, k)), "mismatch for view: ", k)
			}
		})
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
