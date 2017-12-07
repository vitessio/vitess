/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"bytes"
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Benchmark run on 6/27/17, with optimized byte-level operations
// using bytes2.Buffer:
// BenchmarkExecuteVarBinary-4          300           4751125 ns/op

// Benchmark run on 6/25/17, prior to improvements:
// BenchmarkExecuteVarBinary-4          100          14610045 ns/op
// BenchmarkExecuteExpression-4        1000           1047798 ns/op

var benchQuery = "select a from test_table where v = :vtg1 and v0 = :vtg2 and v1 = :vtg3 and v2 = :vtg4 and v3 = :vtg5 and v4 = :vtg6 and v5 = :vtg7 and v6 = :vtg8 and v7 = :vtg9 and v8 = :vtg10 and v9 = :vtg11"
var benchVarValue []byte

func init() {
	// benchQuerySize is the approximate size of the query.
	// This code is in sync with bench_test.go in vtgate.
	benchQuerySize := 1000000

	// Size of value is 1/10 size of query. Then we add
	// 10 such values to the where clause.
	baseval := &bytes.Buffer{}
	for i := 0; i < benchQuerySize/100; i++ {
		baseval.WriteString("\\'123456789")
	}
	benchVarValue = baseval.Bytes()
}

func BenchmarkExecuteVarBinary(b *testing.B) {
	db := setUpTabletServerTest(nil)
	defer db.Close()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	bv := map[string]*querypb.BindVariable{
		"vtg1": sqltypes.Int64BindVariable(1),
	}
	for i := 2; i <= 11; i++ {
		bv[fmt.Sprintf("vtg%d", i)] = sqltypes.BytesBindVariable(benchVarValue)
	}

	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbconfigs); err != nil {
		panic(err)
	}
	defer tsv.StopService()

	db.AllowAll = true
	for i := 0; i < b.N; i++ {
		if _, err := tsv.Execute(context.Background(), &target, benchQuery, bv, 0, nil); err != nil {
			panic(err)
		}
	}
}

func BenchmarkExecuteExpression(b *testing.B) {
	db := setUpTabletServerTest(nil)
	defer db.Close()
	testUtils := newTestUtils()
	// sql that will be executed in this test
	bv := map[string]*querypb.BindVariable{
		"vtg1": sqltypes.Int64BindVariable(1),
	}
	for i := 2; i <= 11; i++ {
		bv[fmt.Sprintf("vtg%d", i)] = &querypb.BindVariable{
			Type:  querypb.Type_EXPRESSION,
			Value: benchVarValue,
		}
	}

	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServerWithNilTopoServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	if err := tsv.StartService(target, dbconfigs); err != nil {
		panic(err)
	}
	defer tsv.StopService()

	db.AllowAll = true
	for i := 0; i < b.N; i++ {
		if _, err := tsv.Execute(context.Background(), &target, benchQuery, bv, 0, nil); err != nil {
			panic(err)
		}
	}
}
