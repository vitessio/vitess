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

package vtgate

import (
	"bytes"
	"fmt"
	"testing"

	"golang.org/x/net/context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// Benchmark run on 6/27/17, with optimized byte-level operations
// using bytes2.Buffer:
// BenchmarkWithNormalizer-4            300           5694660 ns/op

// Benchmark run on 6/25/17, prior to improvements:
// BenchmarkWithNormalizer-4            100          10629161 ns/op
// BenchmarkWithoutNormalizer-4      200000              8584 ns/op

var benchQuery string

func init() {
	// benchQuerySize is the approximate size of the query.
	benchQuerySize := 1000000

	// Size of value is 1/10 size of query. Then we add
	// 10 such values to the where clause.
	baseval := &bytes.Buffer{}
	for i := 0; i < benchQuerySize/100; i++ {
		// Add an escape character: This will force the upcoming
		// tokenizer improvement to still create a copy of the string.
		// Then we can see if avoiding the copy will be worth it.
		baseval.WriteString("\\'123456789")
	}

	buf := &bytes.Buffer{}
	buf.WriteString("select a from t1 where v = 1")
	for i := 0; i < 10; i++ {
		fmt.Fprintf(buf, " and v%d = '%d%s'", i, i, baseval.String())
	}
	benchQuery = buf.String()
	// fmt.Printf("len: %d\n", len(benchQuery))
}

func BenchmarkWithNormalizer(b *testing.B) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	saved := rpcVTGate.executor.normalize
	rpcVTGate.executor.normalize = true
	defer func() { rpcVTGate.executor.normalize = saved }()

	for i := 0; i < b.N; i++ {
		_, _, err := rpcVTGate.Execute(
			context.Background(),
			&vtgatepb.Session{
				TargetString: "@master",
				Options:      executeOptions,
			},
			benchQuery,
			nil,
		)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkWithoutNormalizer(b *testing.B) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	saved := rpcVTGate.executor.normalize
	rpcVTGate.executor.normalize = false
	defer func() { rpcVTGate.executor.normalize = saved }()

	for i := 0; i < b.N; i++ {
		_, _, err := rpcVTGate.Execute(
			context.Background(),
			&vtgatepb.Session{
				TargetString: "@master",
				Options:      executeOptions,
			},
			benchQuery,
			nil,
		)
		if err != nil {
			panic(err)
		}
	}
}
