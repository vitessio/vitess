/*
Copyright 2024 The Vitess Authors.

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

package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func BenchmarkSimpleConcatenateTryExecute(b *testing.B) {
	fakeSrc1, fakeSrc2, prim := createSimpleConcatenateForTest()
	ctx := context.Background()
	b.ResetTimer()
	vcursor := &noopVCursor{}
	count := 0
	for i := 0; i < b.N; i++ {
		res, err := prim.TryExecute(ctx, vcursor, map[string]*querypb.BindVariable{}, true)
		require.NoError(b, err)
		count += len(res.Rows)
		fakeSrc1.curResult = 0
		fakeSrc2.curResult = 0
	}
}

func BenchmarkSimpleConcatenateTryStreamExecute(b *testing.B) {
	fakeSrc1, fakeSrc2, prim := createSimpleConcatenateForTest()
	ctx := context.Background()
	b.ResetTimer()
	vcursor := &noopVCursor{}
	var count atomic.Int32
	for i := 0; i < b.N; i++ {
		err := prim.TryStreamExecute(ctx, vcursor, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error {
			count.Add(int32(len(result.Rows)))
			return nil
		})
		require.NoError(b, err)
		fakeSrc1.curResult = 0
		fakeSrc2.curResult = 0
	}
}

func createSimpleConcatenateForTest() (*fakePrimitive, *fakePrimitive, *SimpleConcatenate) {
	fake := r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2")
	var rows []string
	for x := range 10 {
		rows = append(rows, fmt.Sprintf("%d|a%d|b%d", x, x, x))
	}
	result := sqltypes.MakeTestResult(fake.Fields, rows...)
	fake.Rows = result.Rows
	fakeSrc1 := &fakePrimitive{results: []*sqltypes.Result{fake, fake}}
	fakeSrc2 := &fakePrimitive{results: []*sqltypes.Result{fake, fake}}
	prim := NewSimpleConcatenate([]Primitive{
		fakeSrc1,
		fakeSrc2,
	})
	return fakeSrc1, fakeSrc2, prim
}

func TestName(t *testing.T) {
	var x atomic.Bool
	for range 100 {
		go func() {
			old := x.Swap(true)
			t.Log(old)
		}()
	}
}
