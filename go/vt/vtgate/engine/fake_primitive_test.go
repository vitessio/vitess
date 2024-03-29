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

package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// fakePrimitive fakes a primitive. For every call, it sends the
// next result from the results. If the next result is nil, it
// returns sendErr. For streaming calls, it sends the field info
// first and two rows at a time till all rows are sent.
type fakePrimitive struct {
	results   []*sqltypes.Result
	curResult int
	// sendErr is sent at the end of the stream if it's set.
	sendErr error

	log []string

	allResultsInOneCall bool

	async bool
}

func (f *fakePrimitive) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{}, nil
}

var _ Primitive = (*fakePrimitive)(nil)

func (f *fakePrimitive) rewind() {
	f.curResult = 0
	f.log = nil
}

func (f *fakePrimitive) RouteType() string {
	return "Fake"
}

func (f *fakePrimitive) GetKeyspaceName() string {
	return "fakeKs"
}

func (f *fakePrimitive) GetTableName() string {
	return "fakeTable"
}

func (f *fakePrimitive) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("Execute %v %v", printBindVars(bindVars), wantfields))
	if f.results == nil {
		return nil, f.sendErr
	}

	r := f.results[f.curResult]
	f.curResult++
	if r == nil {
		return nil, f.sendErr
	}
	return r, nil
}

func (f *fakePrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	f.log = append(f.log, fmt.Sprintf("StreamExecute %v %v", printBindVars(bindVars), wantfields))
	if f.results == nil {
		return f.sendErr
	}

	if f.async {
		return f.asyncCall(callback)
	}
	return f.syncCall(wantfields, callback)
}

func (f *fakePrimitive) syncCall(wantfields bool, callback func(*sqltypes.Result) error) error {
	readMoreResults := true
	for readMoreResults && f.curResult < len(f.results) {
		readMoreResults = f.allResultsInOneCall
		r := f.results[f.curResult]
		f.curResult++
		if r == nil {
			return f.sendErr
		}
		if wantfields {
			if err := callback(&sqltypes.Result{Fields: r.Fields}); err != nil {
				return err
			}
		}
		result := &sqltypes.Result{}
		for i := 0; i < len(r.Rows); i++ {
			result.Rows = append(result.Rows, r.Rows[i])
			// Send only two rows at a time.
			if i%2 == 1 {
				if err := callback(result); err != nil {
					return err
				}
				result = &sqltypes.Result{}
			}
		}
		if len(result.Rows) != 0 {
			if err := callback(result); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *fakePrimitive) asyncCall(callback func(*sqltypes.Result) error) error {
	var g errgroup.Group
	var fields []*querypb.Field
	if len(f.results) > 0 {
		fields = f.results[0].Fields
	}
	for _, res := range f.results {
		qr := res
		g.Go(func() error {
			if qr == nil {
				return f.sendErr
			}
			if err := callback(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
			result := &sqltypes.Result{}
			for i := 0; i < len(qr.Rows); i++ {
				result.Rows = append(result.Rows, qr.Rows[i])
				// Send only two rows at a time.
				if i%2 == 1 {
					if err := callback(result); err != nil {
						return err
					}
					result = &sqltypes.Result{}
				}
			}
			if len(result.Rows) != 0 {
				if err := callback(result); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

func (f *fakePrimitive) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("GetFields %v", printBindVars(bindVars)))
	return f.TryExecute(ctx, vcursor, bindVars, true /* wantfields */)
}

func (f *fakePrimitive) ExpectLog(t *testing.T, want []string) {
	t.Helper()
	if !reflect.DeepEqual(f.log, want) {
		t.Errorf("vc.log got:\n%v\nwant:\n%v", strings.Join(f.log, "\n"), strings.Join(want, "\n"))
	}
}

func (f *fakePrimitive) NeedsTransaction() bool {
	return false
}

func wrapStreamExecute(prim Primitive, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	var result *sqltypes.Result
	err := prim.TryStreamExecute(context.Background(), vcursor, bindVars, wantfields, func(r *sqltypes.Result) error {
		if result == nil {
			result = r
		} else {
			result.Rows = append(result.Rows, r.Rows...)
		}
		return nil
	})
	return result, err
}

func (f *fakePrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "fake"}
}
