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

package engine

import (
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
}

func (tp *fakePrimitive) rewind() {
	tp.curResult = 0
}

func (tp *fakePrimitive) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if tp.results == nil {
		return nil, tp.sendErr
	}

	r := tp.results[tp.curResult]
	tp.curResult++
	if r == nil {
		return nil, tp.sendErr
	}
	return r, nil
}

func (tp *fakePrimitive) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	if tp.results == nil {
		return tp.sendErr
	}

	r := tp.results[tp.curResult]
	tp.curResult++
	if r == nil {
		return tp.sendErr
	}
	if err := callback(&sqltypes.Result{Fields: r.Fields}); err != nil {
		return err
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
	return nil
}

func (tp *fakePrimitive) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return tp.Execute(vcursor, bindVars, joinVars, false /* wantfields */)
}
