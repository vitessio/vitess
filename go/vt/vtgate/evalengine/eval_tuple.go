/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type evalTuple struct {
	t []eval
}

var _ eval = (*evalTuple)(nil)

func newEvalTuple(values []*querypb.Value, collation collations.ID) (*evalTuple, error) {
	evals := make([]eval, 0, len(values))

	for _, value := range values {
		val := sqltypes.ProtoToValue(value)

		e, err := valueToEval(val, typedCoercionCollation(val.Type(), collations.CollationForType(val.Type(), collation)), nil)
		if err != nil {
			return nil, err
		}
		evals = append(evals, e)
	}

	return &evalTuple{t: evals}, nil
}

// ToRawBytes encodes the members as the scalar path does: they are evalengine
// output, not caller bytes, so they are trusted rather than re-parsed. A float
// +Inf or a nested tuple is not a SQL literal and would fail NewValue.
func (e *evalTuple) ToRawBytes() []byte {
	vals := make([]sqltypes.Value, 0, len(e.t))
	for _, e2 := range e.t {
		vals = append(vals, evalToSQLValue(e2))
	}
	return sqltypes.TupleToProto(vals).Value
}

func (e *evalTuple) SQLType() sqltypes.Type {
	return sqltypes.Tuple
}

func (e *evalTuple) Size() int32 {
	return 0
}

func (e *evalTuple) Scale() int32 {
	return 0
}
