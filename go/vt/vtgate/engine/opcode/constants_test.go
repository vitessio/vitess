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

package opcode

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestCheckAllAggrOpCodes(t *testing.T) {
	// This test is just checking that we never reach the panic when using Type() on valid opcodes
	for i := AggregateOpcode(0); i < _NumOfOpCodes; i++ {
		i.SQLType(sqltypes.Null)
	}
}

func TestType(t *testing.T) {
	tt := []struct {
		opcode AggregateOpcode
		typ    querypb.Type
		out    querypb.Type
	}{
		{AggregateUnassigned, sqltypes.VarChar, sqltypes.Null},
		{AggregateGroupConcat, sqltypes.VarChar, sqltypes.Text},
		{AggregateGroupConcat, sqltypes.Blob, sqltypes.Blob},
		{AggregateGroupConcat, sqltypes.Unknown, sqltypes.Unknown},
		{AggregateMax, sqltypes.Int64, sqltypes.Int64},
		{AggregateMax, sqltypes.Float64, sqltypes.Float64},
		{AggregateSumDistinct, sqltypes.Unknown, sqltypes.Unknown},
		{AggregateSumDistinct, sqltypes.Int64, sqltypes.Decimal},
		{AggregateSumDistinct, sqltypes.Decimal, sqltypes.Decimal},
		{AggregateCount, sqltypes.Int32, sqltypes.Int64},
		{AggregateCountStar, sqltypes.Int64, sqltypes.Int64},
		{AggregateGtid, sqltypes.VarChar, sqltypes.VarChar},
	}

	for _, tc := range tt {
		t.Run(tc.opcode.String()+"_"+tc.typ.String(), func(t *testing.T) {
			out := tc.opcode.SQLType(tc.typ)
			assert.Equal(t, tc.out, out)
		})
	}
}

func TestType_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			errMsg, ok := r.(string)
			assert.True(t, ok, "Expected a string panic message")
			assert.Contains(t, errMsg, "ERROR", "Expected panic message containing 'ERROR'")
		}
	}()
	AggregateOpcode(999).SQLType(sqltypes.VarChar)
}

func TestNeedsListArg(t *testing.T) {
	tt := []struct {
		opcode PulloutOpcode
		out    bool
	}{
		{PulloutValue, false},
		{PulloutIn, true},
		{PulloutNotIn, true},
		{PulloutExists, false},
		{PulloutNotExists, false},
	}

	for _, tc := range tt {
		t.Run(tc.opcode.String(), func(t *testing.T) {
			out := tc.opcode.NeedsListArg()
			assert.Equal(t, tc.out, out)
		})
	}
}

func TestPulloutOpcode_MarshalJSON(t *testing.T) {
	tt := []struct {
		opcode PulloutOpcode
		out    string
	}{
		{PulloutValue, "\"PulloutValue\""},
		{PulloutIn, "\"PulloutIn\""},
		{PulloutNotIn, "\"PulloutNotIn\""},
		{PulloutExists, "\"PulloutExists\""},
		{PulloutNotExists, "\"PulloutNotExists\""},
	}

	for _, tc := range tt {
		t.Run(tc.opcode.String(), func(t *testing.T) {
			out, err := json.Marshal(tc.opcode)
			require.NoError(t, err, "Unexpected error")
			assert.Equal(t, tc.out, string(out))
		})
	}
}

func TestAggregateOpcode_MarshalJSON(t *testing.T) {
	tt := []struct {
		opcode AggregateOpcode
		out    string
	}{
		{AggregateCount, "\"count\""},
		{AggregateSum, "\"sum\""},
		{AggregateMin, "\"min\""},
		{AggregateMax, "\"max\""},
		{AggregateCountDistinct, "\"count_distinct\""},
		{AggregateSumDistinct, "\"sum_distinct\""},
		{AggregateGtid, "\"vgtid\""},
		{AggregateCountStar, "\"count_star\""},
		{AggregateGroupConcat, "\"group_concat\""},
		{AggregateAnyValue, "\"any_value\""},
		{AggregateAvg, "\"avg\""},
		{999, "\"ERROR\""},
	}

	for _, tc := range tt {
		t.Run(tc.opcode.String(), func(t *testing.T) {
			out, err := json.Marshal(tc.opcode)
			require.NoError(t, err, "Unexpected error")
			assert.Equal(t, tc.out, string(out))
		})
	}
}

func TestNeedsComparableValues(t *testing.T) {
	for i := AggregateOpcode(0); i < _NumOfOpCodes; i++ {
		if i == AggregateCountDistinct || i == AggregateSumDistinct || i == AggregateMin || i == AggregateMax {
			assert.True(t, i.NeedsComparableValues())
		} else {
			assert.False(t, i.NeedsComparableValues())
		}
	}
}

func TestIsDistinct(t *testing.T) {
	for i := AggregateOpcode(0); i < _NumOfOpCodes; i++ {
		if i == AggregateCountDistinct || i == AggregateSumDistinct {
			assert.True(t, i.IsDistinct())
		} else {
			assert.False(t, i.IsDistinct())
		}
	}
}

func TestCheckAllWindowOpCodes(t *testing.T) {
	// This test is just checking that we never reach the panic when using Type() on valid opcodes
	for i := WindowOpcode(0); i < _NumOfWindowOpCodes; i++ {
		i.SQLType(sqltypes.Null)
	}
}

func TestWindowType(t *testing.T) {
	tt := []struct {
		opcode WindowOpcode
		typ    querypb.Type
		out    querypb.Type
	}{
		{WindowUnassigned, sqltypes.VarChar, sqltypes.Null},
		{WindowRowNumber, sqltypes.VarChar, sqltypes.Int64},
		{WindowRank, sqltypes.VarChar, sqltypes.Int64},
		{WindowDenseRank, sqltypes.VarChar, sqltypes.Int64},
		{WindowPercentRank, sqltypes.VarChar, sqltypes.Float64},
		{WindowCumeDist, sqltypes.VarChar, sqltypes.Float64},
		{WindowNtile, sqltypes.VarChar, sqltypes.Int64},
		{WindowFirstValue, sqltypes.Int64, sqltypes.Int64},
		{WindowFirstValue, sqltypes.VarChar, sqltypes.VarChar},
		{WindowLastValue, sqltypes.Int64, sqltypes.Int64},
		{WindowNthValue, sqltypes.Int64, sqltypes.Int64},
		{WindowLead, sqltypes.Int64, sqltypes.Int64},
		{WindowLag, sqltypes.Int64, sqltypes.Int64},
	}

	for _, tc := range tt {
		t.Run(tc.opcode.String()+"_"+tc.typ.String(), func(t *testing.T) {
			out := tc.opcode.SQLType(tc.typ)
			assert.Equal(t, tc.out, out)
		})
	}
}

func TestWindowType_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			errMsg, ok := r.(string)
			assert.True(t, ok, "Expected a string panic message")
			assert.Contains(t, errMsg, "invalid window function found", "Expected panic message containing 'invalid window function found'")
		}
	}()
	WindowOpcode(999).SQLType(sqltypes.VarChar)
}

func TestWindowOpcode_MarshalJSON(t *testing.T) {
	tt := []struct {
		opcode WindowOpcode
		out    string
	}{
		{WindowRowNumber, "\"row_number\""},
		{WindowRank, "\"rank\""},
		{WindowDenseRank, "\"dense_rank\""},
		{WindowPercentRank, "\"percent_rank\""},
		{WindowCumeDist, "\"cume_dist\""},
		{WindowNtile, "\"ntile\""},
		{WindowLead, "\"lead\""},
		{WindowLag, "\"lag\""},
		{WindowFirstValue, "\"first_value\""},
		{WindowLastValue, "\"last_value\""},
		{WindowNthValue, "\"nth_value\""},
		{999, "\"ERROR\""},
	}

	for _, tc := range tt {
		t.Run(tc.opcode.String(), func(t *testing.T) {
			out, err := json.Marshal(tc.opcode)
			require.NoError(t, err, "Unexpected error")
			assert.Equal(t, tc.out, string(out))
		})
	}
}

func TestWindowNullable(t *testing.T) {
	tt := []struct {
		opcode   WindowOpcode
		nullable bool
	}{
		{WindowRowNumber, false},
		{WindowRank, false},
		{WindowDenseRank, false},
		{WindowPercentRank, false},
		{WindowCumeDist, false},
		{WindowNtile, false},
		{WindowLead, true},
		{WindowLag, true},
		{WindowFirstValue, true},
		{WindowLastValue, true},
		{WindowNthValue, true},
	}

	for _, tc := range tt {
		t.Run(tc.opcode.String(), func(t *testing.T) {
			assert.Equal(t, tc.nullable, tc.opcode.Nullable())
		})
	}
}
