/*
Copyright 2026 The Vitess Authors.

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

package sqlmode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

// The expected values in these tests were verified against MySQL 8.0.46.

func TestParse(t *testing.T) {
	tests := []struct {
		value       string
		expected    Mode
		expectedErr string
	}{
		{value: "", expected: 0},
		{value: ",", expected: 0},
		{value: "STRICT_TRANS_TABLES,NO_ZERO_DATE", expected: StrictTransTables | NoZeroDate},
		{value: "strict_trans_tables", expected: StrictTransTables},
		{value: "ANSI_QUOTES,,STRICT_ALL_TABLES", expected: AnsiQuotes | StrictAllTables},
		{value: "NOT_USED", expected: NotUsed},
		{value: "ANSI", expected: Ansi},
		{value: "TRADITIONAL", expected: Traditional},
		// trailing spaces of the whole value are ignored, other whitespace is not
		{value: " ", expected: 0},
		{value: "ANSI_QUOTES  ", expected: AnsiQuotes},
		{value: "STRICT_ALL_TABLES,ANSI_QUOTES  ", expected: StrictAllTables | AnsiQuotes},
		{value: " ANSI", expectedErr: "Variable 'sql_mode' can't be set to the value of ' ANSI'"},
		{value: "  ANSI_QUOTES", expectedErr: "Variable 'sql_mode' can't be set to the value of '  ANSI_QUOTES'"},
		{value: "ANSI_QUOTES  ,STRICT_ALL_TABLES", expectedErr: "Variable 'sql_mode' can't be set to the value of 'ANSI_QUOTES  '"},
		{value: "ansi_quotes, strict_all_tables", expectedErr: "Variable 'sql_mode' can't be set to the value of ' strict_all_tables'"},
		{value: "BOGUS", expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'"},
		{value: "1048576", expectedErr: "Variable 'sql_mode' can't be set to the value of '1048576'"},
		{value: "STRICT", expectedErr: "Variable 'sql_mode' can't be set to the value of 'STRICT'"},
		{value: "NO_AUTO_CREATE_USER", expectedErr: "Variable 'sql_mode' can't be set to the value of 'NO_AUTO_CREATE_USER'"},
		{value: "NOT_USED_9", expectedErr: "sql_mode=0x00000100 is not supported."},
		{value: "NOT_USED_29", expectedErr: "sql_mode=0x10000000 is not supported."},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			mode, err := Parse(tt.value)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode)
		})
	}
}

func TestFromBits(t *testing.T) {
	tests := []struct {
		bits        uint64
		expected    Mode
		expectedErr string
	}{
		{bits: 0, expected: 0},
		{bits: 16, expected: NotUsed},
		{bits: 262144, expected: Ansi},
		{bits: 2097152, expected: StrictTransTables},
		{bits: 4294967296, expected: TimeTruncateFractional},
		{bits: 8589934592, expectedErr: "Variable 'sql_mode' can't be set to the value of '8589934592'"},
		{bits: 256, expectedErr: "sql_mode=0x00000100 is not supported."},
		{bits: 268435456, expectedErr: "sql_mode=0x10000000 is not supported."},
		// only the removed bits are reported, matching MySQL
		{bits: 16384 + 2097152, expectedErr: "sql_mode=0x00004000 is not supported."},
		{bits: 8589934591, expectedErr: "sql_mode=0x1003ff00 is not supported."},
	}
	for _, tt := range tests {
		t.Run(sqltypes.NewUint64(tt.bits).String(), func(t *testing.T) {
			mode, err := FromBits(tt.bits)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode)
		})
	}
}

func TestFromValue(t *testing.T) {
	tests := []struct {
		value       sqltypes.Value
		expected    Mode
		expectedErr string
	}{
		{value: sqltypes.NewVarChar("STRICT_TRANS_TABLES"), expected: StrictTransTables},
		{value: sqltypes.NewInt64(2097152), expected: StrictTransTables},
		{value: sqltypes.NewUint64(1 << 20), expected: NoBackslashEscapes},
		{value: sqltypes.NewVarChar("2097152"), expectedErr: "Variable 'sql_mode' can't be set to the value of '2097152'"},
		{value: sqltypes.NewInt64(-1), expectedErr: "Variable 'sql_mode' can't be set to the value of '-1'"},
		{value: sqltypes.NewFloat64(1048576.5), expectedErr: "Incorrect argument type to variable 'sql_mode'"},
		{value: sqltypes.NewDecimal("1048576.0"), expectedErr: "Incorrect argument type to variable 'sql_mode'"},
		{value: sqltypes.NULL, expectedErr: "Variable 'sql_mode' can't be set to the value of 'NULL'"},
	}
	for _, tt := range tests {
		t.Run(tt.value.String(), func(t *testing.T) {
			mode, err := FromValue(tt.value)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode)
		})
	}
}

func TestExpand(t *testing.T) {
	assert.Equal(t, RealAsFloat|PipesAsConcat|AnsiQuotes|IgnoreSpace|OnlyFullGroupBy|Ansi, Ansi.Expand())
	assert.Equal(t,
		StrictTransTables|StrictAllTables|NoZeroInDate|NoZeroDate|ErrorForDivisionByZero|NoEngineSubstitution|Traditional,
		Traditional.Expand())
	assert.Equal(t, StrictTransTables|NoZeroDate, (StrictTransTables | NoZeroDate).Expand())
	// expanding is idempotent and members already present stay present
	assert.Equal(t, Ansi.Expand(), Ansi.Expand().Expand())
	assert.Equal(t, Ansi.Expand(), (Ansi | AnsiQuotes).Expand())
}

func TestString(t *testing.T) {
	assert.Empty(t, Mode(0).String())
	assert.Equal(t, "STRICT_TRANS_TABLES", StrictTransTables.String())
	// names are formatted in bit order regardless of how the mode was assembled
	assert.Equal(t, "STRICT_TRANS_TABLES,NO_ZERO_DATE", (NoZeroDate | StrictTransTables).String())
	assert.Equal(t,
		"REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI",
		Ansi.Expand().String())
	assert.Equal(t,
		"STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION",
		Traditional.Expand().String())
	// all valid bits set, as formatted by MySQL 8.0.46 for SET sql_mode = 0x1FFFFFFFF ^ 0x1003FF00
	all, err := FromBits(8321237247)
	require.NoError(t, err)
	assert.Equal(t,
		"REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,NOT_USED,ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,ANSI,NO_AUTO_VALUE_ON_ZERO,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH,TIME_TRUNCATE_FRACTIONAL",
		all.String())
}

func TestWithoutLexerModes(t *testing.T) {
	// modes that only affect runtime semantics pass through untouched
	runtime := StrictTransTables | NoZeroDate | OnlyFullGroupBy | NoEngineSubstitution
	assert.Equal(t, runtime, runtime.WithoutLexerModes())

	// modes that change how MySQL interprets SQL text are stripped
	assert.Equal(t, StrictTransTables, (StrictTransTables | IgnoreSpace).WithoutLexerModes())
	assert.Equal(t, StrictTransTables, (StrictTransTables | HighNotPrecedence).WithoutLexerModes())
	assert.Equal(t, Mode(0), (RealAsFloat | PipesAsConcat | AnsiQuotes | NoBackslashEscapes).WithoutLexerModes())

	// the expanded ANSI combination keeps its runtime member
	assert.Equal(t, OnlyFullGroupBy, Ansi.Expand().WithoutLexerModes())
}

func TestValidate(t *testing.T) {
	tests := []struct {
		value       sqltypes.Value
		expected    string
		expectedErr string
	}{
		{value: sqltypes.NewVarChar(""), expected: ""},
		{value: sqltypes.NewVarChar("no_zero_date,STRICT_TRANS_TABLES"), expected: "STRICT_TRANS_TABLES,NO_ZERO_DATE"},
		{value: sqltypes.NewVarChar("TRADITIONAL"), expected: "STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION"},
		{value: sqltypes.NewInt64(1 << 21), expected: "STRICT_TRANS_TABLES"},
		{value: sqltypes.NewInt64(0), expected: ""},
		// lexer modes are supported for incoming queries: they validate and expand,
		// while the transport toward backends still strips them
		{value: sqltypes.NewVarChar("ANSI"), expected: "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"},
		{value: sqltypes.NewInt64(1 << 18), expected: "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"},
		{value: sqltypes.NewVarChar("no_backslash_escapes"), expected: "NO_BACKSLASH_ESCAPES"},
		{value: sqltypes.NewUint64(1 << 20), expected: "NO_BACKSLASH_ESCAPES"},
		{value: sqltypes.NewVarChar("STRICT_TRANS_TABLES,ANSI_QUOTES"), expected: "ANSI_QUOTES,STRICT_TRANS_TABLES"},
		{value: sqltypes.NewVarChar("PIPES_AS_CONCAT"), expected: "PIPES_AS_CONCAT"},
		{value: sqltypes.NewVarChar("REAL_AS_FLOAT"), expected: "REAL_AS_FLOAT"},
		{value: sqltypes.NewVarChar("IGNORE_SPACE"), expected: "IGNORE_SPACE"},
		{value: sqltypes.NewVarChar("HIGH_NOT_PRECEDENCE"), expected: "HIGH_NOT_PRECEDENCE"},
		// invalid values fail with MySQL's own error messages
		{value: sqltypes.NewVarChar("BOGUS"), expectedErr: "Variable 'sql_mode' can't be set to the value of 'BOGUS'"},
		{value: sqltypes.NewInt64(256), expectedErr: "sql_mode=0x00000100 is not supported."},
	}
	for _, tt := range tests {
		t.Run(tt.value.String(), func(t *testing.T) {
			mode, err := Validate(tt.value)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode.String())
		})
	}
}

func TestValidateNoLexerModes(t *testing.T) {
	// the check covers exactly the LexerModes, reporting combination modes under their
	// own name, and passes everything else
	for _, mn := range modeNames {
		err := ValidateNoLexerModes(mn.mode)
		if LexerModes&mn.mode != 0 {
			require.EqualError(t, err, "setting the "+mn.name+" sql_mode is unsupported")
		} else {
			require.NoError(t, err, "mode %s", mn.name)
		}
	}
	require.EqualError(t, ValidateNoLexerModes(Ansi), "setting the ANSI sql_mode is unsupported")
	require.EqualError(t, ValidateNoLexerModes(StrictTransTables|IgnoreSpace), "setting the IGNORE_SPACE sql_mode is unsupported")
	require.NoError(t, ValidateNoLexerModes(StrictTransTables|NoZeroDate))
}

func TestNeutralizeSessionQuery(t *testing.T) {
	// the exact statement was verified against MySQL 8.0.46: a global of
	// 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES,IGNORE_SPACE,HIGH_NOT_PRECEDENCE,PIPES_AS_CONCAT,
	// REAL_AS_FLOAT,STRICT_TRANS_TABLES,NO_ZERO_DATE' neutralizes to
	// 'STRICT_TRANS_TABLES,NO_ZERO_DATE', and a global containing the expanded ANSI
	// combination keeps ONLY_FULL_GROUP_BY — matching Expand().WithoutLexerModes()
	assert.Equal(t,
		"set @@session.sql_mode = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@@global.sql_mode, 'NO_BACKSLASH_ESCAPES', ''), 'HIGH_NOT_PRECEDENCE', ''), 'PIPES_AS_CONCAT', ''), 'REAL_AS_FLOAT', ''), 'IGNORE_SPACE', ''), 'ANSI_QUOTES', ''), 'ANSI', '')",
		NeutralizeSessionQuery)

	// every LexerModes member name is stripped
	for _, mn := range modeNames {
		if LexerModes&mn.mode != 0 {
			assert.Contains(t, NeutralizedGlobalExpr, "'"+mn.name+"'")
		}
	}
}
