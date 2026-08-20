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

// Package sqlmode implements MySQL's sql_mode value semantics: parsing mode name lists
// and numeric bitmasks, expansion of combination modes, and canonical formatting. The
// mode table and validation behavior follow MySQL 8.x, verified against live servers.
package sqlmode

import (
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Mode is a MySQL sql_mode bitmask. The bit positions match MySQL's numeric sql_mode
// representation, the same values accepted by SET sql_mode = <number>.
type Mode uint64

const (
	RealAsFloat            Mode = 1 << 0
	PipesAsConcat          Mode = 1 << 1
	AnsiQuotes             Mode = 1 << 2
	IgnoreSpace            Mode = 1 << 3
	NotUsed                Mode = 1 << 4
	OnlyFullGroupBy        Mode = 1 << 5
	NoUnsignedSubtraction  Mode = 1 << 6
	NoDirInCreate          Mode = 1 << 7
	Ansi                   Mode = 1 << 18
	NoAutoValueOnZero      Mode = 1 << 19
	NoBackslashEscapes     Mode = 1 << 20
	StrictTransTables      Mode = 1 << 21
	StrictAllTables        Mode = 1 << 22
	NoZeroInDate           Mode = 1 << 23
	NoZeroDate             Mode = 1 << 24
	AllowInvalidDates      Mode = 1 << 25
	ErrorForDivisionByZero Mode = 1 << 26
	Traditional            Mode = 1 << 27
	HighNotPrecedence      Mode = 1 << 29
	NoEngineSubstitution   Mode = 1 << 30
	PadCharToFullLength    Mode = 1 << 31
	TimeTruncateFractional Mode = 1 << 32
)

// maxMode is the largest valid sql_mode bitmask: all 33 set members.
const maxMode Mode = 1<<33 - 1

// removedModes are the bits of modes removed in MySQL 8.0 (the old DB2/MAXDB/MSSQL/...
// compatibility modes at bits 8-17 and NO_AUTO_CREATE_USER at bit 28). MySQL still parses
// their placeholder names but rejects the bits with ER_UNSUPPORTED_SQL_MODE.
const removedModes Mode = 0x1003FF00

// LexerModes are the modes that change how MySQL interprets SQL text: quoting and escaping
// (ANSI_QUOTES, NO_BACKSLASH_ESCAPES), operator meaning and precedence (PIPES_AS_CONCAT,
// HIGH_NOT_PRECEDENCE), type name aliasing (REAL_AS_FLOAT), and reserved function names
// (IGNORE_SPACE); ANSI is the combination mode enabling several of them. These modes only
// concern the interpretation of the *client's* SQL: queries vtgate sends to a backend are
// always serialized in vtgate's canonical, default-lexer format, so a session's lexer
// modes are stripped from the sql_mode transported to backends. That split is what allows
// supporting these modes for incoming queries without backend query serialization ever
// having to account for them.
const LexerModes = RealAsFloat | PipesAsConcat | AnsiQuotes | IgnoreSpace | Ansi | NoBackslashEscapes | HighNotPrecedence

// WithoutLexerModes returns the mode with all LexerModes removed — the value safe to send
// to a backend alongside vtgate's canonically-formatted queries.
func (m Mode) WithoutLexerModes() Mode {
	return m &^ LexerModes
}

// neutralizedExpr builds a MySQL expression that evaluates to the given sql_mode
// source with all LexerModes member names stripped. It is built with nested REPLACE
// calls because MySQL offers no numeric arithmetic on the @@sql_mode system variable
// (string-to-number coercion of the SET value yields 0). Names are stripped longest
// first so that no earlier replacement can mangle a longer name it is a substring of
// (ANSI within ANSI_QUOTES); MySQL ignores the empty list members REPLACE leaves behind.
func neutralizedExpr(source string) string {
	var names []string
	for _, mn := range modeNames {
		if LexerModes&mn.mode != 0 {
			names = append(names, mn.name)
		}
	}
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	expr := source
	for _, name := range names {
		expr = fmt.Sprintf("REPLACE(%s, '%s', '')", expr, name)
	}
	return expr
}

// NeutralizedGlobalExpr evaluates to the server's global sql_mode with all LexerModes
// member names stripped. The settings-pool reset restores it in place of `default`,
// matching MySQL's semantics for `SET sql_mode = default` — verified against MySQL
// 8.0.46: `default` resolves to the global value current at the time of the SET, and
// it discards any adjustments the server's connection initialization (init_connect)
// made to the session, so the global is the right source here even though the
// connection-setup neutralization sources from the session.
var NeutralizedGlobalExpr = neutralizedExpr("@@global.sql_mode")

// NeutralizeSessionQuery sets the connection's session sql_mode to the session's
// current value with LexerModes stripped. Vitess runs it on every connection it
// creates for its own SQL: MySQL lexes each statement under the session mode in effect
// before the statement — a SET_VAR hint cannot influence the parsing of its own
// statement — so a statement must always be lexed under the same default rules it was
// serialized with, regardless of the server's configuration. Runtime modes are
// preserved. The session, not the global value, is the source: at connection setup the
// session has inherited the global value, except where the server's own connection
// initialization (init_connect) already adjusted it — those adjustments run before this
// statement and their runtime modes must survive it. Verified against MySQL 8.0.46,
// and the guarantee is scoped to MySQL: on MariaDB — a deprecated migration source,
// not a supported serving backend — the statement executes but MariaDB-only
// combination modes that imply lexer behavior (ORACLE, MSSQL, POSTGRESQL, DB2, MAXDB)
// are not stripped and would re-enable their members on assignment, and MariaDB's
// tolerance of the empty list members REPLACE leaves behind is unverified.
var NeutralizeSessionQuery = "set @@session.sql_mode = " + neutralizedExpr("@@session.sql_mode")

// modeNames lists all sql_mode set members in MySQL's numeric bit order. The
// NOT_USED_* placeholders parse to their bit like in MySQL, where validation
// then rejects them as removed modes.
var modeNames = []struct {
	mode Mode
	name string
}{
	{RealAsFloat, "REAL_AS_FLOAT"},
	{PipesAsConcat, "PIPES_AS_CONCAT"},
	{AnsiQuotes, "ANSI_QUOTES"},
	{IgnoreSpace, "IGNORE_SPACE"},
	{NotUsed, "NOT_USED"},
	{OnlyFullGroupBy, "ONLY_FULL_GROUP_BY"},
	{NoUnsignedSubtraction, "NO_UNSIGNED_SUBTRACTION"},
	{NoDirInCreate, "NO_DIR_IN_CREATE"},
	{1 << 8, "NOT_USED_9"},
	{1 << 9, "NOT_USED_10"},
	{1 << 10, "NOT_USED_11"},
	{1 << 11, "NOT_USED_12"},
	{1 << 12, "NOT_USED_13"},
	{1 << 13, "NOT_USED_14"},
	{1 << 14, "NOT_USED_15"},
	{1 << 15, "NOT_USED_16"},
	{1 << 16, "NOT_USED_17"},
	{1 << 17, "NOT_USED_18"},
	{Ansi, "ANSI"},
	{NoAutoValueOnZero, "NO_AUTO_VALUE_ON_ZERO"},
	{NoBackslashEscapes, "NO_BACKSLASH_ESCAPES"},
	{StrictTransTables, "STRICT_TRANS_TABLES"},
	{StrictAllTables, "STRICT_ALL_TABLES"},
	{NoZeroInDate, "NO_ZERO_IN_DATE"},
	{NoZeroDate, "NO_ZERO_DATE"},
	{AllowInvalidDates, "ALLOW_INVALID_DATES"},
	{ErrorForDivisionByZero, "ERROR_FOR_DIVISION_BY_ZERO"},
	{Traditional, "TRADITIONAL"},
	{1 << 28, "NOT_USED_29"},
	{HighNotPrecedence, "HIGH_NOT_PRECEDENCE"},
	{NoEngineSubstitution, "NO_ENGINE_SUBSTITUTION"},
	{PadCharToFullLength, "PAD_CHAR_TO_FULL_LENGTH"},
	{TimeTruncateFractional, "TIME_TRUNCATE_FRACTIONAL"},
}

var namesToMode = func() map[string]Mode {
	m := make(map[string]Mode, len(modeNames))
	for _, mn := range modeNames {
		m[mn.name] = mn.mode
	}
	return m
}()

// expansions maps the combination modes to the member modes they enable.
var expansions = map[Mode]Mode{
	Ansi:        RealAsFloat | PipesAsConcat | AnsiQuotes | IgnoreSpace | OnlyFullGroupBy,
	Traditional: StrictTransTables | StrictAllTables | NoZeroInDate | NoZeroDate | ErrorForDivisionByZero | NoEngineSubstitution,
}

// Parse parses a comma-separated list of sql_mode names into a Mode, following MySQL's
// semantics: trailing spaces of the whole value are ignored, names match
// case-insensitively and in full, empty list elements are ignored, and whitespace inside
// the list is not trimmed. Unknown names fail with ER_WRONG_VALUE_FOR_VAR and removed
// modes with ER_UNSUPPORTED_SQL_MODE, like in MySQL.
func Parse(value string) (Mode, error) {
	var mode Mode
	for part := range strings.SplitSeq(strings.TrimRight(value, " "), ",") {
		if part == "" {
			continue
		}
		bit, ok := namesToMode[strings.ToUpper(part)]
		if !ok {
			return 0, wrongValueError(part)
		}
		mode |= bit
	}
	return mode, checkRemoved(mode)
}

// FromBits validates a numeric sql_mode value and converts it to a Mode, following
// MySQL's semantics: values beyond the defined set members fail with
// ER_WRONG_VALUE_FOR_VAR and removed modes with ER_UNSUPPORTED_SQL_MODE.
func FromBits(bits uint64) (Mode, error) {
	if bits > uint64(maxMode) {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "Variable 'sql_mode' can't be set to the value of '%d'", bits)
	}
	mode := Mode(bits)
	return mode, checkRemoved(mode)
}

// FromValue validates an evaluated sql_mode assignment value, following MySQL's
// semantics: integral values are treated as a bitmask, strings as a list of mode names,
// and other types are rejected the way MySQL rejects them.
func FromValue(value sqltypes.Value) (Mode, error) {
	switch {
	case value.IsIntegral():
		bits, err := value.ToUint64()
		if err != nil {
			return 0, wrongValueError(value.ToString())
		}
		return FromBits(bits)
	case value.IsFloat(), value.IsDecimal():
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "Incorrect argument type to variable 'sql_mode'")
	case value.IsNull():
		return 0, wrongValueError("NULL")
	default:
		return Parse(value.ToString())
	}
}

// Expand returns the mode with all combination modes (ANSI, TRADITIONAL) expanded into
// their members, keeping the combination bits set like MySQL does.
func (m Mode) Expand() Mode {
	expanded := m
	for combination, members := range expansions {
		if m&combination != 0 {
			expanded |= members
		}
	}
	return expanded
}

// String formats the mode the way MySQL formats @@sql_mode: the names of all set bits,
// in bit order, comma-separated.
func (m Mode) String() string {
	var buf strings.Builder
	for _, mn := range modeNames {
		if m&mn.mode == 0 {
			continue
		}
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(mn.name)
	}
	return buf.String()
}

// Validate parses and validates an sql_mode assignment value the way MySQL does. It
// returns the expanded mode, whose String form is the canonical value MySQL would report
// back for @@sql_mode. Both vtgate (SET statements, the --sql-mode flag) and vttablet
// (settings, SET_VAR hints, SET statements) validate with this, so the same value fails
// with the same error at either layer. Every valid mode is accepted — the layer that
// parses SQL honors the LexerModes itself and transports values with them stripped.
func Validate(value sqltypes.Value) (Mode, error) {
	mode, err := FromValue(value)
	if err != nil {
		return 0, err
	}
	return mode.Expand(), nil
}

// lexerModeList lists the LexerModes members in reporting order, with the ANSI
// combination first so it is reported under its own name rather than that of one of its
// members.
var lexerModeList = []Mode{
	Ansi,
	AnsiQuotes,
	NoBackslashEscapes,
	PipesAsConcat,
	RealAsFloat,
	IgnoreSpace,
	HighNotPrecedence,
}

// ValidateNoLexerModes rejects modes that change how SQL text is interpreted. It is for
// call sites that run Vitess-formatted SQL under a caller-provided sql_mode with no
// parser involved — e.g. schema migration session variables — where such a mode cannot
// be honored and would change what the statements mean.
func ValidateNoLexerModes(mode Mode) error {
	expanded := mode.Expand()
	for _, lexerMode := range lexerModeList {
		if expanded&lexerMode != 0 {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "setting the %s sql_mode is unsupported", lexerMode)
		}
	}
	return nil
}

func checkRemoved(mode Mode) error {
	removed := mode & removedModes
	if removed == 0 {
		return nil
	}
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.UnsupportedSQLMode, "sql_mode=0x%08x is not supported.", uint64(removed))
}

func wrongValueError(value string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "Variable 'sql_mode' can't be set to the value of '%s'", value)
}
