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

package vstreamer

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// validateLastPK checks the lastpk values a client sent us before they are
// written into the copy-phase snapshot query.
//
// A lastpk arrives as a querypb.QueryResult on the VStream request, and
// sqltypes.MakeRowTrusted builds each value from the type the client declared
// together with the client's raw bytes; it does not check one against the other.
// buildSelect then writes those values into the WHERE clause with
// Value.EncodeSQL, which quotes and escapes only the Null, binary, quoted and
// Bit types. For anything else it writes the bytes verbatim, so a client that
// declares a numeric type can put arbitrary SQL into the statement.
//
// lastpk is positional: lastpk[i] belongs to the column named by pkColumns[i],
// which is how buildSelect pairs them.
func validateLastPK(lastpk []sqltypes.Value, fields []*querypb.Field, pkColumns []int) error {
	for i, pkCol := range pkColumns {
		if i >= len(lastpk) {
			// The caller checks the arity; stop rather than index out of range.
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"lastpk has %d values, fewer than the %d primary key columns", len(lastpk), len(pkColumns))
		}
		if pkCol < 0 || pkCol >= len(fields) {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"primary key column index %d is out of range for a table with %d columns", pkCol, len(fields))
		}
		if err := validateLastPKValue(lastpk[i], fields[pkCol]); err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"invalid lastpk value for column %s: %v", fields[pkCol].Name, err)
		}
	}
	return nil
}

// validateLastPKValue checks one lastpk value against the column it will be
// compared with.
func validateLastPKValue(v sqltypes.Value, field *querypb.Field) error {
	if v.IsNull() {
		// EncodeSQL writes the null literal, which cannot carry a payload.
		return nil
	}

	// The value has to belong to the column, not merely to the type the client
	// declared for it. NewValue parses the bytes for the column's own type and
	// rejects them when they do not fit.
	if _, err := sqltypes.NewValue(field.Type, v.Raw()); err != nil {
		return err
	}

	// The declared type, not the column type, is what EncodeSQL switches on, so
	// the two have to agree about whether the value gets quoted. Otherwise a
	// client could declare a numeric type for a textual column and have its
	// bytes written verbatim.
	if isEscapedByEncodeSQL(v.Type()) != isEscapedByEncodeSQL(field.Type) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"declared type %v does not match the column type %v", v.Type(), field.Type)
	}

	if isEscapedByEncodeSQL(v.Type()) {
		// EncodeSQL quotes and escapes it, so the bytes cannot end the literal.
		return nil
	}

	// EncodeSQL writes this type verbatim, so the bytes have to be a literal on
	// their own. Parsing alone is not enough: decimal.NewFromMySQL stops
	// scanning once the integral part exceeds MySQL's precision, and
	// fastparse accepts Go's NaN and Inf words, so a payload can parse and still
	// carry a tail.
	if !isPlainNumericLiteral(v.Raw()) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"%q is not a numeric literal", v.Raw())
	}
	return nil
}

// isEscapedByEncodeSQL reports whether Value.EncodeSQL quotes and escapes a
// value of this type rather than writing its bytes verbatim. It mirrors the
// switch in Value.EncodeSQL; keep the two in step.
func isEscapedByEncodeSQL(typ querypb.Type) bool {
	return typ == sqltypes.Null ||
		sqltypes.IsBinary(typ) ||
		sqltypes.IsQuoted(typ) ||
		typ == sqltypes.Bit
}

// isPlainNumericLiteral reports whether val is exactly
// [+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)? and so is safe to write
// into a statement without quoting. It rejects the shapes Go's parsers accept
// but MySQL has no literal for, such as NaN, Inf and surrounding whitespace.
func isPlainNumericLiteral(val []byte) bool {
	i := 0
	if i < len(val) && (val[i] == '+' || val[i] == '-') {
		i++
	}

	mantissa := 0
	for i < len(val) && isDigit(val[i]) {
		i++
		mantissa++
	}
	if i < len(val) && val[i] == '.' {
		i++
		for i < len(val) && isDigit(val[i]) {
			i++
			mantissa++
		}
	}
	if mantissa == 0 {
		return false
	}

	if i < len(val) && (val[i] == 'e' || val[i] == 'E') {
		i++
		if i < len(val) && (val[i] == '+' || val[i] == '-') {
			i++
		}
		exponent := 0
		for i < len(val) && isDigit(val[i]) {
			i++
			exponent++
		}
		if exponent == 0 {
			return false
		}
	}

	return i == len(val)
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// writeLastPKValue writes a validated lastpk value into a statement as a literal
// that means the same thing whether or not the server's sql_mode includes
// NO_BACKSLASH_ESCAPES.
//
// Value.EncodeSQL cannot be used for the quoted and binary types: it escapes a
// quote as \', which stops being an escape under that mode, so the literal ends
// early and the rest of the value is read as SQL. VerifyMode only requires that
// a strict mode be present, so a tablet may be running with it set, and a UNION
// spliced into the copy-phase query needs no statement separator at all.
func writeLastPKValue(buf *sqlparser.TrackedBuffer, v sqltypes.Value) {
	switch {
	case v.IsBinary():
		// Keep the introducer so the comparison keeps its binary semantics.
		buf.WriteString("_binary")
		writeQuotedLiteral(buf, v.Raw())
	case isEscapedByEncodeSQL(v.Type()) && v.Type() != sqltypes.Bit:
		writeQuotedLiteral(buf, v.Raw())
	default:
		// Null writes the null literal and Bit writes b'0101', neither of which
		// can carry a payload. Everything else has been validated as a plain
		// numeric literal, so writing it verbatim is safe.
		v.EncodeSQL(buf)
	}
}

// writeQuotedLiteral writes val as a single-quoted literal, doubling both the
// quote and the backslash.
//
// Doubling the quote is the escape SQL itself defines, and it holds under every
// sql_mode, so the literal can never end early. The backslash is doubled so that
// it stays inert where backslashes *are* escapes. Under NO_BACKSLASH_ESCAPES a
// value containing a backslash therefore reads back with that backslash doubled:
// a wrong resume bound for such a key, but never a way out of the literal.
func writeQuotedLiteral(buf *sqlparser.TrackedBuffer, val []byte) {
	buf.WriteByte('\'')
	for _, c := range val {
		switch c {
		case '\'':
			buf.WriteString("''")
		case '\\':
			buf.WriteString(`\\`)
		default:
			buf.WriteByte(c)
		}
	}
	buf.WriteByte('\'')
}
