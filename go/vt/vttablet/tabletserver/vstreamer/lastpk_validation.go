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

// validateLastPKShape checks that a lastpk QueryResult a client sent us can be
// decoded. sqltypes.Proto3ToResult builds the row with MakeRowTrusted, which
// indexes the fields with each of the row's lengths and slices the row's values
// by them without checking either, so a malformed QueryResult would panic
// instead of failing.
func validateLastPKShape(qr *querypb.QueryResult) error {
	if len(qr.Rows) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "lastpk has %d rows, expected 1", len(qr.Rows))
	}
	row := qr.Rows[0]
	if row == nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "lastpk row is nil")
	}
	if len(row.Lengths) != len(qr.Fields) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"lastpk row has %d values, but there are %d fields", len(row.Lengths), len(qr.Fields))
	}
	for i, field := range qr.Fields {
		if field == nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "lastpk field %d is nil", i)
		}
	}
	// A length of -1 is a NULL value, which takes no bytes.
	remaining := int64(len(row.Values))
	for i, length := range row.Lengths {
		if length < -1 {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "lastpk value %d has an invalid length %d", i, length)
		}
		if length > remaining {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"lastpk value %d has length %d, but only %d bytes of values remain", i, length, remaining)
		}
		if length > 0 {
			remaining -= length
		}
	}
	return nil
}

// validateLastPK checks the lastpk values a client sent us and returns them
// typed as the table's own primary key columns, ready to be written into the
// copy-phase snapshot query with writeLastPKValue.
//
// A lastpk arrives as a querypb.QueryResult on the VStream request, and
// sqltypes.MakeRowTrusted builds each value from the type the client declared
// together with the client's raw bytes; it does not check one against the other.
// A lastpk is a resume token that Vitess hands out with each column's own type,
// so a value of another type is a bug or an attack, and is rejected rather than
// coerced; only a number declared with another numeric type is accepted. The
// values are then retyped from the table, so that how one is written into the
// statement never depends on a type the client chose.
//
// lastpk is positional: lastpk[i] belongs to the column named by pkColumns[i],
// which is how buildSelect pairs them.
func validateLastPK(lastpk []sqltypes.Value, fields []*querypb.Field, pkColumns []int) ([]sqltypes.Value, error) {
	values := make([]sqltypes.Value, len(pkColumns))
	for i, pkCol := range pkColumns {
		if i >= len(lastpk) {
			// The caller checks the arity; stop rather than index out of range.
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"lastpk has %d values, fewer than the %d primary key columns", len(lastpk), len(pkColumns))
		}
		if pkCol < 0 || pkCol >= len(fields) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"primary key column index %d is out of range for a table with %d columns", pkCol, len(fields))
		}
		value, err := validateLastPKValue(lastpk[i], fields[pkCol])
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"invalid lastpk value for column %s: %v", fields[pkCol].Name, err)
		}
		values[i] = value
	}
	return values, nil
}

// validateLastPKValue checks one lastpk value against the column it will be
// compared with, and returns it typed as that column.
func validateLastPKValue(v sqltypes.Value, field *querypb.Field) (sqltypes.Value, error) {
	if v.IsNull() {
		// The null literal cannot carry a payload.
		return v, nil
	}
	// A number may be declared with another numeric type, as a client building
	// its own lastpk may use a 64-bit integer for any integer column: it is
	// parsed again as the column's type below, so the declared type decides
	// nothing.
	if v.Type() != field.Type && !(sqltypes.IsNumber(v.Type()) && sqltypes.IsNumber(field.Type)) {
		return sqltypes.Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"declared type %v does not match the column type %v", v.Type(), field.Type)
	}
	// NewValue parses the bytes for the column's type and rejects them when they
	// do not fit.
	value, err := sqltypes.NewValue(field.Type, v.Raw())
	if err != nil {
		return sqltypes.Value{}, err
	}
	// writeLastPKValue writes a number verbatim, so its bytes have to be a
	// literal on their own. NewValue keeps the bytes it was given and, for an
	// integral or a float, tolerates the space and tab padding that fastparse
	// skips, which a lastpk value has no reason to carry. The check is stricter
	// than NewValue's own, so it stays here.
	if sqltypes.IsNumber(value.Type()) && !sqltypes.IsNumericLiteral(value.Raw(), true) {
		return sqltypes.Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"%q is not a numeric literal", value.Raw())
	}
	return value, nil
}

// writeLastPKValue writes a value returned by validateLastPK into a statement
// as a literal.
//
// Only a number, which validateLastPK has checked is a plain literal, is written
// verbatim. Every other type is quoted, so that a type this switch does not
// know about cannot reach the statement unquoted. Value.EncodeSQL is not used
// for quoting: it escapes a quote as \', which stops being an escape under
// NO_BACKSLASH_ESCAPES, so the literal would end early and the rest of the value
// would be read as SQL, and a UNION spliced into the copy-phase query needs no
// statement separator at all. The row streamer's connection cannot be in that
// mode, as dbconfigs.Connector.Connect runs sqlmode.NeutralizeSessionQuery, but
// writeQuotedLiteral stays inside the literal without relying on that.
func writeLastPKValue(buf *sqlparser.TrackedBuffer, v sqltypes.Value) {
	switch {
	case v.IsNull(), v.Type() == sqltypes.Bit:
		// The null literal and b'0101', neither of which can carry a payload.
		v.EncodeSQL(buf)
	case sqltypes.IsNumber(v.Type()):
		buf.Write(v.Raw())
	case v.IsBinary():
		// Keep the introducer so the comparison keeps its binary semantics.
		buf.WriteString("_binary")
		writeQuotedLiteral(buf, v.Raw())
	default:
		writeQuotedLiteral(buf, v.Raw())
	}
}

// writeQuotedLiteral writes val as a single-quoted literal, doubling both the
// quote and the backslash.
//
// Doubling the quote is the escape SQL itself defines, and it holds under every
// sql_mode, so the literal can never end early. The backslash is doubled so that
// it stays inert where backslashes are escapes, which they always are on a
// connection that sqlmode.NeutralizeSessionQuery has set up, and the literal
// then reads back exactly. Under NO_BACKSLASH_ESCAPES a value containing a
// backslash would read back with that backslash doubled: a wrong resume bound
// for such a key, but never a way out of the literal.
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
