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

package vtgate

import (
	"unicode/utf8"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// handshakeCharset returns the charset a MySQL protocol client asked for in
// its handshake, as a session stores it: empty for utf8mb4.
func handshakeCharset(env *collations.Environment, c *mysql.Conn) string {
	cs := env.LookupCharsetName(c.CharacterSet)
	if cs == "utf8mb4" {
		return ""
	}
	return cs
}

// resultCharset returns the charset that MySQL sends result set metadata in
// to a client with the given session, or nil when the metadata is sent as
// vtgate has it, in UTF-8.
func resultCharset(env *collations.Environment, session *vtgatepb.Session) charset.Charset {
	switch session.GetCharacterSetResults() {
	case "", "utf8mb3", "binary":
		// Names are valid utf8mb3, which utf8mb4 clients read as is.
		return nil
	case "ucs2", "utf16", "utf16le", "utf32":
		// MySQL does not accept these for character_set_results.
		return nil
	}
	coll := colldata.Lookup(env.DefaultCollationForCharset(session.GetCharacterSetResults()))
	if coll == nil {
		return nil
	}
	return coll.Charset()
}

// encodeFieldNames converts the metadata of result fields to the charset
// that MySQL would send them in, as MySQL does before it writes them to the
// client. It never changes fields in place: it returns a new slice when a
// name changes, and reports whether one did.
func encodeFieldNames(cs charset.Charset, fields []*querypb.Field) ([]*querypb.Field, bool) {
	if cs == nil {
		return fields, false
	}
	var encoded []*querypb.Field
	// buf holds each converted name until it is copied to a string.
	var buf []byte
	for i, field := range fields {
		// Every charset that character_set_results accepts encodes ASCII
		// as ASCII, and names are nearly always ASCII.
		if isASCII(field.Name) && isASCII(field.OrgName) && isASCII(field.Table) && isASCII(field.OrgTable) && isASCII(field.Database) {
			continue
		}
		var nameOK, orgNameOK, tableOK, orgTableOK, databaseOK bool
		var name, orgName, table, orgTable, database string
		name, nameOK, buf = encodeName(cs, field.Name, buf)
		orgName, orgNameOK, buf = encodeName(cs, field.OrgName, buf)
		table, tableOK, buf = encodeName(cs, field.Table, buf)
		orgTable, orgTableOK, buf = encodeName(cs, field.OrgTable, buf)
		database, databaseOK, buf = encodeName(cs, field.Database, buf)
		if nameOK && orgNameOK && tableOK && orgTableOK && databaseOK {
			continue
		}
		if encoded == nil {
			encoded = make([]*querypb.Field, len(fields))
			copy(encoded, fields)
		}
		field = field.CloneVT()
		field.Name, field.OrgName, field.Table, field.OrgTable, field.Database = name, orgName, table, orgTable, database
		encoded[i] = field
	}
	if encoded == nil {
		return fields, false
	}
	return encoded, true
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// encodeName converts a UTF-8 name to the given charset, replacing the
// characters the charset cannot represent with '?'. It reports whether the
// name is unchanged. buf is scratch space for the conversion: encodeName
// returns it, possibly grown, for the next call to reuse.
func encodeName(cs charset.Charset, name string, buf []byte) (string, bool, []byte) {
	if isASCII(name) {
		return name, true, buf
	}
	if buf == nil {
		// The conversion needs room for at least one character.
		buf = make([]byte, 0, max(64, 2*len(name)))
	}
	// A character that the charset cannot represent becomes '?', and is
	// reported as an error, which MySQL ignores too. The conversion only
	// reads the name.
	out, _ := charset.ConvertFromUTF8(buf[:0], cs, hack.StringBytes(name))
	if string(out) == name {
		return name, true, out
	}
	return string(out), false, out
}

// encodeResult returns the result with its field metadata in the charset
// that MySQL would send it in.
func encodeResult(cs charset.Charset, qr *sqltypes.Result) *sqltypes.Result {
	if cs == nil || qr == nil || len(qr.Fields) == 0 {
		return qr
	}
	fields, changed := encodeFieldNames(cs, qr.Fields)
	if !changed {
		return qr
	}
	encoded := *qr
	encoded.Fields = fields
	return &encoded
}
