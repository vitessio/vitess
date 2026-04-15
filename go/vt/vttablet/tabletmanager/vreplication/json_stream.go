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

package vreplication

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	vjson "vitess.io/vitess/go/mysql/json"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// appendStreamJSONForSQL converts text JSON into a SQL expression using
// JSON_OBJECT/JSON_ARRAY syntax, writing directly to buf without building
// an intermediate tree. This has O(recursion depth) memory overhead versus
// O(total nodes * 72 bytes) for the tree-based vjson.MarshalSQLValue.
//
// The output format matches the tree-based encoder so that MySQL stores
// identical binary JSON, including preservation of large integer precision
// via bare numeric literals.
func appendStreamJSONForSQL(buf *bytes2.Buffer, raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := streamMarshalValue(dec, buf, true, 0); err != nil {
		return err
	}
	// Require EOF so we match vjson.ParseBytes behavior. dec.More() is not
	// sufficient — it only checks for ] or }, so inputs like "1]" slip through.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing data after JSON value")
	}
	return nil
}

func streamMarshalValue(dec *json.Decoder, buf *bytes2.Buffer, top bool, depth int) error {
	if depth > vjson.MaxDepth {
		return fmt.Errorf("too big depth for the nested JSON; it exceeds %d", vjson.MaxDepth)
	}
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("reading JSON token: %w", err)
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			return streamMarshalObject(dec, buf, depth)
		case '[':
			return streamMarshalArray(dec, buf, depth)
		default:
			return fmt.Errorf("unexpected JSON delimiter: %v", v)
		}
	case string:
		streamMarshalString(buf, v, top)
		return nil
	case json.Number:
		streamMarshalNumber(buf, v.String(), top)
		return nil
	case bool:
		streamMarshalBool(buf, v, top)
		return nil
	case nil:
		streamMarshalNull(buf, top)
		return nil
	default:
		return fmt.Errorf("unexpected JSON token type: %T", tok)
	}
}

func streamMarshalObject(dec *json.Decoder, buf *bytes2.Buffer, depth int) error {
	buf.WriteString("JSON_OBJECT(")
	first := true
	for dec.More() {
		if !first {
			buf.WriteString(", ")
		}
		first = false

		// Key (always a string).
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("reading JSON object key: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return fmt.Errorf("expected string key in JSON object, got %T", keyTok)
		}
		buf.WriteString("_utf8mb4")
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(key)).EncodeSQLBytes2(buf)
		buf.WriteString(", ")

		// Value.
		if err := streamMarshalValue(dec, buf, false, depth+1); err != nil {
			return err
		}
	}
	// Consume the closing '}'.
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("reading JSON object closing brace: %w", err)
	}
	buf.WriteByte(')')
	return nil
}

func streamMarshalArray(dec *json.Decoder, buf *bytes2.Buffer, depth int) error {
	buf.WriteString("JSON_ARRAY(")
	first := true
	for dec.More() {
		if !first {
			buf.WriteString(", ")
		}
		first = false

		if err := streamMarshalValue(dec, buf, false, depth+1); err != nil {
			return err
		}
	}
	// Consume the closing ']'.
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("reading JSON array closing bracket: %w", err)
	}
	buf.WriteByte(')')
	return nil
}

func streamMarshalString(buf *bytes2.Buffer, s string, top bool) {
	if top {
		buf.WriteString("CAST(JSON_QUOTE(")
	}
	buf.WriteString("_utf8mb4")
	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(s)).EncodeSQLBytes2(buf)
	if top {
		buf.WriteString(") as JSON)")
	}
}

func streamMarshalNumber(buf *bytes2.Buffer, num string, top bool) {
	if top {
		buf.WriteString("CAST(")
	}
	buf.WriteString(num)
	if top {
		buf.WriteString(" as JSON)")
	}
}

func streamMarshalBool(buf *bytes2.Buffer, v bool, top bool) {
	if top {
		buf.WriteString("CAST(_utf8mb4'")
	}
	if v {
		buf.WriteString("true")
	} else {
		buf.WriteString("false")
	}
	if top {
		buf.WriteString("' as JSON)")
	}
}

func streamMarshalNull(buf *bytes2.Buffer, top bool) {
	if top {
		buf.WriteString("CAST(_utf8mb4'")
	}
	buf.WriteString("null")
	if top {
		buf.WriteString("' as JSON)")
	}
}
