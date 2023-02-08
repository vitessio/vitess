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
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
)

type errJsonType string

func (fn errJsonType) Error() string {
	return fmt.Sprintf("Invalid data type for JSON data to function %s; a JSON string or JSON type is required.", string(fn))
}

var errJsonPath = errors.New("Invalid JSON path expression.")

type evalJson = json.Value

var _ eval = (*evalJson)(nil)

func intoJson(fn string, e eval) (*evalJson, error) {
	switch e := e.(type) {
	case *evalJson:
		return e, nil
	case *evalBytes:
		var p json.Parser
		return p.ParseBytes(e.bytes)
	default:
		return nil, errJsonType(fn)
	}
}

func intoJsonPath(e eval) (*json.Path, error) {
	switch e := e.(type) {
	case *evalBytes:
		var p json.PathParser
		return p.ParseBytes(e.bytes)
	default:
		return nil, errJsonPath
	}
}

func evalBinaryToJson(e *evalBytes) *evalJson {
	const prefix = "base64:type15:"

	dst := make([]byte, len(prefix)+mysqlBase64.EncodedLen(len(e.bytes)))
	copy(dst, prefix)
	base64.StdEncoding.Encode(dst[len(prefix):], e.bytes)
	return json.NewString(dst)
}

func evalToJson(e eval) (*evalJson, error) {
	switch e := e.(type) {
	case nil:
		return json.ValueNull, nil
	case *evalJson:
		return e, nil
	case *evalFloat:
		f := e.ToRawBytes()
		if bytes.IndexByte(f, '.') < 0 {
			f = append(f, '.', '0')
		}
		return json.NewNumber(f), nil
	case evalNumeric:
		if e == evalBoolTrue {
			return json.ValueTrue, nil
		}
		if e == evalBoolFalse {
			return json.ValueFalse, nil
		}
		return json.NewNumber(e.ToRawBytes()), nil
	case *evalBytes:
		if sqltypes.IsBinary(e.SQLType()) {
			return evalBinaryToJson(e), nil
		}

		jsonText, err := collations.ConvertForJSON(nil, e.bytes, collations.Local().LookupByID(e.col.Collation))
		if err != nil {
			return nil, err
		}

		var p json.Parser
		j, err := p.ParseBytes(jsonText)
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Invalid JSON text in argument 1 to function cast_as_json")
		}
		return j, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", e.SQLType())
	}
}
