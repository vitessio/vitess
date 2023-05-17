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
	"errors"
	"fmt"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type errJSONType string

func (fn errJSONType) Error() string {
	return fmt.Sprintf("Invalid data type for JSON data to function %s; a JSON string or JSON type is required.", string(fn))
}

var errJSONPath = errors.New("Invalid JSON path expression.")

type evalJSON = json.Value

var _ eval = (*evalJSON)(nil)
var _ hashable = (*evalJSON)(nil)

func intoJSON(fn string, e eval) (*evalJSON, error) {
	switch e := e.(type) {
	case *evalJSON:
		return e, nil
	case *evalBytes:
		var p json.Parser
		return p.ParseBytes(e.bytes)
	default:
		return nil, errJSONType(fn)
	}
}

func intoJSONPath(e eval) (*json.Path, error) {
	switch e := e.(type) {
	case *evalBytes:
		var p json.PathParser
		return p.ParseBytes(e.bytes)
	default:
		return nil, errJSONPath
	}
}

func evalConvert_bj(e *evalBytes) *evalJSON {
	if e.tt == int16(sqltypes.Bit) {
		return json.NewBit(e.string())
	}
	return json.NewBlob(e.string())
}

func evalConvert_fj(e *evalFloat) *evalJSON {
	f := e.ToRawBytes()
	if bytes.IndexByte(f, '.') < 0 {
		f = append(f, '.', '0')
	}
	return json.NewNumber(hack.String(f), json.NumberTypeFloat)
}

func evalConvert_nj(e evalNumeric) *evalJSON {
	if e == evalBoolTrue {
		return json.ValueTrue
	}
	if e == evalBoolFalse {
		return json.ValueFalse
	}
	switch e := e.(type) {
	case *evalInt64:
		return json.NewNumber(hack.String(e.ToRawBytes()), json.NumberTypeSigned)
	case *evalUint64:
		return json.NewNumber(hack.String(e.ToRawBytes()), json.NumberTypeUnsigned)
	case *evalDecimal:
		return json.NewNumber(hack.String(e.ToRawBytes()), json.NumberTypeDecimal)
	}
	panic("unreachable")
}

func evalConvert_cj(e *evalBytes) (*evalJSON, error) {
	jsonText, err := charset.Convert(nil, charset.Charset_utf8mb4{}, e.bytes, e.col.Collation.Get().Charset())
	if err != nil {
		return nil, err
	}
	var p json.Parser
	return p.ParseBytes(jsonText)
}

func evalConvertArg_cj(e *evalBytes) (*evalJSON, error) {
	jsonText, err := charset.Convert(nil, charset.Charset_utf8mb4{}, e.bytes, e.col.Collation.Get().Charset())
	if err != nil {
		return nil, err
	}
	return json.NewString(string(jsonText)), nil
}

func evalToJSON(e eval) (*evalJSON, error) {
	switch e := e.(type) {
	case nil:
		return json.ValueNull, nil
	case *evalJSON:
		return e, nil
	case *evalFloat:
		return evalConvert_fj(e), nil
	case evalNumeric:
		return evalConvert_nj(e), nil
	case *evalBytes:
		if sqltypes.IsBinary(e.SQLType()) {
			return evalConvert_bj(e), nil
		}
		return evalConvert_cj(e)
	case *evalTemporal:
		return e.toJSON(), nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", e.SQLType())
	}
}

func argToJSON(e eval) (*evalJSON, error) {
	switch e := e.(type) {
	case nil:
		return json.ValueNull, nil
	case *evalJSON:
		return e, nil
	case *evalFloat:
		return evalConvert_fj(e), nil
	case evalNumeric:
		return evalConvert_nj(e), nil
	case *evalBytes:
		if sqltypes.IsBinary(e.SQLType()) {
			return evalConvert_bj(e), nil
		}
		return evalConvertArg_cj(e)
	case *evalTemporal:
		return e.toJSON(), nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", e.SQLType())
	}
}
