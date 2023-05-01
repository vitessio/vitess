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

package json

import (
	"encoding/hex"
	"fmt"
	"math/big"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/sqltypes"
)

// MarshalSQLTo appends marshaled v to dst and returns the result in
// the form like `JSON_OBJECT` or `JSON_ARRAY` to ensure we don't
// lose any type information.
func (v *Value) MarshalSQLTo(dst []byte) []byte {
	return v.marshalSQLInternal(true, dst)
}

func (v *Value) marshalSQLInternal(top bool, dst []byte) []byte {
	switch v.Type() {
	case TypeObject:
		dst = append(dst, "JSON_OBJECT("...)
		for i, vv := range v.o.kvs {
			if i != 0 {
				dst = append(dst, ", "...)
			}
			dst = append(dst, "_utf8mb4'"...)
			dst = append(dst, vv.k...)
			dst = append(dst, "', "...)
			dst = vv.v.marshalSQLInternal(false, dst)
		}
		dst = append(dst, ')')
		return dst
	case TypeArray:
		dst = append(dst, "JSON_ARRAY("...)
		for i, vv := range v.a {
			if i != 0 {
				dst = append(dst, ", "...)
			}
			dst = vv.marshalSQLInternal(false, dst)
		}
		dst = append(dst, ')')
		return dst
	case TypeString:
		if top {
			dst = append(dst, "CAST(JSON_QUOTE("...)
		}
		dst = append(dst, "_utf8mb4"...)
		dst = append(dst, sqltypes.EncodeStringSQL(v.s)...)
		if top {
			dst = append(dst, ") as JSON)"...)
		}
		return dst
	case TypeDate:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "date '"...)
		dst = append(dst, v.MarshalDate()...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeDateTime:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "timestamp '"...)
		dst = append(dst, v.MarshalDateTime()...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeTime:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "time '"...)
		dst = append(dst, v.MarshalTime()...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBlob:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "x'"...)
		dst = append(dst, hex.EncodeToString([]byte(v.s))...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBit:
		if top {
			dst = append(dst, "CAST("...)
		}
		var i big.Int
		i.SetBytes([]byte(v.s))
		dst = append(dst, "b'"...)
		dst = append(dst, i.Text(2)...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeNumber:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, v.s...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBoolean:
		if top {
			dst = append(dst, "CAST("...)
		}
		if v == ValueTrue {
			dst = append(dst, "true"...)
		} else {
			dst = append(dst, "false"...)
		}
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeNull:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "null"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	default:
		panic(fmt.Errorf("BUG: unexpected Value type: %d", v.t))
	}
}

// MarshalSQLValue converts the byte representation of a json value
// and returns it formatted by MarshalSQLTo
func MarshalSQLValue(buf []byte) (*sqltypes.Value, error) {
	var parser Parser
	if len(buf) == 0 {
		buf = sqltypes.NullBytes
	}
	jsonVal, err := parser.ParseBytes(buf)
	if err != nil {
		return nil, err
	}
	newVal := sqltypes.MakeTrusted(querypb.Type_JSON, jsonVal.MarshalSQLTo(nil))
	if err != nil {
		return nil, err
	}
	return &newVal, nil
}
