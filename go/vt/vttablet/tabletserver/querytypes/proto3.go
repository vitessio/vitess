/*
Copyright 2017 Google Inc.

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

package querytypes

import (
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// BoundQueryToProto3 converts internal types to proto3 BoundQuery
func BoundQueryToProto3(sql string, bindVars map[string]interface{}) (*querypb.BoundQuery, error) {
	bv, err := BindVariablesToProto3(bindVars)
	if err != nil {
		return nil, err
	}
	return &querypb.BoundQuery{
		Sql:           sql,
		BindVariables: bv,
	}, nil
}

// BoundQueriesToProto3 converts internal types to proto3 BoundQuery
func BoundQueriesToProto3(sql []string, bindVars []map[string]interface{}) ([]*querypb.BoundQuery, error) {
	boundQueries := make([]*querypb.BoundQuery, len(sql))
	if bindVars == nil {
		bindVars = make([]map[string]interface{}, len(sql))
	}
	for index, query := range sql {
		boundQuery, err := BoundQueryToProto3(query, bindVars[index])
		if err != nil {
			return nil, err
		}
		boundQueries[index] = boundQuery
	}
	return boundQueries, nil
}

// BindVariablesToProto3 converts internal type to proto3 BindVariable array
func BindVariablesToProto3(bindVars map[string]interface{}) (map[string]*querypb.BindVariable, error) {
	if len(bindVars) == 0 {
		return nil, nil
	}

	result := make(map[string]*querypb.BindVariable)
	for k, v := range bindVars {
		var bv *querypb.BindVariable
		switch v := v.(type) {
		case *querypb.BindVariable:
			// Already the right type, nothing to do.
			bv = v
		case []interface{}:
			// This is how the list variables will normally appear.
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv = &querypb.BindVariable{
				Type:   sqltypes.Tuple,
				Values: make([]*querypb.Value, len(v)),
			}
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				typ, val, err := bindVariableToValue(lv)
				if err != nil {
					return nil, fmt.Errorf("key: %s: %v", k, err)
				}
				if typ != sqltypes.Null {
					values[i].Type = typ
					values[i].Value = val
					bv.Values[i] = &values[i]
				}
			}
		case []string:
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv = &querypb.BindVariable{
				Type:   sqltypes.Tuple,
				Values: make([]*querypb.Value, len(v)),
			}
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				values[i].Type = sqltypes.VarChar
				values[i].Value = []byte(lv)
				bv.Values[i] = &values[i]
			}
		case [][]byte:
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv = &querypb.BindVariable{
				Type:   sqltypes.Tuple,
				Values: make([]*querypb.Value, len(v)),
			}
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				values[i].Type = sqltypes.VarBinary
				values[i].Value = lv
				bv.Values[i] = &values[i]
			}
		case []int:
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv = &querypb.BindVariable{
				Type:   sqltypes.Tuple,
				Values: make([]*querypb.Value, len(v)),
			}
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				values[i].Type = sqltypes.Int64
				values[i].Value = strconv.AppendInt(nil, int64(lv), 10)
				bv.Values[i] = &values[i]
			}
		case []int64:
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv = &querypb.BindVariable{
				Type:   sqltypes.Tuple,
				Values: make([]*querypb.Value, len(v)),
			}
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				values[i].Type = sqltypes.Int64
				values[i].Value = strconv.AppendInt(nil, lv, 10)
				bv.Values[i] = &values[i]
			}
		case []uint64:
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv = &querypb.BindVariable{
				Type:   sqltypes.Tuple,
				Values: make([]*querypb.Value, len(v)),
			}
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				values[i].Type = sqltypes.Uint64
				values[i].Value = strconv.AppendUint(nil, lv, 10)
				bv.Values[i] = &values[i]
			}
		default:
			typ, val, err := bindVariableToValue(v)
			if err != nil {
				return nil, fmt.Errorf("key: %s: %v", k, err)
			}
			bv = &querypb.BindVariable{
				Type:  typ,
				Value: val,
			}
		}
		result[k] = bv
	}
	return result, nil
}

// bindVariableToValue converts a native bind variable value
// to a proto Type and value.
func bindVariableToValue(v interface{}) (querypb.Type, []byte, error) {
	switch v := v.(type) {
	case string:
		return sqltypes.VarChar, []byte(v), nil
	case []byte:
		return sqltypes.VarBinary, v, nil
	case int:
		return sqltypes.Int64, strconv.AppendInt(nil, int64(v), 10), nil
	case int8:
		return sqltypes.Int64, strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return sqltypes.Int64, strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return sqltypes.Int64, strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return sqltypes.Int64, strconv.AppendInt(nil, v, 10), nil
	case uint:
		return sqltypes.Uint64, strconv.AppendUint(nil, uint64(v), 10), nil
	case uint8:
		return sqltypes.Uint64, strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return sqltypes.Uint64, strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return sqltypes.Uint64, strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return sqltypes.Uint64, strconv.AppendUint(nil, v, 10), nil
	case float32:
		return sqltypes.Float64, strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case float64:
		return sqltypes.Float64, strconv.AppendFloat(nil, v, 'f', -1, 64), nil
	case sqltypes.Value:
		return v.Type(), v.Raw(), nil
	case *querypb.BindVariable:
		val, err := sqltypes.ValueFromBytes(v.Type, v.Value)
		if err != nil {
			return sqltypes.Null, nil, fmt.Errorf("bindVariableToValue: %v", err)
		}
		return val.Type(), val.Raw(), nil
	case nil:
		return sqltypes.Null, nil, nil
	}
	return sqltypes.Null, nil, fmt.Errorf("bindVariableToValue: unexpected type %T", v)
}

// Proto3ToBoundQuery converts a proto.BoundQuery to the internal data structure
func Proto3ToBoundQuery(query *querypb.BoundQuery, enforceSafety bool) (*BoundQuery, error) {
	bv, err := Proto3ToBindVariables(query.BindVariables, enforceSafety)
	if err != nil {
		return nil, err
	}
	return &BoundQuery{
		Sql:           string(query.Sql),
		BindVariables: bv,
	}, nil
}

// Proto3ToBoundQueryList converts am array of proto.BoundQuery to the internal data structure
func Proto3ToBoundQueryList(queries []*querypb.BoundQuery, enforceSafety bool) ([]BoundQuery, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	result := make([]BoundQuery, len(queries))
	for i, q := range queries {
		res, err := Proto3ToBoundQuery(q, enforceSafety)
		if err != nil {
			return nil, err
		}
		result[i] = *res
	}
	return result, nil
}

// Proto3ToBindVariables converts a proto.BinVariable map to internal data structure
func Proto3ToBindVariables(bv map[string]*querypb.BindVariable, enforceSafety bool) (map[string]interface{}, error) {
	if len(bv) == 0 {
		return nil, nil
	}
	result := make(map[string]interface{})
	for k, v := range bv {
		if v == nil {
			result[k] = nil
			continue
		}
		if enforceSafety {
			if err := checkBindvar(v); err != nil {
				return nil, err
			}
		}
		result[k] = v
	}
	return result, nil
}

var errUnsafe = fmt.Errorf("type: %v is not allowed as a bind variable", querypb.Type_EXPRESSION)

// checkBindvar returns an error if the bind var uses an unsafe type.
func checkBindvar(v *querypb.BindVariable) error {
	switch v.Type {
	case querypb.Type_EXPRESSION:
		return errUnsafe
	case querypb.Type_TUPLE:
		for _, tv := range v.Values {
			if tv.Type == querypb.Type_EXPRESSION {
				return errUnsafe
			}
		}
	}
	return nil
}

// QueryResultListToProto3 temporarily resurrected.
func QueryResultListToProto3(results []sqltypes.Result) []*querypb.QueryResult {
	if len(results) == 0 {
		return nil
	}
	result := make([]*querypb.QueryResult, len(results))
	for i := range results {
		result[i] = sqltypes.ResultToProto3(&results[i])
	}
	return result
}

// Proto3ToQuerySplits converts a proto3 QuerySplit array to a native QuerySplit array
func Proto3ToQuerySplits(queries []*querypb.QuerySplit, enforceSafety bool) ([]QuerySplit, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	result := make([]QuerySplit, len(queries))
	for i, qs := range queries {
		bv, err := Proto3ToBindVariables(qs.Query.BindVariables, enforceSafety)
		if err != nil {
			return nil, err
		}
		result[i] = QuerySplit{
			Sql:           qs.Query.Sql,
			BindVariables: bv,
			RowCount:      qs.RowCount,
		}
	}
	return result, nil
}

// QuerySplitsToProto3 converts a native QuerySplit array to the proto3 version
func QuerySplitsToProto3(queries []QuerySplit) ([]*querypb.QuerySplit, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	result := make([]*querypb.QuerySplit, len(queries))
	for i, qs := range queries {
		q, err := BoundQueryToProto3(qs.Sql, qs.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i] = &querypb.QuerySplit{
			Query:    q,
			RowCount: qs.RowCount,
		}
	}
	return result, nil
}
