// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

// BindVariablesToProto3 converts internal type to proto3 BindVariable array
func BindVariablesToProto3(bindVars map[string]interface{}) (map[string]*querypb.BindVariable, error) {
	if len(bindVars) == 0 {
		return nil, nil
	}

	result := make(map[string]*querypb.BindVariable)
	for k, v := range bindVars {
		bv := new(querypb.BindVariable)
		switch v := v.(type) {
		case []interface{}:
			// This is how the list variables will normally appear.
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv.Type = sqltypes.Tuple
			bv.Values = make([]*querypb.Value, len(v))
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				val, err := BindVariableToValue(lv)
				if err != nil {
					return nil, fmt.Errorf("key: %s: %v", k, err)
				}
				if val.Type != sqltypes.Null {
					values[i] = val
					bv.Values[i] = &values[i]
				}
			}
		case []string:
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv.Type = sqltypes.Tuple
			bv.Values = make([]*querypb.Value, len(v))
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
			bv.Type = sqltypes.Tuple
			bv.Values = make([]*querypb.Value, len(v))
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
			bv.Type = sqltypes.Tuple
			bv.Values = make([]*querypb.Value, len(v))
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
			bv.Type = sqltypes.Tuple
			bv.Values = make([]*querypb.Value, len(v))
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
			bv.Type = sqltypes.Tuple
			bv.Values = make([]*querypb.Value, len(v))
			values := make([]querypb.Value, len(v))
			for i, lv := range v {
				values[i].Type = sqltypes.Uint64
				values[i].Value = strconv.AppendUint(nil, lv, 10)
				bv.Values[i] = &values[i]
			}
		default:
			val, err := BindVariableToValue(v)
			if err != nil {
				return nil, fmt.Errorf("key: %s: %v", k, err)
			}
			bv.Type = val.Type
			bv.Value = val.Value
		}
		result[k] = bv
	}
	return result, nil
}

// BindVariableToValue converts a native bind variable value
// to a proto Value.
func BindVariableToValue(v interface{}) (querypb.Value, error) {
	switch v := v.(type) {
	case string:
		return querypb.Value{
			Type:  sqltypes.VarChar,
			Value: []byte(v),
		}, nil
	case []byte:
		return querypb.Value{
			Type:  sqltypes.VarBinary,
			Value: v,
		}, nil
	case int:
		return querypb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int8:
		return querypb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int16:
		return querypb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int32:
		return querypb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int64:
		return querypb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, v, 10),
		}, nil
	case uint:
		return querypb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, uint64(v), 10),
		}, nil
	case uint8:
		return querypb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, uint64(v), 10),
		}, nil
	case uint16:
		return querypb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, uint64(v), 10),
		}, nil
	case uint32:
		return querypb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, uint64(v), 10),
		}, nil
	case uint64:
		return querypb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, v, 10),
		}, nil
	case float32:
		return querypb.Value{
			Type:  sqltypes.Float64,
			Value: strconv.AppendFloat(nil, float64(v), 'f', -1, 64),
		}, nil
	case float64:
		return querypb.Value{
			Type:  sqltypes.Float64,
			Value: strconv.AppendFloat(nil, v, 'f', -1, 64),
		}, nil
	case sqltypes.Value:
		return querypb.Value{Type: v.Type(), Value: v.Raw()}, nil
	case nil:
		return querypb.Value{}, nil
	}
	return querypb.Value{}, fmt.Errorf("unexpected type %T", v)
}

// Proto3ToBoundQuery converts a proto.BoundQuery to the internal data structure
func Proto3ToBoundQuery(query *querypb.BoundQuery) (*BoundQuery, error) {
	bv, err := Proto3ToBindVariables(query.BindVariables)
	if err != nil {
		return nil, err
	}
	return &BoundQuery{
		Sql:           string(query.Sql),
		BindVariables: bv,
	}, nil
}

// Proto3ToBoundQueryList converts am array of proto.BoundQuery to the internal data structure
func Proto3ToBoundQueryList(queries []*querypb.BoundQuery) ([]BoundQuery, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	result := make([]BoundQuery, len(queries))
	for i, q := range queries {
		res, err := Proto3ToBoundQuery(q)
		if err != nil {
			return nil, err
		}
		result[i] = *res
	}
	return result, nil
}

// Proto3ToBindVariables converts a proto.BinVariable map to internal data structure
func Proto3ToBindVariables(bv map[string]*querypb.BindVariable) (map[string]interface{}, error) {
	if len(bv) == 0 {
		return nil, nil
	}
	result := make(map[string]interface{})
	for k, v := range bv {
		if v == nil {
			continue
		}
		if v.Type == sqltypes.Tuple {
			list := make([]interface{}, len(v.Values))
			for i, lv := range v.Values {
				v, err := sqltypes.ValueFromBytes(lv.Type, lv.Value)
				if err != nil {
					return nil, err
				}
				// TODO(sougou): Change this to just v.
				list[i] = v.ToNative()
			}
			result[k] = list
		} else {
			v, err := sqltypes.ValueFromBytes(v.Type, v.Value)
			if err != nil {
				return nil, err
			}
			// TODO(sougou): Change this to just v.
			result[k] = v.ToNative()
		}
	}
	return result, nil
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
func Proto3ToQuerySplits(queries []*querypb.QuerySplit) ([]QuerySplit, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	result := make([]QuerySplit, len(queries))
	for i, qs := range queries {
		bv, err := Proto3ToBindVariables(qs.Query.BindVariables)
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
