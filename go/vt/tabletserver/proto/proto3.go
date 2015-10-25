// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"strconv"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TargetToProto3 transform the bson RPC target to proto3
func TargetToProto3(target *Target) *pb.Target {
	if target == nil {
		return nil
	}
	return &pb.Target{
		Keyspace:   target.Keyspace,
		Shard:      target.Shard,
		TabletType: pbt.TabletType(target.TabletType),
	}
}

// BoundQueryToProto3 converts internal types to proto3 BoundQuery
func BoundQueryToProto3(sql string, bindVars map[string]interface{}) (*pb.BoundQuery, error) {
	bv, err := BindVariablesToProto3(bindVars)
	if err != nil {
		return nil, err
	}
	return &pb.BoundQuery{
		Sql:           sql,
		BindVariables: bv,
	}, nil
}

// BindVariablesToProto3 converts internal type to proto3 BindVariable array
func BindVariablesToProto3(bindVars map[string]interface{}) (map[string]*pb.BindVariable, error) {
	if len(bindVars) == 0 {
		return nil, nil
	}

	result := make(map[string]*pb.BindVariable)
	for k, v := range bindVars {
		bv := new(pb.BindVariable)
		switch v := v.(type) {
		case []interface{}:
			// This is how the list variables will normally appear.
			if len(v) == 0 {
				return nil, fmt.Errorf("empty list not allowed: %s", k)
			}
			bv.Type = sqltypes.Tuple
			bv.Values = make([]*pb.Value, len(v))
			values := make([]pb.Value, len(v))
			for i, lv := range v {
				val, err := buildValue(lv)
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
			bv.Values = make([]*pb.Value, len(v))
			values := make([]pb.Value, len(v))
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
			bv.Values = make([]*pb.Value, len(v))
			values := make([]pb.Value, len(v))
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
			bv.Values = make([]*pb.Value, len(v))
			values := make([]pb.Value, len(v))
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
			bv.Values = make([]*pb.Value, len(v))
			values := make([]pb.Value, len(v))
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
			bv.Values = make([]*pb.Value, len(v))
			values := make([]pb.Value, len(v))
			for i, lv := range v {
				values[i].Type = sqltypes.Int64
				values[i].Value = strconv.AppendUint(nil, lv, 10)
				bv.Values[i] = &values[i]
			}
		default:
			val, err := buildValue(v)
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

func buildValue(v interface{}) (pb.Value, error) {
	switch v := v.(type) {
	case string:
		return pb.Value{
			Type:  sqltypes.VarChar,
			Value: []byte(v),
		}, nil
	case []byte:
		return pb.Value{
			Type:  sqltypes.VarBinary,
			Value: v,
		}, nil
	case int:
		return pb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int16:
		return pb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int32:
		return pb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int64:
		return pb.Value{
			Type:  sqltypes.Int64,
			Value: strconv.AppendInt(nil, v, 10),
		}, nil
	case uint:
		return pb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, uint64(v), 10),
		}, nil
	case uint16:
		return pb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, uint64(v), 10),
		}, nil
	case uint32:
		return pb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, uint64(v), 10),
		}, nil
	case uint64:
		return pb.Value{
			Type:  sqltypes.Uint64,
			Value: strconv.AppendUint(nil, v, 10),
		}, nil
	case float32:
		return pb.Value{
			Type:  sqltypes.Float64,
			Value: strconv.AppendFloat(nil, float64(v), 'f', -1, 64),
		}, nil
	case float64:
		return pb.Value{
			Type:  sqltypes.Float64,
			Value: strconv.AppendFloat(nil, v, 'f', -1, 64),
		}, nil
	case sqltypes.Value:
		switch {
		case v.IsNull():
			return pb.Value{}, nil
		case v.IsNumeric():
			return pb.Value{Type: sqltypes.Int64, Value: v.Raw()}, nil
		case v.IsFractional():
			return pb.Value{Type: sqltypes.Float64, Value: v.Raw()}, nil
		}
		return pb.Value{Type: sqltypes.VarBinary, Value: v.Raw()}, nil
	case nil:
		return pb.Value{}, nil
	}
	return pb.Value{}, fmt.Errorf("unexpected type %T", v)
}

// Proto3ToBoundQuery converts a proto.BoundQuery to the internal data structure
func Proto3ToBoundQuery(query *pb.BoundQuery) (*BoundQuery, error) {
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
func Proto3ToBoundQueryList(queries []*pb.BoundQuery) ([]BoundQuery, error) {
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
func Proto3ToBindVariables(bv map[string]*pb.BindVariable) (map[string]interface{}, error) {
	if len(bv) == 0 {
		return nil, nil
	}
	result := make(map[string]interface{})
	var err error
	for k, v := range bv {
		if v.Type == sqltypes.Tuple {
			list := make([]interface{}, len(v.Values))
			for i, lv := range v.Values {
				asbind := &pb.BindVariable{
					Type:  lv.Type,
					Value: lv.Value,
				}
				list[i], err = buildSQLValue(asbind)
				if err != nil {
					return nil, err
				}
			}
			result[k] = list
		} else {
			result[k], err = buildSQLValue(v)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func buildSQLValue(v *pb.BindVariable) (interface{}, error) {
	// TODO(sougou): Revisit this when QueryResult gets revamped
	if v == nil || v.Type == sqltypes.Null {
		return nil, nil
	} else if int(v.Type)&sqltypes.IsIntegral != 0 {
		if int(v.Type)&sqltypes.IsUnsigned != 0 {
			return strconv.ParseUint(string(v.Value), 0, 64)
		}
		return strconv.ParseInt(string(v.Value), 0, 64)
	} else if int(v.Type)&sqltypes.IsFloat != 0 {
		return strconv.ParseFloat(string(v.Value), 64)
	}
	return v.Value, nil
}

// Proto3ToQueryResultList converts a proto3 QueryResult to an internal data structure.
func Proto3ToQueryResultList(results []*pb.QueryResult) *QueryResultList {
	result := &QueryResultList{
		List: make([]mproto.QueryResult, len(results)),
	}
	for i, qr := range results {
		result.List[i] = *mproto.Proto3ToQueryResult(qr)
	}
	return result
}

// QueryResultListToProto3 changes the internal array of QueryResult to the proto3 version
func QueryResultListToProto3(results []mproto.QueryResult) ([]*pb.QueryResult, error) {
	if len(results) == 0 {
		return nil, nil
	}
	result := make([]*pb.QueryResult, len(results))
	var err error
	for i := range results {
		result[i], err = mproto.QueryResultToProto3(&results[i])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Proto3ToQuerySplits converts a proto3 QuerySplit array to a native QuerySplit array
func Proto3ToQuerySplits(queries []*pb.QuerySplit) ([]QuerySplit, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	result := make([]QuerySplit, len(queries))
	for i, qs := range queries {
		res, err := Proto3ToBoundQuery(qs.Query)
		if err != nil {
			return nil, err
		}
		result[i] = QuerySplit{
			Query:    *res,
			RowCount: qs.RowCount,
		}
	}
	return result, nil
}

// QuerySplitsToProto3 converts a native QuerySplit array to the proto3 version
func QuerySplitsToProto3(queries []QuerySplit) ([]*pb.QuerySplit, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	result := make([]*pb.QuerySplit, len(queries))
	for i, qs := range queries {
		q, err := BoundQueryToProto3(qs.Query.Sql, qs.Query.BindVariables)
		if err != nil {
			return nil, err
		}
		result[i] = &pb.QuerySplit{
			Query:    q,
			RowCount: qs.RowCount,
		}
	}
	return result, nil
}
