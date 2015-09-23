// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"

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
func BoundQueryToProto3(sql string, bindVars map[string]interface{}) *pb.BoundQuery {
	return &pb.BoundQuery{
		Sql:           sql,
		BindVariables: BindVariablesToProto3(bindVars),
	}
}

// BindVariablesToProto3 converts internal type to proto3 BindVariable array
func BindVariablesToProto3(bindVars map[string]interface{}) map[string]*pb.BindVariable {
	if len(bindVars) == 0 {
		return nil
	}

	result := make(map[string]*pb.BindVariable)
	for k, v := range bindVars {
		bv := new(pb.BindVariable)
		switch v := v.(type) {
		case []interface{}:
			// This is how the list variables will normally appear.
			if len(v) == 0 {
				continue
			}

			// This assumes homogenous types, but that is what we support.
			val := v[0]
			switch val.(type) {
			// string and []byte are TYPE_BYTES_LIST
			case string:
				bv.Type = pb.BindVariable_TYPE_BYTES_LIST
				listArg := make([][]byte, len(v))
				for i, lv := range v {
					listArg[i] = []byte(lv.(string))
				}
				bv.ValueBytesList = listArg
			case []byte:
				bv.Type = pb.BindVariable_TYPE_BYTES_LIST
				listArg := make([][]byte, len(v))
				for i, lv := range v {
					listArg[i] = lv.([]byte)
				}
				bv.ValueBytesList = listArg

			// int, int16, int32, int64 are TYPE_INT_LIST
			case int:
				bv.Type = pb.BindVariable_TYPE_INT_LIST
				listArg := make([]int64, len(v))
				for i, lv := range v {
					listArg[i] = int64(lv.(int))
				}
				bv.ValueIntList = listArg
			case int16:
				bv.Type = pb.BindVariable_TYPE_INT_LIST
				listArg := make([]int64, len(v))
				for i, lv := range v {
					listArg[i] = int64(lv.(int16))
				}
				bv.ValueIntList = listArg
			case int32:
				bv.Type = pb.BindVariable_TYPE_INT_LIST
				listArg := make([]int64, len(v))
				for i, lv := range v {
					listArg[i] = int64(lv.(int32))
				}
				bv.ValueIntList = listArg
			case int64:
				bv.Type = pb.BindVariable_TYPE_INT_LIST
				listArg := make([]int64, len(v))
				for i, lv := range v {
					listArg[i] = lv.(int64)
				}
				bv.ValueIntList = listArg

			// uint, uint16, uint32, uint64 are TYPE_UINT_LIST
			case uint:
				bv.Type = pb.BindVariable_TYPE_UINT_LIST
				listArg := make([]uint64, len(v))
				for i, lv := range v {
					listArg[i] = uint64(lv.(uint))
				}
				bv.ValueUintList = listArg
			case uint16:
				bv.Type = pb.BindVariable_TYPE_UINT_LIST
				listArg := make([]uint64, len(v))
				for i, lv := range v {
					listArg[i] = uint64(lv.(uint16))
				}
				bv.ValueUintList = listArg
			case uint32:
				bv.Type = pb.BindVariable_TYPE_UINT_LIST
				listArg := make([]uint64, len(v))
				for i, lv := range v {
					listArg[i] = uint64(lv.(uint32))
				}
				bv.ValueUintList = listArg
			case uint64:
				bv.Type = pb.BindVariable_TYPE_UINT_LIST
				listArg := make([]uint64, len(v))
				for i, lv := range v {
					listArg[i] = lv.(uint64)
				}
				bv.ValueUintList = listArg

			// float32, float64 are TYPE_FLOAT_LIST
			case float32:
				bv.Type = pb.BindVariable_TYPE_FLOAT_LIST
				listArg := make([]float64, len(v))
				for i, lv := range v {
					listArg[i] = float64(lv.(float32))
				}
				bv.ValueFloatList = listArg
			case float64:
				bv.Type = pb.BindVariable_TYPE_FLOAT_LIST
				listArg := make([]float64, len(v))
				for i, lv := range v {
					listArg[i] = lv.(float64)
				}
				bv.ValueFloatList = listArg
			}
		case string:
			bv.Type = pb.BindVariable_TYPE_BYTES
			bv.ValueBytes = []byte(v)
		case []string:
			bv.Type = pb.BindVariable_TYPE_BYTES_LIST
			listArg := make([][]byte, len(v))
			for i, lv := range v {
				listArg[i] = []byte(lv)
			}
			bv.ValueBytesList = listArg
		case []byte:
			bv.Type = pb.BindVariable_TYPE_BYTES
			bv.ValueBytes = v
		case [][]byte:
			bv.Type = pb.BindVariable_TYPE_BYTES_LIST
			listArg := make([][]byte, len(v))
			for i, lv := range v {
				listArg[i] = lv
			}
			bv.ValueBytesList = listArg
		case int:
			bv.Type = pb.BindVariable_TYPE_INT
			bv.ValueInt = int64(v)
		case int16:
			bv.Type = pb.BindVariable_TYPE_INT
			bv.ValueInt = int64(v)
		case int32:
			bv.Type = pb.BindVariable_TYPE_INT
			bv.ValueInt = int64(v)
		case int64:
			bv.Type = pb.BindVariable_TYPE_INT
			bv.ValueInt = v
		case []int:
			bv.Type = pb.BindVariable_TYPE_INT_LIST
			listArg := make([]int64, len(v))
			for i, lv := range v {
				listArg[i] = int64(lv)
			}
			bv.ValueIntList = listArg
		case []int16:
			bv.Type = pb.BindVariable_TYPE_INT_LIST
			listArg := make([]int64, len(v))
			for i, lv := range v {
				listArg[i] = int64(lv)
			}
			bv.ValueIntList = listArg
		case []int32:
			bv.Type = pb.BindVariable_TYPE_INT_LIST
			listArg := make([]int64, len(v))
			for i, lv := range v {
				listArg[i] = int64(lv)
			}
			bv.ValueIntList = listArg
		case []int64:
			bv.Type = pb.BindVariable_TYPE_INT_LIST
			listArg := make([]int64, len(v))
			for i, lv := range v {
				listArg[i] = lv
			}
			bv.ValueIntList = listArg
		case uint:
			bv.Type = pb.BindVariable_TYPE_UINT
			bv.ValueUint = uint64(v)
		case uint16:
			bv.Type = pb.BindVariable_TYPE_UINT
			bv.ValueUint = uint64(v)
		case uint32:
			bv.Type = pb.BindVariable_TYPE_UINT
			bv.ValueUint = uint64(v)
		case uint64:
			bv.Type = pb.BindVariable_TYPE_UINT
			bv.ValueUint = v
		case []uint:
			bv.Type = pb.BindVariable_TYPE_UINT_LIST
			listArg := make([]uint64, len(v))
			for i, lv := range v {
				listArg[i] = uint64(lv)
			}
			bv.ValueUintList = listArg
		case []uint16:
			bv.Type = pb.BindVariable_TYPE_UINT_LIST
			listArg := make([]uint64, len(v))
			for i, lv := range v {
				listArg[i] = uint64(lv)
			}
			bv.ValueUintList = listArg
		case []uint32:
			bv.Type = pb.BindVariable_TYPE_UINT_LIST
			listArg := make([]uint64, len(v))
			for i, lv := range v {
				listArg[i] = uint64(lv)
			}
			bv.ValueUintList = listArg
		case []uint64:
			bv.Type = pb.BindVariable_TYPE_UINT_LIST
			listArg := make([]uint64, len(v))
			for i, lv := range v {
				listArg[i] = lv
			}
			bv.ValueUintList = listArg
		case float32:
			bv.Type = pb.BindVariable_TYPE_FLOAT
			bv.ValueFloat = float64(v)
		case float64:
			bv.Type = pb.BindVariable_TYPE_FLOAT
			bv.ValueFloat = float64(v)
		case []float32:
			bv.Type = pb.BindVariable_TYPE_FLOAT_LIST
			listArg := make([]float64, len(v))
			for i, lv := range v {
				listArg[i] = float64(lv)
			}
			bv.ValueFloatList = listArg
		case []float64:
			bv.Type = pb.BindVariable_TYPE_FLOAT_LIST
			listArg := make([]float64, len(v))
			for i, lv := range v {
				listArg[i] = lv
			}
			bv.ValueFloatList = listArg
		}
		result[k] = bv
	}
	return result
}

// Proto3ToBoundQuery converts a proto.BoundQuery to the internal data structure
func Proto3ToBoundQuery(query *pb.BoundQuery) *BoundQuery {
	return &BoundQuery{
		Sql:           string(query.Sql),
		BindVariables: Proto3ToBindVariables(query.BindVariables),
	}
}

// Proto3ToBoundQueryList converts am array of proto.BoundQuery to the internal data structure
func Proto3ToBoundQueryList(queries []*pb.BoundQuery) []BoundQuery {
	if len(queries) == 0 {
		return nil
	}
	result := make([]BoundQuery, len(queries))
	for i, q := range queries {
		result[i] = *Proto3ToBoundQuery(q)
	}
	return result
}

// Proto3ToBindVariables converts a proto.BinVariable map to internal data structure
func Proto3ToBindVariables(bv map[string]*pb.BindVariable) map[string]interface{} {
	if len(bv) == 0 {
		return nil
	}

	result := make(map[string]interface{})
	for k, v := range bv {
		switch v.Type {
		case pb.BindVariable_TYPE_BYTES:
			result[k] = v.ValueBytes
		case pb.BindVariable_TYPE_INT:
			result[k] = v.ValueInt
		case pb.BindVariable_TYPE_UINT:
			result[k] = v.ValueUint
		case pb.BindVariable_TYPE_FLOAT:
			result[k] = v.ValueFloat
		case pb.BindVariable_TYPE_BYTES_LIST:
			bytesList := v.ValueBytesList
			interfaceList := make([]interface{}, len(bytesList))
			for i, lv := range bytesList {
				interfaceList[i] = []byte(lv)
			}
			result[k] = interfaceList
		case pb.BindVariable_TYPE_INT_LIST:
			intList := v.ValueIntList
			interfaceList := make([]interface{}, len(intList))
			for i, lv := range intList {
				interfaceList[i] = lv
			}
			result[k] = interfaceList
		case pb.BindVariable_TYPE_UINT_LIST:
			uintList := v.ValueUintList
			interfaceList := make([]interface{}, len(uintList))
			for i, lv := range uintList {
				interfaceList[i] = lv
			}
			result[k] = interfaceList
		case pb.BindVariable_TYPE_FLOAT_LIST:
			floatList := v.ValueFloatList
			interfaceList := make([]interface{}, len(floatList))
			for i, lv := range floatList {
				interfaceList[i] = lv
			}
			result[k] = interfaceList
		default:
			result[k] = nil
		}
	}
	return result
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
func QueryResultListToProto3(results []mproto.QueryResult) []*pb.QueryResult {
	if len(results) == 0 {
		return nil
	}
	result := make([]*pb.QueryResult, len(results))
	for i := range results {
		result[i] = mproto.QueryResultToProto3(&results[i])
	}
	return result
}

// Proto3ToQuerySplits converts a proto3 QuerySplit array to a native QuerySplit array
func Proto3ToQuerySplits(queries []*pb.QuerySplit) []QuerySplit {
	if len(queries) == 0 {
		return nil
	}
	result := make([]QuerySplit, len(queries))
	for i, qs := range queries {
		result[i] = QuerySplit{
			Query:    *Proto3ToBoundQuery(qs.Query),
			RowCount: qs.RowCount,
		}
	}
	return result
}

// QuerySplitsToProto3 converts a native QuerySplit array to the proto3 version
func QuerySplitsToProto3(queries []QuerySplit) []*pb.QuerySplit {
	if len(queries) == 0 {
		return nil
	}
	result := make([]*pb.QuerySplit, len(queries))
	for i, qs := range queries {
		result[i] = &pb.QuerySplit{
			Query:    BoundQueryToProto3(qs.Query.Sql, qs.Query.BindVariables),
			RowCount: qs.RowCount,
		}
	}
	return result
}
