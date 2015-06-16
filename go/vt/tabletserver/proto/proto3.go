// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"

	pb "github.com/youtube/vitess/go/vt/proto/query"
)

// BoundQueryToProto3 converts internal types to proto3 BoundQuery
func BoundQueryToProto3(sql string, bindVars map[string]interface{}) *pb.BoundQuery {
	result := &pb.BoundQuery{
		Sql: []byte(sql),
	}
	if len(bindVars) > 0 {
		result.BindVariables = make(map[string]*pb.BindVariable)
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
				case string, []byte:
					bv.Type = pb.BindVariable_TYPE_BYTES_LIST
					listArg := make([][]byte, len(v))
					for i, lv := range v {
						listArg[i] = lv.([]byte)
					}
					bv.ValueBytesList = listArg
				case int, int16, int32, int64:
					bv.Type = pb.BindVariable_TYPE_INT_LIST
					listArg := make([]int64, len(v))
					for i, lv := range v {
						listArg[i] = lv.(int64)
					}
					bv.ValueIntList = listArg
				case uint, uint16, uint32, uint64:
					bv.Type = pb.BindVariable_TYPE_UINT_LIST
					listArg := make([]uint64, len(v))
					for i, lv := range v {
						listArg[i] = lv.(uint64)
					}
					bv.ValueUintList = listArg
				case float32, float64:
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
			result.BindVariables[k] = bv
		}
	}
	return result
}

// ProtoToQueryResult converts a proto3 QueryResult to an internal data structure.
func ProtoToQueryResult(qr *pb.QueryResult) *mproto.QueryResult {
	result := &mproto.QueryResult{
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
		//		Rows         [][]sqltypes.Value
		//		Err          *RPCError

	}

	if len(qr.Fields) > 0 {
		result.Fields = make([]mproto.Field, len(qr.Fields))
		for i, f := range qr.Fields {
			result.Fields[i].Name = f.Name
			result.Fields[i].Type = int64(f.Type)
			result.Fields[i].Flags = int64(f.Flags)
		}
	}

	if len(qr.Rows) > 0 {
		result.Rows = make([][]sqltypes.Value, len(qr.Rows))
		for i, r := range qr.Rows {
			result.Rows[i] = make([]sqltypes.Value, len(r.Values))
			for j, c := range r.Values {
				if c == nil {
					result.Rows[i][j] = sqltypes.NULL
				} else {
					result.Rows[i][j] = sqltypes.MakeString(c)
				}
			}
		}
	}

	return result
}
