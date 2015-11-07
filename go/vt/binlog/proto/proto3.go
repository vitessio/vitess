// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// This file contains the methods to convert data structures to and
// from proto3. Eventually the code will be changed to use proto3
// structures internally, and this will be obsolete.

// StreamEventToProto converts a StreamEvent to a proto3
func StreamEventToProto(s *StreamEvent) *pb.StreamEvent {
	result := &pb.StreamEvent{
		TableName:        s.TableName,
		PrimaryKeyFields: mproto.FieldsToProto3(s.PrimaryKeyFields),
		PrimaryKeyValues: mproto.RowsToProto3(s.PrimaryKeyValues),
		Sql:              s.Sql,
		Timestamp:        s.Timestamp,
		TransactionId:    s.TransactionID,
	}
	switch s.Category {
	case "DML":
		result.Category = pb.StreamEvent_SE_DML
	case "DDL":
		result.Category = pb.StreamEvent_SE_DDL
	case "POS":
		result.Category = pb.StreamEvent_SE_POS
	default:
		result.Category = pb.StreamEvent_SE_ERR
	}
	return result
}

// ProtoToStreamEvent converts a proto to a StreamEvent
func ProtoToStreamEvent(s *pb.StreamEvent) *StreamEvent {
	result := &StreamEvent{
		TableName:        s.TableName,
		PrimaryKeyFields: mproto.Proto3ToFields(s.PrimaryKeyFields),
		PrimaryKeyValues: mproto.Proto3ToRows(s.PrimaryKeyValues),
		Sql:              s.Sql,
		Timestamp:        s.Timestamp,
		TransactionID:    s.TransactionId,
	}
	switch s.Category {
	case pb.StreamEvent_SE_DML:
		result.Category = "DML"
	case pb.StreamEvent_SE_DDL:
		result.Category = "DDL"
	case pb.StreamEvent_SE_POS:
		result.Category = "POS"
	default:
		result.Category = "ERR"
	}
	return result
}
