// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbt "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
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

// StatementToProto converts a Statement to a proto3
func StatementToProto(s *Statement) *pb.BinlogTransaction_Statement {
	return &pb.BinlogTransaction_Statement{
		Category: pb.BinlogTransaction_Statement_Category(s.Category),
		Charset:  mproto.CharsetToProto(s.Charset),
		Sql:      s.Sql,
	}
}

// ProtoToStatement converts a proto to a Statement
func ProtoToStatement(s *pb.BinlogTransaction_Statement) Statement {
	return Statement{
		Category: int(s.Category),
		Charset:  mproto.ProtoToCharset(s.Charset),
		Sql:      s.Sql,
	}
}

// BinlogTransactionToProto converts a BinlogTransaction to a proto3
func BinlogTransactionToProto(bt *BinlogTransaction) *pb.BinlogTransaction {
	result := &pb.BinlogTransaction{
		Timestamp:     bt.Timestamp,
		TransactionId: bt.TransactionID,
	}
	if len(bt.Statements) > 0 {
		result.Statements = make([]*pb.BinlogTransaction_Statement, len(bt.Statements))
		for i, s := range bt.Statements {
			result.Statements[i] = StatementToProto(&s)
		}
	}

	return result
}

// ProtoToBinlogTransaction converts a proto to a BinlogTransaction
func ProtoToBinlogTransaction(bt *pb.BinlogTransaction) *BinlogTransaction {
	result := &BinlogTransaction{
		Timestamp:     bt.Timestamp,
		TransactionID: bt.TransactionId,
	}
	if len(bt.Statements) > 0 {
		result.Statements = make([]Statement, len(bt.Statements))
		for i, s := range bt.Statements {
			result.Statements[i] = ProtoToStatement(s)
		}
	}
	return result
}

// BlpPositionToProto converts a BlpPosition to a proto3
func BlpPositionToProto(b *BlpPosition) *pbt.BlpPosition {
	return &pbt.BlpPosition{
		Uid:      b.Uid,
		Position: myproto.EncodeReplicationPosition(b.Position),
	}
}

// ProtoToBlpPosition converts a proto to a BlpPosition
func ProtoToBlpPosition(b *pbt.BlpPosition) *BlpPosition {
	pos, err := myproto.DecodeReplicationPosition(b.Position)
	if err != nil {
		panic(fmt.Errorf("cannot decode position: %v", err))
	}
	return &BlpPosition{
		Uid:      b.Uid,
		Position: pos,
	}
}

// BlpPositionListToProto converts a BlpPositionList to a proto3
func BlpPositionListToProto(l *BlpPositionList) []*pbt.BlpPosition {
	if len(l.Entries) == 0 {
		return nil
	}
	result := make([]*pbt.BlpPosition, len(l.Entries))
	for i, p := range l.Entries {
		result[i] = BlpPositionToProto(&p)
	}
	return result
}

// ProtoToBlpPositionList converts a proto to a BlpPositionList
func ProtoToBlpPositionList(l []*pbt.BlpPosition) *BlpPositionList {
	result := &BlpPositionList{}
	if len(l) > 0 {
		result.Entries = make([]BlpPosition, len(l))
		for i, p := range l {
			result.Entries[i] = *ProtoToBlpPosition(p)
		}
	}
	return result
}
