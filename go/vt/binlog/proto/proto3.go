// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbt "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// This file contains the methods to convert data structures to and
// from proto3. Eventually the code will be changed to use proto3
// structures internally, and this will be obsolete.

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
		Timestamp: bt.Timestamp,
		Gtid:      myproto.EncodeGTID(bt.GTIDField.Value),
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
		Timestamp: bt.Timestamp,
		GTIDField: myproto.GTIDField{
			Value: myproto.MustDecodeGTID(bt.Gtid),
		},
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
		Position: myproto.ReplicationPositionToProto(b.Position),
	}
}

// ProtoToBlpPosition converts a proto to a BlpPosition
func ProtoToBlpPosition(b *pbt.BlpPosition) *BlpPosition {
	return &BlpPosition{
		Uid:      b.Uid,
		Position: myproto.ProtoToReplicationPosition(b.Position),
	}
}
