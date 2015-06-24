// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// This file contains the methods to convert data structures to and
// from proto3. Eventually the code will be changed to use proto3
// structures internally, and this will be obsolete.

// CharsetToProto converts a Charset to a proto3
func CharsetToProto(c *mproto.Charset) *pb.Charset {
	if c == nil {
		return nil
	}
	return &pb.Charset{
		Client: int32(c.Client),
		Conn:   int32(c.Conn),
		Server: int32(c.Server),
	}
}

// ProtoToCharset converts a proto to a Charset
func ProtoToCharset(c *pb.Charset) *mproto.Charset {
	if c == nil {
		return nil
	}
	return &mproto.Charset{
		Client: int(c.Client),
		Conn:   int(c.Conn),
		Server: int(c.Server),
	}
}

// StatementToProto converts a Statement to a proto3
func StatementToProto(s *Statement) *pb.BinlogTransaction_Statement {
	return &pb.BinlogTransaction_Statement{
		Category: pb.BinlogTransaction_Statement_Category(s.Category),
		Charset:  CharsetToProto(s.Charset),
		Sql:      s.Sql,
	}
}

// ProtoToStatement converts a proto to a Statement
func ProtoToStatement(s *pb.BinlogTransaction_Statement) Statement {
	return Statement{
		Category: int(s.Category),
		Charset:  ProtoToCharset(s.Charset),
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
