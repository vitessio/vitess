// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// BinlogEvent represents a single event from a raw MySQL binlog dump stream.
// The implementation is provided by each supported flavor in go/vt/mysqlctl.
//
// BinlogStreamer receives these events through a mysqlctl.SlaveConnection and
// processes them, grouping statements into BinlogTransactions as appropriate.
//
// Methods that only access header fields can't fail as long as IsValid()
// returns true, so they have a single return value. Methods that might fail
// even when IsValid() is true return an error value.
//
// Methods that require information from the initial FORMAT_DESCRIPTION_EVENT
// will have a BinlogFormat parameter.
type BinlogEvent interface {
	// IsValid returns true if the underlying data buffer contains a valid event.
	// This should be called first on any BinlogEvent, and other methods should
	// only be called if this one returns true. This ensures you won't get panics
	// due to bounds checking on the byte array.
	IsValid() bool

	// IsFormatDescription returns true if this is a FORMAT_DESCRIPTION_EVENT.
	IsFormatDescription() bool
	// IsQuery returns true if this is a QUERY_EVENT, which encompasses all SQL
	// statements.
	IsQuery() bool
	// IsXID returns true if this is an XID_EVENT, which is an alternate form of
	// COMMIT.
	IsXID() bool
	// IsGTID returns true if this is a GTID_EVENT.
	IsGTID() bool
	// IsRotate returns true if this is a ROTATE_EVENT.
	IsRotate() bool
	// IsIntVar returns true if this is an INTVAR_EVENT.
	IsIntVar() bool
	// IsRand returns true if this is a RAND_EVENT.
	IsRand() bool
	// HasGTID returns true if this event contains a GTID. That could either be
	// because it's a GTID_EVENT (MariaDB, MySQL 5.6), or because it is some
	// arbitrary event type that has a GTID in the header (Google MySQL).
	HasGTID(BinlogFormat) bool

	// Timestamp returns the timestamp from the event header.
	Timestamp() uint32

	// Format returns a BinlogFormat struct based on the event data.
	// This is only valid if IsFormatDescription() returns true.
	Format() (BinlogFormat, error)
	// GTID returns the GTID from the event.
	// This is only valid if HasGTID() returns true.
	GTID(BinlogFormat) (myproto.GTID, error)
	// IsBeginGTID returns true if this is a GTID_EVENT that also serves as a
	// BEGIN statement. Otherwise, the GTID_EVENT is just providing the GTID for
	// the following QUERY_EVENT.
	// This is only valid if IsGTID() returns true.
	IsBeginGTID(BinlogFormat) bool
	// Query returns a Query struct representing data from a QUERY_EVENT.
	// This is only valid if IsQuery() returns true.
	Query(BinlogFormat) (Query, error)
	// IntVar returns the name and value of the variable for an INTVAR_EVENT.
	// This is only valid if IsIntVar() returns true.
	IntVar(BinlogFormat) (string, uint64, error)
	// Rand returns the two seed values for a RAND_EVENT.
	// This is only valid if IsRand() returns true.
	Rand(BinlogFormat) (uint64, uint64, error)

	// StripChecksum returns the checksum and a modified event with the checksum
	// stripped off, if any. If there is no checksum, it returns the same event
	// and a nil checksum.
	StripChecksum(BinlogFormat) (ev BinlogEvent, checksum []byte)
}

// BinlogFormat contains relevant data from the FORMAT_DESCRIPTION_EVENT.
// This structure is passed to subsequent event types to let them know how to
// parse themselves.
type BinlogFormat struct {
	// FormatVersion is the version number of the binlog file format.
	FormatVersion uint16
	// ServerVersion is the name of the MySQL server version.
	ServerVersion string
	// HeaderLength is the size in bytes of event headers other than FORMAT_DESCRIPTION_EVENT.
	HeaderLength byte
	// ChecksumAlgorithm is the ID number of the binlog checksum algorithm.
	ChecksumAlgorithm byte
}

// IsZero returns true if the BinlogFormat has not been initialized.
func (f BinlogFormat) IsZero() bool {
	return f.FormatVersion == 0 && f.HeaderLength == 0
}

// Query contains data from a QUERY_EVENT.
type Query struct {
	Database string
	Charset  *mproto.Charset
	Sql      []byte
}

// String pretty-prints a Query.
func (q Query) String() string {
	return fmt.Sprintf("{Database: %q, Charset: %v, Sql: %q}",
		q.Database, q.Charset, string(q.Sql))
}
