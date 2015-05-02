// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"encoding/hex"
	"fmt"
)

const mysql56FlavorID = "MySQL56"

// SID is the 16-byte unique ID of a MySQL 5.6 server.
type SID [16]byte

// String prints an SID in the form used by MySQL 5.6.
func (sid SID) String() string {
	dst := []byte("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	hex.Encode(dst, sid[:4])
	hex.Encode(dst[9:], sid[4:6])
	hex.Encode(dst[14:], sid[6:8])
	hex.Encode(dst[19:], sid[8:10])
	hex.Encode(dst[24:], sid[10:16])
	return string(dst)
}

// ParseSID parses an SID in the form used by MySQL 5.6.
func ParseSID(s string) (sid SID, err error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return sid, fmt.Errorf("invalid MySQL 5.6 SID %q", s)
	}

	// Drop the dashes so we can just check the error of Decode once.
	b := make([]byte, 0, 32)
	b = append(b, s[:8]...)
	b = append(b, s[9:13]...)
	b = append(b, s[14:18]...)
	b = append(b, s[19:23]...)
	b = append(b, s[24:]...)

	if _, err := hex.Decode(sid[:], b); err != nil {
		return sid, fmt.Errorf("invalid MySQL 5.6 SID %q: %v", s, err)
	}
	return sid, nil
}

// Mysql56GTID implements GTID
type Mysql56GTID struct {
	// Server is the SID of the server that originally committed the transaction.
	Server SID
	// Sequence is the sequence number of the transaction within a given Server's
	// scope.
	Sequence int64
}

// String implements GTID.String().
func (gtid Mysql56GTID) String() string {
	return fmt.Sprintf("%s:%d", gtid.Server, gtid.Sequence)
}

// Flavor implements GTID.Flavor().
func (gtid Mysql56GTID) Flavor() string {
	return mysql56FlavorID
}

// SequenceDomain implements GTID.SequenceDomain().
func (gtid Mysql56GTID) SequenceDomain() interface{} {
	return nil
}

// SourceServer implements GTID.SourceServer().
func (gtid Mysql56GTID) SourceServer() interface{} {
	return gtid.Server
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid Mysql56GTID) SequenceNumber() interface{} {
	return gtid.Sequence
}

// GTIDSet implements GTID.GTIDSet().
func (gtid Mysql56GTID) GTIDSet() GTIDSet {
	return Mysql56GTIDSet{}.AddGTID(gtid)
}
