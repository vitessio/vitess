// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package replication

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const perconaFlavorID = "Percona"

// parsePerconaGTID is registered as a GTID parser.
func parsePerconaGTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid MySQL 5.6 GTID (%v): expecting UUID:Sequence", s)
	}

	// Parse Server ID.
	sid, err := ParsePSID(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid MySQL 5.6 GTID Server ID (%v): %v", parts[0], err)
	}

	// Parse Sequence number.
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid MySQL 5.6 GTID Sequence number (%v): %v", parts[1], err)
	}

	return PerconaGTID{Server: sid, Sequence: seq}, nil
}

// PSID is the 16-byte unique ID of a MySQL 5.6 server.
type PSID [16]byte

// String prints an PSID in the form used by MySQL 5.6.
func (sid PSID) String() string {
	dst := []byte("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	hex.Encode(dst, sid[:4])
	hex.Encode(dst[9:], sid[4:6])
	hex.Encode(dst[14:], sid[6:8])
	hex.Encode(dst[19:], sid[8:10])
	hex.Encode(dst[24:], sid[10:16])
	return string(dst)
}

// ParsePSID parses an PSID in the form used by MySQL 5.6.
func ParsePSID(s string) (sid PSID, err error) {
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

// PerconaGTID implements GTID
type PerconaGTID struct {
	// Server is the SID of the server that originally committed the transaction.
	Server PSID
	// Sequence is the sequence number of the transaction within a given Server's
	// scope.
	Sequence int64
}

// String implements GTID.String().
func (gtid PerconaGTID) String() string {
	return fmt.Sprintf("%s:%d", gtid.Server, gtid.Sequence)
}

// Flavor implements GTID.Flavor().
func (gtid PerconaGTID) Flavor() string {
	return perconaFlavorID
}

// SequenceDomain implements GTID.SequenceDomain().
func (gtid PerconaGTID) SequenceDomain() interface{} {
	return nil
}

// SourceServer implements GTID.SourceServer().
func (gtid PerconaGTID) SourceServer() interface{} {
	return gtid.Server
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid PerconaGTID) SequenceNumber() interface{} {
	return gtid.Sequence
}

// GTIDSet implements GTID.GTIDSet().
func (gtid PerconaGTID) GTIDSet() GTIDSet {
	return PerconaGTIDSet{}.AddGTID(gtid)
}

func init() {
	gtidParsers[perconaFlavorID] = parsePerconaGTID
}
