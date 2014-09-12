// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"strconv"
	"strings"
)

const mariadbFlavorID = "MariaDB"

// parseMariadbGTID is registered as a GTID parser.
func parseMariadbGTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid MariaDB GTID (%v): expecting Domain-Server-Sequence", s)
	}

	// Parse Domain ID.
	Domain, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID Domain ID (%v): %v", parts[0], err)
	}

	// Parse Server ID.
	Server, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID Server ID (%v): %v", parts[1], err)
	}

	// Parse Sequence number.
	Sequence, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID Sequence number (%v): %v", parts[2], err)
	}

	return MariadbGTID{
		Domain:   uint32(Domain),
		Server:   uint32(Server),
		Sequence: Sequence,
	}, nil
}

// parseMariadbGTIDSet is registered as a GTIDSet parser.
func parseMariadbGTIDSet(s string) (GTIDSet, error) {
	gtid, err := parseMariadbGTID(s)
	if err != nil {
		return nil, err
	}
	return gtid.(MariadbGTID), err
}

// MariadbGTID implements GTID.
type MariadbGTID struct {
	// Domain is the ID number of the domain within which sequence numbers apply.
	Domain uint32
	// Server is the ID of the server that generated the transaction.
	Server uint32
	// Sequence is the sequence number of the transaction within the domain.
	Sequence uint64
}

// String implements GTID.String().
func (gtid MariadbGTID) String() string {
	return fmt.Sprintf("%d-%d-%d", gtid.Domain, gtid.Server, gtid.Sequence)
}

// Flavor implements GTID.Flavor().
func (gtid MariadbGTID) Flavor() string {
	return mariadbFlavorID
}

// SequenceDomain implements GTID.SequenceDomain().
func (gtid MariadbGTID) SequenceDomain() string {
	return strconv.FormatUint(uint64(gtid.Domain), 10)
}

// SourceServer implements GTID.SourceServer().
func (gtid MariadbGTID) SourceServer() string {
	return strconv.FormatUint(uint64(gtid.Server), 10)
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid MariadbGTID) SequenceNumber() uint64 {
	return gtid.Sequence
}

// GTIDSet implements GTID.GTIDSet().
func (gtid MariadbGTID) GTIDSet() GTIDSet {
	return gtid
}

// Last implements GTIDSet.Last().
func (gtid MariadbGTID) Last() GTID {
	return gtid
}

// ContainsGTID implements GTIDSet.ContainsGTID().
func (gtid MariadbGTID) ContainsGTID(other GTID) bool {
	if other == nil {
		return true
	}
	mdbOther, ok := other.(MariadbGTID)
	if !ok || gtid.Domain != mdbOther.Domain {
		return false
	}
	return gtid.Sequence >= mdbOther.Sequence
}

// Contains implements GTIDSet.Contains().
func (gtid MariadbGTID) Contains(other GTIDSet) bool {
	if other == nil {
		return true
	}
	mdbOther, ok := other.(MariadbGTID)
	if !ok || gtid.Domain != mdbOther.Domain {
		return false
	}
	return gtid.Sequence >= mdbOther.Sequence
}

// Equal implements GTIDSet.Equal().
func (gtid MariadbGTID) Equal(other GTIDSet) bool {
	mdbOther, ok := other.(MariadbGTID)
	if !ok {
		return false
	}
	return gtid == mdbOther
}

// AddGTID implements GTIDSet.AddGTID().
func (gtid MariadbGTID) AddGTID(other GTID) GTIDSet {
	mdbOther, ok := other.(MariadbGTID)
	if !ok || gtid.Domain != mdbOther.Domain || gtid.Sequence >= mdbOther.Sequence {
		return gtid
	}
	return mdbOther
}

func init() {
	gtidParsers[mariadbFlavorID] = parseMariadbGTID
	gtidSetParsers[mariadbFlavorID] = parseMariadbGTIDSet
}
