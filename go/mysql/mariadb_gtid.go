/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

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
func (gtid MariadbGTID) SequenceDomain() interface{} {
	return gtid.Domain
}

// SourceServer implements GTID.SourceServer().
func (gtid MariadbGTID) SourceServer() interface{} {
	return gtid.Server
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid MariadbGTID) SequenceNumber() interface{} {
	return gtid.Sequence
}

// GTIDSet implements GTID.GTIDSet().
func (gtid MariadbGTID) GTIDSet() GTIDSet {
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
