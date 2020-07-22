/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// MariadbFlavorID is the string identifier for the MariaDB flavor.
const MariadbFlavorID = "MariaDB"

// parseMariadbGTID is registered as a GTID parser.
func parseMariadbGTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid MariaDB GTID (%v): expecting Domain-Server-Sequence", s)
	}

	// Parse Domain ID.
	Domain, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, vterrors.Wrapf(err, "invalid MariaDB GTID Domain ID (%v)", parts[0])
	}

	// Parse Server ID.
	Server, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, vterrors.Wrapf(err, "invalid MariaDB GTID Server ID (%v)", parts[1])
	}

	// Parse Sequence number.
	Sequence, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, vterrors.Wrapf(err, "invalid MariaDB GTID Sequence number (%v)", parts[2])
	}

	return MariadbGTID{
		Domain:   uint32(Domain),
		Server:   uint32(Server),
		Sequence: Sequence,
	}, nil
}

// parseMariadbGTIDSet is registered as a GTIDSet parser.
func parseMariadbGTIDSet(s string) (GTIDSet, error) {
	gtidStrings := strings.Split(s, ",")
	gtidSet := make(MariadbGTIDSet, len(gtidStrings))
	for _, gtidString := range gtidStrings {
		gtid, err := parseMariadbGTID(gtidString)
		if err != nil {
			return nil, err
		}
		mdbGTID := gtid.(MariadbGTID)
		gtidSet[mdbGTID.Domain] = mdbGTID
	}
	return gtidSet, nil
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
	return MariadbFlavorID
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
	return MariadbGTIDSet{gtid.Domain: gtid}
}

// MariadbGTIDSet implements GTIDSet.
type MariadbGTIDSet map[uint32]MariadbGTID

// String implements GTIDSet.String()
func (gtidSet MariadbGTIDSet) String() string {
	// Sort domains so the string format is deterministic.
	domains := make([]uint32, 0, len(gtidSet))
	for domain := range gtidSet {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool {
		return domains[i] < domains[j]
	})

	// Convert each domain's GTID to a string and join all with comma.
	s := make([]string, len(gtidSet))
	for i, domain := range domains {
		s[i] = gtidSet[domain].String()
	}
	return strings.Join(s, ",")
}

// Flavor implements GTIDSet.Flavor()
func (gtidSet MariadbGTIDSet) Flavor() string {
	return MariadbFlavorID
}

// ContainsGTID implements GTIDSet.ContainsGTID().
func (gtidSet MariadbGTIDSet) ContainsGTID(other GTID) bool {
	if other == nil {
		return true
	}
	mdbOther, ok := other.(MariadbGTID)
	if !ok {
		return false
	}
	gtid, ok := gtidSet[mdbOther.Domain]
	if !ok {
		return false
	}
	return gtid.Sequence >= mdbOther.Sequence
}

// Contains implements GTIDSet.Contains().
func (gtidSet MariadbGTIDSet) Contains(other GTIDSet) bool {
	if other == nil {
		return false
	}
	mdbOther, ok := other.(MariadbGTIDSet)
	if !ok {
		return false
	}
	for _, gtid := range mdbOther {
		if !gtidSet.ContainsGTID(gtid) {
			return false
		}
	}
	return true
}

// Equal implements GTIDSet.Equal().
func (gtidSet MariadbGTIDSet) Equal(other GTIDSet) bool {
	mdbOther, ok := other.(MariadbGTIDSet)
	if !ok {
		return false
	}
	if len(gtidSet) != len(mdbOther) {
		return false
	}
	for domain, gtid := range gtidSet {
		otherGTID, ok := mdbOther[domain]
		if !ok {
			return false
		}
		if gtid != otherGTID {
			return false
		}
	}
	return true
}

// AddGTID implements GTIDSet.AddGTID().
func (gtidSet MariadbGTIDSet) AddGTID(other GTID) GTIDSet {
	if other == nil {
		return gtidSet
	}
	mdbOther, ok := other.(MariadbGTID)
	if !ok {
		return gtidSet
	}
	newSet := gtidSet.deepCopy()
	newSet.addGTID(mdbOther)
	return newSet
}

// Union implements GTIDSet.Union(). This is a pure method, and does not mutate the receiver.
func (gtidSet MariadbGTIDSet) Union(other GTIDSet) GTIDSet {
	if gtidSet == nil && other != nil {
		return other
	}
	if gtidSet == nil || other == nil {
		return gtidSet
	}

	mdbOther, ok := other.(MariadbGTIDSet)
	if !ok {
		return gtidSet
	}

	newSet := gtidSet.deepCopy()
	for _, otherGTID := range mdbOther {
		newSet.addGTID(otherGTID)
	}
	return newSet
}

//Last returns the last gtid
func (gtidSet MariadbGTIDSet) Last() string {
	// Sort domains so the string format is deterministic.
	domains := make([]uint32, 0, len(gtidSet))
	for domain := range gtidSet {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool {
		return domains[i] < domains[j]
	})

	lastGTID := domains[len(gtidSet)-1]
	return gtidSet[lastGTID].String()
}

// deepCopy returns a deep copy of the set.
func (gtidSet MariadbGTIDSet) deepCopy() MariadbGTIDSet {
	newSet := make(MariadbGTIDSet, len(gtidSet))
	for domain, gtid := range gtidSet {
		newSet[domain] = gtid
	}
	return newSet
}

// addGTID is an internal method that adds a GTID to the set.
// Unlike the exported methods, this mutates the receiver.
func (gtidSet MariadbGTIDSet) addGTID(otherGTID MariadbGTID) {
	gtid, ok := gtidSet[otherGTID.Domain]
	if !ok || otherGTID.Sequence > gtid.Sequence {
		gtidSet[otherGTID.Domain] = otherGTID
	}
}

func init() {
	gtidParsers[MariadbFlavorID] = parseMariadbGTID
	gtidSetParsers[MariadbFlavorID] = parseMariadbGTIDSet
}
