// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"strconv"
	"strings"
)

const googleMysqlFlavorID = "GoogleMysql"

// parseGoogleGTID is registered as a GTID parser.
func parseGoogleGTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid Google MySQL GTID (%v): expecting ServerID-GroupID", s)
	}

	server_id, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid Google MySQL server_id (%v): %v", parts[0], err)
	}

	group_id, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid Google MySQL group_id (%v): %v", parts[1], err)
	}

	return GoogleGTID{ServerID: uint32(server_id), GroupID: group_id}, nil
}

// parseGoogleGTIDSet is registered as a GTIDSet parser.
func parseGoogleGTIDSet(s string) (GTIDSet, error) {
	gtid, err := parseGoogleGTID(s)
	return gtid.(GoogleGTID), err
}

// GoogleGTID implements GTID and GTIDSet. In Google MySQL, a single GTID is
// already enough to define the set of all GTIDs that came before it.
type GoogleGTID struct {
	// ServerID is the server_id of the server that originally generated the
	// transaction.
	ServerID uint32
	// GroupID is the unique ID of a transaction group.
	GroupID uint64
}

// String implements GTID.String(). Google MySQL doesn't define a canonical way
// to represent both a group_id and a server_id together, so we've invented one.
func (gtid GoogleGTID) String() string {
	return fmt.Sprintf("%d-%d", gtid.ServerID, gtid.GroupID)
}

// Flavor implements GTID.Flavor().
func (gtid GoogleGTID) Flavor() string {
	return googleMysqlFlavorID
}

// Domain implements GTID.SequenceDomain().
func (gtid GoogleGTID) SequenceDomain() string {
	return ""
}

// SourceServer implements GTID.SourceServer().
func (gtid GoogleGTID) SourceServer() string {
	return strconv.FormatUint(uint64(gtid.ServerID), 10)
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid GoogleGTID) SequenceNumber() uint64 {
	return gtid.GroupID
}

// GTIDSet implements GTID.GTIDSet().
func (gtid GoogleGTID) GTIDSet() GTIDSet {
	return gtid
}

// Last implements GTIDSet.Last().
func (gtid GoogleGTID) Last() GTID {
	return gtid
}

// ContainsGTID implements GTIDSet.ContainsGTID().
func (gtid GoogleGTID) ContainsGTID(other GTID) bool {
	if other == nil {
		return true
	}
	gOther, ok := other.(GoogleGTID)
	if !ok {
		return false
	}
	return gtid.GroupID >= gOther.GroupID
}

// Contains implements GTIDSet.Contains().
func (gtid GoogleGTID) Contains(other GTIDSet) bool {
	if other == nil {
		return true
	}
	gOther, ok := other.(GoogleGTID)
	if !ok {
		return false
	}
	return gtid.GroupID >= gOther.GroupID
}

// Equal implements GTIDSet.Equal().
func (gtid GoogleGTID) Equal(other GTIDSet) bool {
	gOther, ok := other.(GoogleGTID)
	if !ok {
		return false
	}
	return gtid.GroupID == gOther.GroupID
}

// AddGTID implements GTIDSet.AddGTID().
func (gtid GoogleGTID) AddGTID(other GTID) GTIDSet {
	gOther, ok := other.(GoogleGTID)
	if !ok || gtid.GroupID >= gOther.GroupID {
		return gtid
	}
	return gOther
}

func init() {
	gtidParsers[googleMysqlFlavorID] = parseGoogleGTID
	gtidSetParsers[googleMysqlFlavorID] = parseGoogleGTIDSet
}
