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

// parseMariadbGTID is registered as a parser for ParseGTID().
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

type MariadbGTID struct {
	Domain   uint32
	Server   uint32
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

// TryCompare implements GTID.TryCompare().
func (gtid MariadbGTID) TryCompare(cmp GTID) (int, error) {
	other, ok := cmp.(MariadbGTID)
	if !ok {
		return 0, fmt.Errorf("can't compare GTID, wrong type: %#v.TryCompare(%#v)",
			gtid, cmp)
	}

	if gtid.Domain != other.Domain {
		return 0, fmt.Errorf("can't compare GTID, MariaDB Domain doesn't match: %v != %v", gtid.Domain, other.Domain)
	}
	if gtid.Server != other.Server {
		return 0, fmt.Errorf("can't compare GTID, MariaDB Server doesn't match: %v != %v", gtid.Server, other.Server)
	}

	switch true {
	case gtid.Sequence < other.Sequence:
		return -1, nil
	case gtid.Sequence > other.Sequence:
		return 1, nil
	default:
		return 0, nil
	}
}

func init() {
	gtidParsers[mariadbFlavorID] = parseMariadbGTID
}
