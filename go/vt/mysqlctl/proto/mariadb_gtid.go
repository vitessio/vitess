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
		return nil, fmt.Errorf("invalid MariaDB GTID (%v): expecting domain-server-sequence", s)
	}

	// Parse domain ID.
	domain, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID domain ID (%v): %v", parts[0], err)
	}

	// Parse server ID.
	server, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID server ID (%v): %v", parts[1], err)
	}

	// Parse sequence number.
	sequence, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID sequence number (%v): %v", parts[2], err)
	}

	return mariadbGTID{
		domain:   uint32(domain),
		server:   uint32(server),
		sequence: sequence,
	}, nil
}

type mariadbGTID struct {
	domain   uint32
	server   uint32
	sequence uint64
}

// String implements GTID.String().
func (gtid mariadbGTID) String() string {
	return fmt.Sprintf("%d-%d-%d", gtid.domain, gtid.server, gtid.sequence)
}

// Flavor implements GTID.Flavor().
func (gtid mariadbGTID) Flavor() string {
	return mariadbFlavorID
}

// TryCompare implements GTID.TryCompare().
func (gtid mariadbGTID) TryCompare(cmp GTID) (int, error) {
	other, ok := cmp.(mariadbGTID)
	if !ok {
		return 0, fmt.Errorf("can't compare GTID, wrong type: %#v.TryCompare(%#v)",
			gtid, cmp)
	}

	if gtid.domain != other.domain {
		return 0, fmt.Errorf("can't compare GTID, MariaDB domain doesn't match: %v != %v", gtid.domain, other.domain)
	}
	if gtid.server != other.server {
		return 0, fmt.Errorf("can't compare GTID, MariaDB server doesn't match: %v != %v", gtid.server, other.server)
	}

	switch true {
	case gtid.sequence < other.sequence:
		return -1, nil
	case gtid.sequence > other.sequence:
		return 1, nil
	default:
		return 0, nil
	}
}

func init() {
	gtidParsers[mariadbFlavorID] = parseMariadbGTID
}
