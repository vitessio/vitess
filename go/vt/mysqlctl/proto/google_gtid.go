// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"strconv"
)

const googleMysqlFlavorID = "GoogleMysql"

// parseGoogleGTID is registered as a parser for ParseGTID().
func parseGoogleGTID(s string) (GTID, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid Google MySQL group_id (%v): %v", s, err)
	}

	return googleGTID{groupID: id}, nil
}

type googleGTID struct {
	groupID uint64
}

// String implements GTID.String().
func (gtid googleGTID) String() string {
	return fmt.Sprintf("%d", gtid.groupID)
}

// Flavor implements GTID.Flavor().
func (gtid googleGTID) Flavor() string {
	return googleMysqlFlavorID
}

// TryCompare implements GTID.TryCompare().
func (gtid googleGTID) TryCompare(cmp GTID) (int, error) {
	other, ok := cmp.(googleGTID)
	if !ok {
		return 0, fmt.Errorf("can't compare GTID, wrong type: %#v.TryCompare(%#v)",
			gtid, cmp)
	}

	switch true {
	case gtid.groupID < other.groupID:
		return -1, nil
	case gtid.groupID > other.groupID:
		return 1, nil
	default:
		return 0, nil
	}
}

func init() {
	gtidParsers[googleMysqlFlavorID] = parseGoogleGTID
}
