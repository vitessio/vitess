// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type pinterval struct {
	start, end int64
}

func (iv pinterval) contains(other pinterval) bool {
	return iv.start <= other.start && other.end <= iv.end
}

type pintervalList []pinterval

// Len implements sort.Interface.
func (s pintervalList) Len() int { return len(s) }

// Less implements sort.Interface.
func (s pintervalList) Less(i, j int) bool { return s[i].start < s[j].start }

// Swap implements sort.Interface.
func (s pintervalList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func parseIntervalP(s string) (pinterval, error) {
	parts := strings.Split(s, "-")
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return pinterval{}, fmt.Errorf("invalid interval (%q): %v", s, err)
	}
	if start < 1 {
		return pinterval{}, fmt.Errorf("invalid interval (%q): start must be > 0", s)
	}

	switch len(parts) {
	case 1:
		return pinterval{start: start, end: start}, nil
	case 2:
		end, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return pinterval{}, fmt.Errorf("invalid interval (%q): %v", s, err)
		}
		return pinterval{start: start, end: end}, nil
	default:
		return pinterval{}, fmt.Errorf("invalid interval (%q): expected start-end or single number", s)
	}
}

// parsePerconaGTIDSet is registered as a GTIDSet parser.
//
// https://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
func parsePerconaGTIDSet(s string) (GTIDSet, error) {
	set := PerconaGTIDSet{}

	// gtid_set: uuid_set [, uuid_set] ...
	for _, uuidSet := range strings.Split(s, ",") {
		uuidSet = strings.TrimSpace(uuidSet)
		if uuidSet == "" {
			continue
		}

		// uuid_set: uuid:interval[:interval]...
		parts := strings.Split(uuidSet, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid MySQL 5.6 GTID set (%q): expected uuid:interval", s)
		}

		// Parse Server ID.
		sid, err := ParsePSID(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid MySQL 5.6 GTID set (%q): %v", s, err)
		}

		// Parse Intervals.
		intervals := make([]pinterval, 0, len(parts)-1)
		for _, part := range parts[1:] {
			iv, err := parseIntervalP(part)
			if err != nil {
				return nil, fmt.Errorf("invalid MySQL 5.6 GTID set (%q): %v", s, err)
			}
			if iv.end < iv.start {
				// According to MySQL 5.6 code:
				//   "The end of an interval may be 0, but any interval that has an
				//    endpoint that is smaller than the start is discarded."
				continue
			}
			intervals = append(intervals, iv)
		}
		if len(intervals) == 0 {
			// We might have discarded all the intervals.
			continue
		}

		// Internally we expect intervals to be stored in order.
		sort.Sort(pintervalList(intervals))
		set[sid] = intervals
	}

	return set, nil
}

// PerconaGTIDSet implements GTIDSet for MySQL 5.6.
type PerconaGTIDSet map[PSID][]pinterval

// SIDs returns a sorted list of SIDs in the set.
func (set PerconaGTIDSet) SIDs() []PSID {
	sids := make([]PSID, 0, len(set))
	for sid := range set {
		sids = append(sids, sid)
	}
	sort.Sort(psidList(sids))
	return sids
}

type psidList []PSID

// Len implements sort.Interface.
func (s psidList) Len() int { return len(s) }

// Less implements sort.Interface.
func (s psidList) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }

// Swap implements sort.Interface.
func (s psidList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// String implements GTIDSet.
func (set PerconaGTIDSet) String() string {
	buf := &bytes.Buffer{}

	for i, sid := range set.SIDs() {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(sid.String())

		for _, pinterval := range set[sid] {
			buf.WriteByte(':')
			buf.WriteString(strconv.FormatInt(pinterval.start, 10))

			if pinterval.end != pinterval.start {
				buf.WriteByte('-')
				buf.WriteString(strconv.FormatInt(pinterval.end, 10))
			}
		}
	}

	return buf.String()
}

// Flavor implements GTIDSet.
func (PerconaGTIDSet) Flavor() string { return perconaFlavorID }

// ContainsGTID implements GTIDSet.
func (set PerconaGTIDSet) ContainsGTID(gtid GTID) bool {
	gtid56, ok := gtid.(PerconaGTID)
	if !ok {
		return false
	}

	for _, iv := range set[gtid56.Server] {
		if iv.start > gtid56.Sequence {
			// We assume intervals are sorted, so we can skip the rest.
			return false
		}
		if gtid56.Sequence <= iv.end {
			// Now we know that: start <= Sequence <= end.
			return true
		}
	}
	// Server wasn't in the set, or no interval contained gtid.
	return false
}

// Contains implements GTIDSet.
func (set PerconaGTIDSet) Contains(other GTIDSet) bool {
	other56, ok := other.(PerconaGTIDSet)
	if !ok {
		return false
	}

	// Check each SID in the other set.
	for sid, otherIntervals := range other56 {
		i := 0
		intervals := set[sid]
		count := len(intervals)

		// Check each interval for this SID in the other set.
		for _, iv := range otherIntervals {
			// Check that interval against each of our intervals.
			// Intervals are monotonically increasing,
			// so we don't need to reset the index each time.
			for {
				if i >= count {
					// We ran out of intervals to check against.
					return false
				}
				if intervals[i].contains(iv) {
					// Yes it's covered. Go on to the next one.
					break
				}
				i++
			}
		}
	}

	// No uncovered intervals were found.
	return true
}

// Equal implements GTIDSet.
func (set PerconaGTIDSet) Equal(other GTIDSet) bool {
	other56, ok := other.(PerconaGTIDSet)
	if !ok {
		return false
	}

	// Check for same number of SIDs.
	if len(set) != len(other56) {
		return false
	}

	// Compare each SID.
	for sid, intervals := range set {
		otherIntervals := other56[sid]

		// Check for same number of intervals.
		if len(intervals) != len(otherIntervals) {
			return false
		}

		// Compare each interval.
		// Since intervals are sorted, they have to be in the same order.
		for i, iv := range intervals {
			if iv != otherIntervals[i] {
				return false
			}
		}
	}

	// No discrepancies were found.
	return true
}

// AddGTID implements GTIDSet.
func (set PerconaGTIDSet) AddGTID(gtid GTID) GTIDSet {
	gtid56, ok := gtid.(PerconaGTID)
	if !ok {
		return set
	}

	// If it's already in the set, we can return the same instance.
	// This is safe because GTIDSets are immutable.
	if set.ContainsGTID(gtid) {
		return set
	}

	// Make a copy and add the new GTID in the proper place.
	// This function is not supposed to modify the original set.
	newSet := make(PerconaGTIDSet)

	added := false

	for sid, intervals := range set {
		newIntervals := make([]pinterval, 0, len(intervals))

		if sid == gtid56.Server {
			// Look for the right place to add this GTID.
			for _, iv := range intervals {
				if !added {
					switch {
					case gtid56.Sequence == iv.start-1:
						// Expand the interval at the beginning.
						iv.start = gtid56.Sequence
						added = true
					case gtid56.Sequence == iv.end+1:
						// Expand the interval at the end.
						iv.end = gtid56.Sequence
						added = true
					case gtid56.Sequence < iv.start-1:
						// The next interval is beyond the new GTID, but it can't
						// be expanded, so we have to insert a new interval.
						newIntervals = append(newIntervals, pinterval{start: gtid56.Sequence, end: gtid56.Sequence})
						added = true
					}
				}
				// Check if this interval can be merged with the previous one.
				count := len(newIntervals)
				if count != 0 && iv.start == newIntervals[count-1].end+1 {
					// Merge instead of appending.
					newIntervals[count-1].end = iv.end
				} else {
					// Can't be merged.
					newIntervals = append(newIntervals, iv)
				}
			}
		} else {
			// Just copy everything.
			newIntervals = append(newIntervals, intervals...)
		}

		newSet[sid] = newIntervals
	}

	if !added {
		// There wasn't any place to insert the new GTID, so just append it
		// as a new interval.
		newSet[gtid56.Server] = append(newSet[gtid56.Server], pinterval{start: gtid56.Sequence, end: gtid56.Sequence})
	}

	return newSet
}

// SIDBlock returns the binary encoding of a MySQL 5.6 GTID set as expected
// by internal commands that refer to an "SID block".
//
// e.g. https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
func (set PerconaGTIDSet) SIDBlock() []byte {
	buf := &bytes.Buffer{}

	// Number of SIDs.
	binary.Write(buf, binary.LittleEndian, uint64(len(set)))

	for _, sid := range set.SIDs() {
		buf.Write(sid[:])

		// Number of intervals.
		intervals := set[sid]
		binary.Write(buf, binary.LittleEndian, uint64(len(intervals)))

		for _, iv := range intervals {
			binary.Write(buf, binary.LittleEndian, iv.start)
			// MySQL's internal form for intervals adds 1 to the end value.
			// See Gtid_set::add_gtid_text() in rpl_gtid_set.cc for example.
			binary.Write(buf, binary.LittleEndian, iv.end+1)
		}
	}

	return buf.Bytes()
}

func init() {
	gtidSetParsers[perconaFlavorID] = parsePerconaGTIDSet
}
