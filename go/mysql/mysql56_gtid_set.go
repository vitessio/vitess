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
	"bytes"
	"encoding/binary"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type interval struct {
	start, end int64
}

func (iv interval) contains(other interval) bool {
	return iv.start <= other.start && other.end <= iv.end
}

type intervalList []interval

// Len implements sort.Interface.
func (s intervalList) Len() int { return len(s) }

// Less implements sort.Interface.
func (s intervalList) Less(i, j int) bool { return s[i].start < s[j].start }

// Swap implements sort.Interface.
func (s intervalList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func parseInterval(s string) (interval, error) {
	parts := strings.Split(s, "-")
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return interval{}, vterrors.Wrapf(err, "invalid interval (%q)", s)
	}
	if start < 1 {
		return interval{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid interval (%q): start must be > 0", s)
	}

	switch len(parts) {
	case 1:
		return interval{start: start, end: start}, nil
	case 2:
		end, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return interval{}, vterrors.Wrapf(err, "invalid interval (%q)", s)
		}
		return interval{start: start, end: end}, nil
	default:
		return interval{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid interval (%q): expected start-end or single number", s)
	}
}

// parseMysql56GTIDSet is registered as a GTIDSet parser.
//
// https://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
func parseMysql56GTIDSet(s string) (GTIDSet, error) {
	set := Mysql56GTIDSet{}

	// gtid_set: uuid_set [, uuid_set] ...
	for _, uuidSet := range strings.Split(s, ",") {
		uuidSet = strings.TrimSpace(uuidSet)
		if uuidSet == "" {
			continue
		}

		// uuid_set: uuid:interval[:interval]...
		parts := strings.Split(uuidSet, ":")
		if len(parts) < 2 {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid MySQL 5.6 GTID set (%q): expected uuid:interval", s)
		}

		// Parse Server ID.
		sid, err := ParseSID(parts[0])
		if err != nil {
			return nil, vterrors.Wrapf(err, "invalid MySQL 5.6 GTID set (%q)", s)
		}

		// Parse Intervals.
		intervals := make([]interval, 0, len(parts)-1)
		for _, part := range parts[1:] {
			iv, err := parseInterval(part)
			if err != nil {
				return nil, vterrors.Wrapf(err, "invalid MySQL 5.6 GTID set (%q)", s)
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
		sort.Sort(intervalList(intervals))
		set[sid] = intervals
	}

	return set, nil
}

// Mysql56GTIDSet implements GTIDSet for MySQL 5.6.
type Mysql56GTIDSet map[SID][]interval

// SIDs returns a sorted list of SIDs in the set.
func (set Mysql56GTIDSet) SIDs() []SID {
	sids := make([]SID, 0, len(set))
	for sid := range set {
		sids = append(sids, sid)
	}
	sort.Sort(sidList(sids))
	return sids
}

type sidList []SID

// Len implements sort.Interface.
func (s sidList) Len() int { return len(s) }

// Less implements sort.Interface.
func (s sidList) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }

// Swap implements sort.Interface.
func (s sidList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// String implements GTIDSet.
func (set Mysql56GTIDSet) String() string {
	buf := &bytes.Buffer{}

	for i, sid := range set.SIDs() {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(sid.String())

		for _, interval := range set[sid] {
			buf.WriteByte(':')
			buf.WriteString(strconv.FormatInt(interval.start, 10))

			if interval.end != interval.start {
				buf.WriteByte('-')
				buf.WriteString(strconv.FormatInt(interval.end, 10))
			}
		}
	}

	return buf.String()
}

// Last returns the last gtid as string
// For gtidset having multiple SIDs or multiple intervals
// it just returns the last SID with last interval
func (set Mysql56GTIDSet) Last() string {
	buf := &bytes.Buffer{}

	if len(set.SIDs()) > 0 {
		sid := set.SIDs()[len(set.SIDs())-1]
		buf.WriteString(sid.String())
		sequences := set[sid]
		if len(sequences) > 0 {
			buf.WriteByte(':')
			lastInterval := sequences[len(sequences)-1]
			buf.WriteString(strconv.FormatInt(lastInterval.end, 10))
		}
	}

	return buf.String()
}

// Flavor implements GTIDSet.
func (Mysql56GTIDSet) Flavor() string { return Mysql56FlavorID }

// ContainsGTID implements GTIDSet.
func (set Mysql56GTIDSet) ContainsGTID(gtid GTID) bool {
	gtid56, ok := gtid.(Mysql56GTID)
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
func (set Mysql56GTIDSet) Contains(other GTIDSet) bool {
	if other == nil {
		return false
	}

	other56, ok := other.(Mysql56GTIDSet)
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
func (set Mysql56GTIDSet) Equal(other GTIDSet) bool {
	other56, ok := other.(Mysql56GTIDSet)
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
func (set Mysql56GTIDSet) AddGTID(gtid GTID) GTIDSet {
	gtid56, ok := gtid.(Mysql56GTID)
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
	newSet := make(Mysql56GTIDSet)

	added := false

	for sid, intervals := range set {
		newIntervals := make([]interval, 0, len(intervals))

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
						newIntervals = append(newIntervals, interval{start: gtid56.Sequence, end: gtid56.Sequence})
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
		newSet[gtid56.Server] = append(newSet[gtid56.Server], interval{start: gtid56.Sequence, end: gtid56.Sequence})
	}

	return newSet
}

// Union implements GTIDSet.Union().
func (set Mysql56GTIDSet) Union(other GTIDSet) GTIDSet {
	if set == nil && other != nil {
		return other
	}
	if set == nil || other == nil {
		return set
	}
	mydbOther, ok := other.(Mysql56GTIDSet)
	if !ok {
		return set
	}

	// Make a copy and add the new GTID in the proper place.
	// This function is not supposed to modify the original set.
	newSet := make(Mysql56GTIDSet)

	for otherSID, otherIntervals := range mydbOther {
		intervals, ok := set[otherSID]
		if !ok {
			// No matching server id, so we must add it from other set.
			newSet[otherSID] = otherIntervals
			continue
		}

		// Found server id match between sets, so now we need to add each interval.
		s1 := intervals
		s2 := otherIntervals
		var nextInterval interval
		var newIntervals []interval

		// While our stacks have intervals to process, do work.
		for popInterval(&nextInterval, &s1, &s2) {
			if len(newIntervals) == 0 {
				newIntervals = append(newIntervals, nextInterval)
				continue
			}

			activeInterval := &newIntervals[len(newIntervals)-1]

			if nextInterval.end <= activeInterval.end {
				// We hit an interval whose start was after or equal to the previous interval's start, but whose
				// end is prior to the active intervals end. Skip to next interval.
				continue
			}

			if nextInterval.start > activeInterval.end+1 {
				// We found a gap, so we need to start a new interval.
				newIntervals = append(newIntervals, nextInterval)
				continue
			}

			// Extend our active interval.
			activeInterval.end = nextInterval.end
		}

		newSet[otherSID] = newIntervals
	}

	// Add any intervals from SIDs that exist in caller set, but don't exist in other set.
	for sid, intervals := range set {
		if _, ok := newSet[sid]; !ok {
			newSet[sid] = intervals
		}
	}

	return newSet
}

// SIDBlock returns the binary encoding of a MySQL 5.6 GTID set as expected
// by internal commands that refer to an "SID block".
//
// e.g. https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
func (set Mysql56GTIDSet) SIDBlock() []byte {
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

// Difference will supply the difference between the receiver and supplied Mysql56GTIDSets, and supply the result
// as a Mysql56GTIDSet.
func (set Mysql56GTIDSet) Difference(other Mysql56GTIDSet) Mysql56GTIDSet {
	if other == nil || set == nil {
		return set
	}

	// Make a fresh, empty set to hold the new value.
	// This function is not supposed to modify the original set.
	differenceSet := make(Mysql56GTIDSet)

	for sid, intervals := range set {
		otherIntervals, ok := other[sid]
		if !ok {
			// We didn't find SID in other set, so diff should include all intervals for sid unique to receiver.
			differenceSet[sid] = intervals
			continue
		}

		// Found server id match between sets, so now we need to subtract each interval.
		var diffIntervals []interval
		advance := func() bool {
			if len(intervals) == 0 {
				return false
			}
			diffIntervals = append(diffIntervals, intervals[0])
			intervals = intervals[1:]
			return true
		}

		var otherInterval interval
		advanceOther := func() bool {
			if len(otherIntervals) == 0 {
				return false
			}
			otherInterval = otherIntervals[0]
			otherIntervals = otherIntervals[1:]
			return true
		}

		if !advance() {
			continue
		}
		if !advanceOther() {
			differenceSet[sid] = intervals
			continue
		}

	diffLoop:
		for {
			iv := diffIntervals[len(diffIntervals)-1]

			switch {
			case iv.end < otherInterval.start:
				// [1, 2] - [3, 5]
				// Need to skip to next s1 interval. This one is completely before otherInterval even starts. It's a diff in whole.
				if !advance() {
					break diffLoop
				}

			case iv.start > otherInterval.end:
				// [3, 5] - [1, 2]
				// Interval is completely past other interval. We need a valid other to compare against.
				if !advanceOther() {
					break diffLoop
				}

			case iv.start >= otherInterval.start && iv.end <= otherInterval.end:
				// [3, 4] - [1, 5]
				// Interval is completed contained. Pop off diffIntervals, and advance to next s1.
				diffIntervals = diffIntervals[:len(diffIntervals)-1]
				if !advance() {
					break diffLoop
				}

			case iv.start < otherInterval.start && iv.end >= otherInterval.start && iv.end <= otherInterval.end:
				// [1, 4] - [3, 5]
				// We have a unique interval prior to where otherInterval starts and should adjust end to match this piece.
				diffIntervals[len(diffIntervals)-1].end = otherInterval.start - 1

				if !advance() {
					break diffLoop
				}

			case iv.start >= otherInterval.start && iv.start <= otherInterval.end && iv.end > otherInterval.end:
				// [3, 7] - [1, 5]
				// We have an end piece to deal with.
				diffIntervals[len(diffIntervals)-1].start = otherInterval.end + 1

				// We need to pop s2 at this point. s1's new interval is fully past otherInterval, so no point in comparing
				// this one next round.
				if !advanceOther() {
					break diffLoop
				}

			case iv.start < otherInterval.start && iv.end > otherInterval.end:
				// [1, 7] - [3, 4]
				// End is strictly greater. In this case we need to create an extra diff interval. We'll deal with any necessary trimming of it next round.
				diffIntervals[len(diffIntervals)-1].end = otherInterval.start - 1
				diffIntervals = append(diffIntervals, interval{start: otherInterval.end + 1, end: iv.end})

				// We need to pop s2 at this point. s1's new interval is fully past otherInterval, so no point in comparing
				// this one next round.
				if !advanceOther() {
					break diffLoop
				}

			default:
				panic("This should never happen.")
			}
		}

		if len(intervals) != 0 {
			// If we've gotten to this point, then we have intervals that exist beyond the bounds of any intervals in otherIntervals, and they
			// are all diffs and should be added in whole.
			diffIntervals = append(diffIntervals, intervals...)
		}

		differenceSet[sid] = diffIntervals
	}

	return differenceSet
}

// NewMysql56GTIDSetFromSIDBlock builds a Mysql56GTIDSet from parsing a SID Block.
// This is the reverse of the SIDBlock method.
//
// Expected format:
//   # bytes field
//   8       nSIDs
// (nSIDs times)
//   16      SID
//   8       nIntervals
// (nIntervals times)
//   8       start
//   8       end
func NewMysql56GTIDSetFromSIDBlock(data []byte) (Mysql56GTIDSet, error) {
	buf := bytes.NewReader(data)
	var set Mysql56GTIDSet = make(map[SID][]interval)
	var nSIDs uint64
	if err := binary.Read(buf, binary.LittleEndian, &nSIDs); err != nil {
		return nil, vterrors.Wrapf(err, "cannot read nSIDs")
	}
	for i := uint64(0); i < nSIDs; i++ {
		var sid SID
		if c, err := buf.Read(sid[:]); err != nil || c != 16 {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "cannot read SID %v: %v %v", i, err, c)
		}
		var nIntervals uint64
		if err := binary.Read(buf, binary.LittleEndian, &nIntervals); err != nil {
			return nil, vterrors.Wrapf(err, "cannot read nIntervals %v", i)
		}
		for j := uint64(0); j < nIntervals; j++ {
			var start, end uint64
			if err := binary.Read(buf, binary.LittleEndian, &start); err != nil {
				return nil, vterrors.Wrapf(err, "cannot read start %v/%v", i, j)
			}
			if err := binary.Read(buf, binary.LittleEndian, &end); err != nil {
				return nil, vterrors.Wrapf(err, "cannot read end %v/%v", i, j)
			}
			set[sid] = append(set[sid], interval{
				start: int64(start),
				end:   int64(end - 1),
			})
		}
	}
	return set, nil
}

// popInterval will look at the two pre-sorted interval stacks supplied, and if at least one of the stacks is non-empty
// will mutate the destination interval with the next earliest interval based on start sequence.
// popInterval will return true if we were able to pop, or false if both stacks are now empty.
func popInterval(dst *interval, s1, s2 *[]interval) bool {
	if len(*s1) == 0 && len(*s2) == 0 {
		return false
	}

	// Find which intervals list has earliest start.
	if len(*s2) == 0 || (len(*s1) != 0 && (*s1)[0].start <= (*s2)[0].start) {
		*dst = (*s1)[0]
		// Progress pointer since this stack has earliest.
		*s1 = (*s1)[1:]
	} else {
		*dst = (*s2)[0]
		// Progress pointer since this stack has earliest.
		*s2 = (*s2)[1:]
	}

	return true
}

func init() {
	gtidSetParsers[Mysql56FlavorID] = parseMysql56GTIDSet
}
