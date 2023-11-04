/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package inst

import (
	"strings"
)

// OracleGtidSet represents a set of GTID ranges as depicted by Retrieved_Gtid_Set, Executed_Gtid_Set or @@gtid_purged.
type OracleGtidSet struct {
	GtidEntries []*OracleGtidSetEntry
}

// NewOracleGtidSet creates a new GTID set.
// Example input:  `230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10539,
// 316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-8935:8984-6124596,
// 321f5c0d-70e5-11e5-adb2-ecf4bb2262ff:1-56`
func NewOracleGtidSet(gtidSet string) (res *OracleGtidSet, err error) {
	res = &OracleGtidSet{}

	gtidSet = strings.TrimSpace(gtidSet)
	if gtidSet == "" {
		return res, nil
	}
	entries := strings.Split(gtidSet, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if gtidRange, err := NewOracleGtidSetEntry(entry); err == nil {
			res.GtidEntries = append(res.GtidEntries, gtidRange)
		} else {
			return res, err
		}
	}
	return res, nil
}

// RemoveUUID removes entries that belong to given UUID.
// By way of how this works there can only be one entry matching our UUID, but we generalize.
// We keep order of entries.
func (oracleGTIDSet *OracleGtidSet) RemoveUUID(uuid string) (removed bool) {
	var filteredEntries []*OracleGtidSetEntry
	for _, entry := range oracleGTIDSet.GtidEntries {
		if entry.UUID == uuid {
			removed = true
		} else {
			filteredEntries = append(filteredEntries, entry)
		}
	}
	if removed {
		oracleGTIDSet.GtidEntries = filteredEntries
	}
	return removed
}

// RetainUUID retains only entries that belong to given UUID.
func (oracleGTIDSet *OracleGtidSet) RetainUUID(uuid string) (anythingRemoved bool) {
	return oracleGTIDSet.RetainUUIDs([]string{uuid})
}

// RetainUUIDs retains only entries that belong to given UUIDs.
func (oracleGTIDSet *OracleGtidSet) RetainUUIDs(uuids []string) (anythingRemoved bool) {
	retainUUIDs := map[string]bool{}
	for _, uuid := range uuids {
		retainUUIDs[uuid] = true
	}
	var filteredEntries []*OracleGtidSetEntry
	for _, entry := range oracleGTIDSet.GtidEntries {
		if retainUUIDs[entry.UUID] {
			filteredEntries = append(filteredEntries, entry)
		} else {
			anythingRemoved = true
		}
	}
	if anythingRemoved {
		oracleGTIDSet.GtidEntries = filteredEntries
	}
	return anythingRemoved
}

// SharedUUIDs returns UUIDs (range-less) that are shared between the two sets
func (oracleGTIDSet *OracleGtidSet) SharedUUIDs(other *OracleGtidSet) (shared []string) {
	thisUUIDs := map[string]bool{}
	for _, entry := range oracleGTIDSet.GtidEntries {
		thisUUIDs[entry.UUID] = true
	}
	for _, entry := range other.GtidEntries {
		if thisUUIDs[entry.UUID] {
			shared = append(shared, entry.UUID)
		}
	}
	return shared
}

// Explode returns a user-friendly string representation of this entry
func (oracleGTIDSet *OracleGtidSet) Explode() (result []*OracleGtidSetEntry) {
	for _, entries := range oracleGTIDSet.GtidEntries {
		result = append(result, entries.Explode()...)
	}
	return result
}

func (oracleGTIDSet *OracleGtidSet) String() string {
	var tokens []string
	for _, entry := range oracleGTIDSet.GtidEntries {
		tokens = append(tokens, entry.String())
	}
	return strings.Join(tokens, ",")
}

func (oracleGTIDSet *OracleGtidSet) IsEmpty() bool {
	return len(oracleGTIDSet.GtidEntries) == 0
}
