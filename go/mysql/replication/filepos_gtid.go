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

package replication

import (
	"fmt"
	"strconv"
	"strings"
)

// FilePosFlavorID is the string identifier for the filePos flavor.
const FilePosFlavorID = "FilePos"

// parsefilePosGTID is registered as a GTID parser.
func parseFilePosGTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid FilePos GTID (%v): expecting file:pos", s)
	}

	pos, err := strconv.ParseUint(parts[1], 0, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid FilePos GTID (%v): expecting pos to be an integer", s)
	}

	return FilePosGTID{
		File: parts[0],
		Pos:  uint32(pos),
	}, nil
}

// ParseFilePosGTIDSet is registered as a GTIDSet parser.
func ParseFilePosGTIDSet(s string) (GTIDSet, error) {
	gtid, err := parseFilePosGTID(s)
	if err != nil {
		return nil, err
	}
	return gtid.(FilePosGTID), err
}

// FilePosGTID implements GTID.
type FilePosGTID struct {
	File string
	Pos  uint32
}

// String implements GTID.String().
func (gtid FilePosGTID) String() string {
	return fmt.Sprintf("%s:%d", gtid.File, gtid.Pos)
}

// Flavor implements GTID.Flavor().
func (gtid FilePosGTID) Flavor() string {
	return FilePosFlavorID
}

// SequenceDomain implements GTID.SequenceDomain().
func (gtid FilePosGTID) SequenceDomain() any {
	return nil
}

// SourceServer implements GTID.SourceServer().
func (gtid FilePosGTID) SourceServer() any {
	return nil
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid FilePosGTID) SequenceNumber() any {
	return nil
}

// GTIDSet implements GTID.GTIDSet().
func (gtid FilePosGTID) GTIDSet() GTIDSet {
	return gtid
}

// ContainsGTID implements GTIDSet.ContainsGTID().
func (gtid FilePosGTID) ContainsGTID(other GTID) bool {
	if other == nil {
		return true
	}
	filePosOther, ok := other.(FilePosGTID)
	if !ok {
		return false
	}
	if filePosOther.File < gtid.File {
		return true
	}
	if filePosOther.File > gtid.File {
		return false
	}
	return filePosOther.Pos <= gtid.Pos
}

// Contains implements GTIDSet.Contains().
func (gtid FilePosGTID) Contains(other GTIDSet) bool {
	if other == nil {
		return false
	}
	filePosOther, ok := other.(FilePosGTID)
	if !ok {
		return false
	}
	return gtid.ContainsGTID(filePosOther)
}

// Equal implements GTIDSet.Equal().
func (gtid FilePosGTID) Equal(other GTIDSet) bool {
	filePosOther, ok := other.(FilePosGTID)
	if !ok {
		return false
	}
	return gtid == filePosOther
}

// AddGTID implements GTIDSet.AddGTID().
func (gtid FilePosGTID) AddGTID(other GTID) GTIDSet {
	filePosOther, ok := other.(FilePosGTID)
	if !ok {
		return gtid
	}
	return filePosOther
}

// Union implements GTIDSet.Union().
func (gtid FilePosGTID) Union(other GTIDSet) GTIDSet {
	filePosOther, ok := other.(FilePosGTID)
	if !ok || gtid.Contains(other) {
		return gtid
	}

	return filePosOther
}

// Last returns last filePosition
// For filePos based GTID we have only one position
// here we will just return the current filePos
func (gtid FilePosGTID) Last() string {
	return gtid.String()
}

func init() {
	gtidParsers[FilePosFlavorID] = parseFilePosGTID
	gtidSetParsers[FilePosFlavorID] = ParseFilePosGTIDSet
}
