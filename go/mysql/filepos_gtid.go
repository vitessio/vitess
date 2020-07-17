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

	pos, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid FilePos GTID (%v): expecting pos to be an integer", s)
	}

	return filePosGTID{
		file: parts[0],
		pos:  pos,
	}, nil
}

// ParseFilePosGTIDSet is registered as a GTIDSet parser.
func ParseFilePosGTIDSet(s string) (GTIDSet, error) {
	gtid, err := parseFilePosGTID(s)
	if err != nil {
		return nil, err
	}
	return gtid.(filePosGTID), err
}

// filePosGTID implements GTID.
type filePosGTID struct {
	file string
	pos  int
}

// String implements GTID.String().
func (gtid filePosGTID) String() string {
	return fmt.Sprintf("%s:%d", gtid.file, gtid.pos)
}

// Flavor implements GTID.Flavor().
func (gtid filePosGTID) Flavor() string {
	return FilePosFlavorID
}

// SequenceDomain implements GTID.SequenceDomain().
func (gtid filePosGTID) SequenceDomain() interface{} {
	return nil
}

// SourceServer implements GTID.SourceServer().
func (gtid filePosGTID) SourceServer() interface{} {
	return nil
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid filePosGTID) SequenceNumber() interface{} {
	return nil
}

// GTIDSet implements GTID.GTIDSet().
func (gtid filePosGTID) GTIDSet() GTIDSet {
	return gtid
}

// ContainsGTID implements GTIDSet.ContainsGTID().
func (gtid filePosGTID) ContainsGTID(other GTID) bool {
	if other == nil {
		return true
	}
	filePosOther, ok := other.(filePosGTID)
	if !ok {
		return false
	}
	if filePosOther.file < gtid.file {
		return true
	}
	if filePosOther.file > gtid.file {
		return false
	}
	return filePosOther.pos <= gtid.pos
}

// Contains implements GTIDSet.Contains().
func (gtid filePosGTID) Contains(other GTIDSet) bool {
	if other == nil {
		return false
	}
	filePosOther, ok := other.(filePosGTID)
	if !ok {
		return false
	}
	return gtid.ContainsGTID(filePosOther)
}

// Equal implements GTIDSet.Equal().
func (gtid filePosGTID) Equal(other GTIDSet) bool {
	filePosOther, ok := other.(filePosGTID)
	if !ok {
		return false
	}
	return gtid == filePosOther
}

// AddGTID implements GTIDSet.AddGTID().
func (gtid filePosGTID) AddGTID(other GTID) GTIDSet {
	filePosOther, ok := other.(filePosGTID)
	if !ok {
		return gtid
	}
	return filePosOther
}

// Union implements GTIDSet.Union().
func (gtid filePosGTID) Union(other GTIDSet) GTIDSet {
	filePosOther, ok := other.(filePosGTID)
	if !ok || gtid.Contains(other) {
		return gtid
	}

	return filePosOther
}

// Last returns last filePosition
// For filePos based GTID we have only one position
// here we will just return the current filePos
func (gtid filePosGTID) Last() string {
	return gtid.String()
}

func init() {
	gtidParsers[FilePosFlavorID] = parseFilePosGTID
	gtidSetParsers[FilePosFlavorID] = ParseFilePosGTIDSet
	flavors[FilePosFlavorID] = newFilePosFlavor
}
