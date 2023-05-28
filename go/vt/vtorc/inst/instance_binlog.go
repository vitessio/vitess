/*
   Copyright 2014 Outbrain Inc.

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
	"regexp"
)

// Event entries may contains table IDs (can be different for same tables on different servers)
// and also COMMIT transaction IDs (different values on different servers).
// So these need to be removed from the event entry if we're to compare and validate matching
// entries.
var eventInfoTransformations = map[*regexp.Regexp]string{
	regexp.MustCompile(`(.*) [/][*].*?[*][/](.*$)`):  "$1 $2",         // strip comments
	regexp.MustCompile(`(COMMIT) .*$`):               "$1",            // commit number varies cross servers
	regexp.MustCompile(`(table_id:) [0-9]+ (.*$)`):   "$1 ### $2",     // table ids change cross servers
	regexp.MustCompile(`(table_id:) [0-9]+$`):        "$1 ###",        // table ids change cross servers
	regexp.MustCompile(` X'([0-9a-fA-F]+)' COLLATE`): " 0x$1 COLLATE", // different ways to represent collate
	regexp.MustCompile(`(BEGIN GTID [^ ]+) cid=.*`):  "$1",            // MariaDB GTID someimtes gets addition of "cid=...". Stripping
}

type BinlogEvent struct {
	Coordinates  BinlogCoordinates
	NextEventPos uint32
	EventType    string
	Info         string
}

func (binlogEvent *BinlogEvent) NextBinlogCoordinates() BinlogCoordinates {
	return BinlogCoordinates{LogFile: binlogEvent.Coordinates.LogFile, LogPos: binlogEvent.NextEventPos, Type: binlogEvent.Coordinates.Type}
}

func (binlogEvent *BinlogEvent) NormalizeInfo() {
	for reg, replace := range eventInfoTransformations {
		binlogEvent.Info = reg.ReplaceAllString(binlogEvent.Info, replace)
	}
}

func (binlogEvent *BinlogEvent) Equals(other *BinlogEvent) bool {
	return binlogEvent.Coordinates.Equals(&other.Coordinates) &&
		binlogEvent.NextEventPos == other.NextEventPos &&
		binlogEvent.EventType == other.EventType && binlogEvent.Info == other.Info
}

func (binlogEvent *BinlogEvent) EqualsIgnoreCoordinates(other *BinlogEvent) bool {
	return binlogEvent.NextEventPos == other.NextEventPos &&
		binlogEvent.EventType == other.EventType && binlogEvent.Info == other.Info
}
