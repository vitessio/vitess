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
	"errors"
	"regexp"

	"vitess.io/vitess/go/vt/log"
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

const maxEmptyEventsEvents int = 10

type BinlogEventCursor struct {
	cachedEvents      []BinlogEvent
	currentEventIndex int
	fetchNextEvents   func(BinlogCoordinates) ([]BinlogEvent, error)
	nextCoordinates   BinlogCoordinates
}

// nextEvent will return the next event entry from binary logs; it will automatically skip to next
// binary log if need be.
// Internally, it uses the cachedEvents array, so that it does not go to the MySQL server upon each call.
// Returns nil upon reaching end of binary logs.
func (binlogEventCursor *BinlogEventCursor) nextEvent(numEmptyEventsEvents int) (*BinlogEvent, error) {
	if numEmptyEventsEvents > maxEmptyEventsEvents {
		log.Infof("End of logs. currentEventIndex: %d, nextCoordinates: %+v", binlogEventCursor.currentEventIndex, binlogEventCursor.nextCoordinates)
		// End of logs
		return nil, nil
	}
	if len(binlogEventCursor.cachedEvents) == 0 {
		// Cache exhausted; get next bulk of entries and return the next entry
		nextFileCoordinates, err := binlogEventCursor.nextCoordinates.NextFileCoordinates()
		if err != nil {
			return nil, err
		}
		log.Infof("zero cached events, next file: %+v", nextFileCoordinates)
		binlogEventCursor.cachedEvents, err = binlogEventCursor.fetchNextEvents(nextFileCoordinates)
		if err != nil {
			return nil, err
		}
		binlogEventCursor.currentEventIndex = -1
		// While this seems recursive do note that recursion level is at most 1, since we either have
		// entries in the next binlog (no further recursion) or we don't (immediate termination)
		return binlogEventCursor.nextEvent(numEmptyEventsEvents + 1)
	}
	if binlogEventCursor.currentEventIndex+1 < len(binlogEventCursor.cachedEvents) {
		// We have enough cache to go by
		binlogEventCursor.currentEventIndex++
		event := &binlogEventCursor.cachedEvents[binlogEventCursor.currentEventIndex]
		binlogEventCursor.nextCoordinates = event.NextBinlogCoordinates()
		return event, nil
	}
	// Cache exhausted; get next bulk of entries and return the next entry
	var err error
	binlogEventCursor.cachedEvents, err = binlogEventCursor.fetchNextEvents(binlogEventCursor.cachedEvents[len(binlogEventCursor.cachedEvents)-1].NextBinlogCoordinates())
	if err != nil {
		return nil, err
	}
	binlogEventCursor.currentEventIndex = -1
	// While this seems recursive do note that recursion level is at most 1, since we either have
	// entries in the next binlog (no further recursion) or we don't (immediate termination)
	return binlogEventCursor.nextEvent(numEmptyEventsEvents + 1)
}

// NextCoordinates return the binlog coordinates of the next entry as yet unprocessed by the cursor.
// Moreover, when the cursor terminates (consumes last entry), these coordinates indicate what will be the futuristic
// coordinates of the next binlog entry.
// The value of this function is used by match-below to move a replica behind another, after exhausting the shared binlog
// entries of both.
func (binlogEventCursor *BinlogEventCursor) getNextCoordinates() (BinlogCoordinates, error) {
	if binlogEventCursor.nextCoordinates.LogPos == 0 {
		return binlogEventCursor.nextCoordinates, errors.New("Next coordinates unfound")
	}
	return binlogEventCursor.nextCoordinates, nil
}
