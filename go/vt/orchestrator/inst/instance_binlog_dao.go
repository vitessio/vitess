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
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/math"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

const maxEmptyBinlogFiles int = 10

var instanceBinlogEntryCache *cache.Cache

func init() {
	go initializeBinlogDaoPostConfiguration()
}

func initializeBinlogDaoPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()

	instanceBinlogEntryCache = cache.New(time.Duration(10)*time.Minute, time.Minute)
}

func compilePseudoGTIDPattern() (pseudoGTIDRegexp *regexp.Regexp, err error) {
	log.Debugf("PseudoGTIDPatternIsFixedSubstring: %+v", config.Config.PseudoGTIDPatternIsFixedSubstring)
	if config.Config.PseudoGTIDPatternIsFixedSubstring {
		return nil, nil
	}
	log.Debugf("Compiling PseudoGTIDPattern: %q", config.Config.PseudoGTIDPattern)
	return regexp.Compile(config.Config.PseudoGTIDPattern)
}

// pseudoGTIDMatches attempts to match given string with pseudo GTID pattern/text.
func pseudoGTIDMatches(pseudoGTIDRegexp *regexp.Regexp, binlogEntryInfo string) (found bool) {
	if config.Config.PseudoGTIDPatternIsFixedSubstring {
		return strings.Contains(binlogEntryInfo, config.Config.PseudoGTIDPattern)
	}
	return pseudoGTIDRegexp.MatchString(binlogEntryInfo)
}

func getInstanceBinlogEntryKey(instance *Instance, entry string) string {
	return fmt.Sprintf("%s;%s", instance.Key.DisplayString(), entry)
}

// Try and find the last position of a pseudo GTID query entry in the given binary log.
// Also return the full text of that entry.
// maxCoordinates is the position beyond which we should not read. This is relevant when reading relay logs; in particular,
// the last relay log. We must be careful not to scan for Pseudo-GTID entries past the position executed by the SQL thread.
// maxCoordinates == nil means no limit.
func getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp *regexp.Regexp, instanceKey *InstanceKey, binlog string, binlogType BinlogType, minCoordinates *BinlogCoordinates, maxCoordinates *BinlogCoordinates) (*BinlogCoordinates, string, error) {
	if binlog == "" {
		return nil, "", log.Errorf("getLastPseudoGTIDEntryInBinlog: empty binlog file name for %+v. maxCoordinates = %+v", *instanceKey, maxCoordinates)
	}
	binlogCoordinates := BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: binlogType}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, "", err
	}

	moreRowsExpected := true
	var nextPos int64 = 0
	var relyLogMinPos int64 = 0
	if minCoordinates != nil && minCoordinates.LogFile == binlog {
		log.Debugf("getLastPseudoGTIDEntryInBinlog: starting with %+v", *minCoordinates)
		nextPos = minCoordinates.LogPos
		relyLogMinPos = minCoordinates.LogPos
	}
	step := 0

	entryText := ""
	for moreRowsExpected {
		query := ""
		if binlogCoordinates.Type == BinaryLog {
			query = fmt.Sprintf("show binlog events in '%s' FROM %d LIMIT %d", binlog, nextPos, config.Config.BinlogEventsChunkSize)
		} else {
			query = fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT %d,%d", binlog, relyLogMinPos, (step * config.Config.BinlogEventsChunkSize), config.Config.BinlogEventsChunkSize)
		}

		moreRowsExpected = false

		err = sqlutils.QueryRowsMapBuffered(db, query, func(m sqlutils.RowMap) error {
			moreRowsExpected = true
			nextPos = m.GetInt64("End_log_pos")
			binlogEntryInfo := m.GetString("Info")
			if pseudoGTIDMatches(pseudoGTIDRegexp, binlogEntryInfo) {
				if maxCoordinates != nil && maxCoordinates.SmallerThan(&BinlogCoordinates{LogFile: binlog, LogPos: m.GetInt64("Pos")}) {
					// past the limitation
					moreRowsExpected = false
					return nil
				}
				binlogCoordinates.LogPos = m.GetInt64("Pos")
				entryText = binlogEntryInfo
				// Found a match. But we keep searching: we're interested in the LAST entry, and, alas,
				// we can only search in ASCENDING order...
			}
			return nil
		})
		if err != nil {
			return nil, "", err
		}
		step++
	}

	// Not found? return nil. an error is reserved to SQL problems.
	if binlogCoordinates.LogPos == 0 {
		return nil, "", nil
	}
	return &binlogCoordinates, entryText, err
}

// getLastPseudoGTIDEntryInInstance will search for the last pseudo GTID entry in an instance's binary logs. Arguments:
// - instance
// - minBinlogCoordinates: a hint, suggested coordinates to start with. The search will _attempt_ to begin search from
//   these coordinates, but if search is empty, then we failback to full search, ignoring this hint
// - maxBinlogCoordinates: a hard limit on the maximum position we're allowed to investigate.
// - exhaustiveSearch: when 'true', continue iterating binary logs. When 'false', only investigate most recent binary log.
func getLastPseudoGTIDEntryInInstance(instance *Instance, minBinlogCoordinates *BinlogCoordinates, maxBinlogCoordinates *BinlogCoordinates, exhaustiveSearch bool) (*BinlogCoordinates, string, error) {
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, "", err
	}
	// Look for last GTID in instance:
	currentBinlog := instance.SelfBinlogCoordinates

	err = nil
	for err == nil {
		log.Debugf("Searching for latest pseudo gtid entry in binlog %+v of %+v", currentBinlog.LogFile, instance.Key)
		resultCoordinates, entryInfo, err := getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentBinlog.LogFile, BinaryLog, minBinlogCoordinates, maxBinlogCoordinates)
		if err != nil {
			return nil, "", err
		}
		if resultCoordinates != nil {
			log.Debugf("Found pseudo gtid entry in %+v, %+v", instance.Key, resultCoordinates)
			return resultCoordinates, entryInfo, err
		}
		if !exhaustiveSearch {
			log.Debugf("Not an exhaustive search. Bailing out")
			break
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentBinlog.LogFile {
			// We tried and failed with the minBinlogCoordinates heuristic/hint. We no longer require it,
			// and continue with exhaustive search, on same binlog.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic binlog search failed; continuing exhaustive search")
			// And we do NOT iterate the log file: we scan same log file again, with no heuristic
			//return nil, "", log.Errorf("past minBinlogCoordinates (%+v); skipping iteration over rest of binary logs", *minBinlogCoordinates)
		} else {
			currentBinlog, err = currentBinlog.PreviousFileCoordinates()
			if err != nil {
				return nil, "", err
			}
		}
	}
	return nil, "", log.Errorf("Cannot find pseudo GTID entry in binlogs of %+v", instance.Key)
}

func getLastPseudoGTIDEntryInRelayLogs(instance *Instance, minBinlogCoordinates *BinlogCoordinates, recordedInstanceRelayLogCoordinates BinlogCoordinates, exhaustiveSearch bool) (*BinlogCoordinates, string, error) {
	// Look for last GTID in relay logs:
	// Since MySQL does not provide with a SHOW RELAY LOGS command, we heuristically start from current
	// relay log (indiciated by Relay_log_file) and walk backwards.
	// Eventually we will hit a relay log name which does not exist.
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, "", err
	}

	currentRelayLog := recordedInstanceRelayLogCoordinates
	err = nil
	for err == nil {
		log.Debugf("Searching for latest pseudo gtid entry in relaylog %+v of %+v, up to pos %+v", currentRelayLog.LogFile, instance.Key, recordedInstanceRelayLogCoordinates)
		if resultCoordinates, entryInfo, err := getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentRelayLog.LogFile, RelayLog, minBinlogCoordinates, &recordedInstanceRelayLogCoordinates); err != nil {
			return nil, "", err
		} else if resultCoordinates != nil {
			log.Debugf("Found pseudo gtid entry in %+v, %+v", instance.Key, resultCoordinates)
			return resultCoordinates, entryInfo, err
		}
		if !exhaustiveSearch {
			break
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentRelayLog.LogFile {
			// We tried and failed with the minBinlogCoordinates hint. We no longer require it,
			// and continue with exhaustive search.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic relaylog search failed; continuing exhaustive search")
			// And we do NOT iterate to previous log file: we scan same log file again, with no heuristic
		} else {
			currentRelayLog, err = currentRelayLog.PreviousFileCoordinates()
		}
	}
	return nil, "", log.Errorf("Cannot find pseudo GTID entry in relay logs of %+v", instance.Key)
}

func readBinlogEvent(binlogEvent *BinlogEvent, m sqlutils.RowMap) error {
	binlogEvent.NextEventPos = m.GetInt64("End_log_pos")
	binlogEvent.Coordinates.LogPos = m.GetInt64("Pos")
	binlogEvent.EventType = m.GetString("Event_type")
	binlogEvent.Info = m.GetString("Info")
	return nil
}

func ReadBinlogEventAtRelayLogCoordinates(instanceKey *InstanceKey, relaylogCoordinates *BinlogCoordinates) (binlogEvent *BinlogEvent, err error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT 1", relaylogCoordinates.LogFile, relaylogCoordinates.LogPos)
	binlogEvent = &BinlogEvent{
		Coordinates: *relaylogCoordinates,
	}
	err = sqlutils.QueryRowsMapBuffered(db, query, func(m sqlutils.RowMap) error {
		return readBinlogEvent(binlogEvent, m)
	})
	return binlogEvent, err
}

// Try and find the last position of a pseudo GTID query entry in the given binary log.
// Also return the full text of that entry.
// maxCoordinates is the position beyond which we should not read. This is relevant when reading relay logs; in particular,
// the last relay log. We must be careful not to scan for Pseudo-GTID entries past the position executed by the SQL thread.
// maxCoordinates == nil means no limit.
func getLastExecutedEntryInRelaylog(instanceKey *InstanceKey, binlog string, minCoordinates *BinlogCoordinates, maxCoordinates *BinlogCoordinates) (binlogEvent *BinlogEvent, err error) {
	if binlog == "" {
		return nil, log.Errorf("getLastExecutedEntryInRelaylog: empty binlog file name for %+v. maxCoordinates = %+v", *instanceKey, maxCoordinates)
	}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	binlogEvent = &BinlogEvent{
		Coordinates: BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: RelayLog},
	}

	moreRowsExpected := true
	var relyLogMinPos int64 = 0
	if minCoordinates != nil && minCoordinates.LogFile == binlog {
		log.Debugf("getLastExecutedEntryInRelaylog: starting with %+v", *minCoordinates)
		relyLogMinPos = minCoordinates.LogPos
	}

	step := 0
	for moreRowsExpected {
		query := fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT %d,%d", binlog, relyLogMinPos, (step * config.Config.BinlogEventsChunkSize), config.Config.BinlogEventsChunkSize)

		moreRowsExpected = false
		err = sqlutils.QueryRowsMapBuffered(db, query, func(m sqlutils.RowMap) error {
			moreRowsExpected = true
			return readBinlogEvent(binlogEvent, m)
		})
		if err != nil {
			return nil, err
		}
		step++
	}

	// Not found? return nil. an error is reserved to SQL problems.
	if binlogEvent.Coordinates.LogPos == 0 {
		return nil, nil
	}
	return binlogEvent, err
}

func GetLastExecutedEntryInRelayLogs(instance *Instance, minBinlogCoordinates *BinlogCoordinates, recordedInstanceRelayLogCoordinates BinlogCoordinates) (binlogEvent *BinlogEvent, err error) {
	// Look for last GTID in relay logs:
	// Since MySQL does not provide with a SHOW RELAY LOGS command, we heuristically start from current
	// relay log (indiciated by Relay_log_file) and walk backwards.

	currentRelayLog := recordedInstanceRelayLogCoordinates
	for err == nil {
		log.Debugf("Searching for latest entry in relaylog %+v of %+v, up to pos %+v", currentRelayLog.LogFile, instance.Key, recordedInstanceRelayLogCoordinates)
		if binlogEvent, err = getLastExecutedEntryInRelaylog(&instance.Key, currentRelayLog.LogFile, minBinlogCoordinates, &recordedInstanceRelayLogCoordinates); err != nil {
			return nil, err
		} else if binlogEvent != nil {
			log.Debugf("Found entry in %+v, %+v", instance.Key, binlogEvent.Coordinates)
			return binlogEvent, err
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentRelayLog.LogFile {
			// We tried and failed with the minBinlogCoordinates hint. We no longer require it,
			// and continue with exhaustive search.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic relaylog search failed; continuing exhaustive search")
			// And we do NOT iterate to previous log file: we scan same log faile again, with no heuristic
		} else {
			currentRelayLog, err = currentRelayLog.PreviousFileCoordinates()
		}
	}
	return binlogEvent, err
}

// SearchBinlogEntryInRelaylog
func searchEventInRelaylog(instanceKey *InstanceKey, binlog string, searchEvent *BinlogEvent, minCoordinates *BinlogCoordinates) (binlogCoordinates, nextCoordinates *BinlogCoordinates, found bool, err error) {
	binlogCoordinates = &BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: RelayLog}
	nextCoordinates = &BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: RelayLog}
	if binlog == "" {
		return binlogCoordinates, nextCoordinates, false, log.Errorf("SearchEventInRelaylog: empty relaylog file name for %+v", *instanceKey)
	}

	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return binlogCoordinates, nextCoordinates, false, err
	}

	moreRowsExpected := true
	var relyLogMinPos int64 = 0
	if minCoordinates != nil && minCoordinates.LogFile == binlog {
		log.Debugf("SearchEventInRelaylog: starting with %+v", *minCoordinates)
		relyLogMinPos = minCoordinates.LogPos
	}
	binlogEvent := &BinlogEvent{
		Coordinates: BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: RelayLog},
	}

	skipRestOfBinlog := false

	step := 0
	for moreRowsExpected {
		query := fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT %d,%d", binlog, relyLogMinPos, (step * config.Config.BinlogEventsChunkSize), config.Config.BinlogEventsChunkSize)

		// We don't know in advance when we will hit the end of the binlog. We will implicitly understand it when our
		// `show binlog events` query does not return any row.
		moreRowsExpected = false
		err = sqlutils.QueryRowsMapBuffered(db, query, func(m sqlutils.RowMap) error {
			if binlogCoordinates.LogPos != 0 && nextCoordinates.LogPos != 0 {
				// Entry found!
				skipRestOfBinlog = true
				return nil
			}
			if skipRestOfBinlog {
				return nil
			}
			moreRowsExpected = true

			if binlogCoordinates.LogPos == 0 {
				readBinlogEvent(binlogEvent, m)
				if binlogEvent.EqualsIgnoreCoordinates(searchEvent) {
					// found it!
					binlogCoordinates.LogPos = m.GetInt64("Pos")
				}
			} else if nextCoordinates.LogPos == 0 {
				// found binlogCoordinates: the next coordinates are nextCoordinates :P
				nextCoordinates.LogPos = m.GetInt64("Pos")
			}
			return nil
		})
		if err != nil {
			return binlogCoordinates, nextCoordinates, (binlogCoordinates.LogPos != 0), err
		}
		if skipRestOfBinlog {
			return binlogCoordinates, nextCoordinates, (binlogCoordinates.LogPos != 0), err
		}
		step++
	}
	return binlogCoordinates, nextCoordinates, (binlogCoordinates.LogPos != 0), err
}

func SearchEventInRelayLogs(searchEvent *BinlogEvent, instance *Instance, minBinlogCoordinates *BinlogCoordinates, recordedInstanceRelayLogCoordinates BinlogCoordinates) (binlogCoordinates, nextCoordinates *BinlogCoordinates, found bool, err error) {
	// Since MySQL does not provide with a SHOW RELAY LOGS command, we heuristically start from current
	// relay log (indiciated by Relay_log_file) and walk backwards.
	log.Debugf("will search for event %+v", *searchEvent)
	if minBinlogCoordinates != nil {
		log.Debugf("Starting with coordinates: %+v", *minBinlogCoordinates)
	}
	currentRelayLog := recordedInstanceRelayLogCoordinates
	for err == nil {
		log.Debugf("Searching for event in relaylog %+v of %+v, up to pos %+v", currentRelayLog.LogFile, instance.Key, recordedInstanceRelayLogCoordinates)
		if binlogCoordinates, nextCoordinates, found, err = searchEventInRelaylog(&instance.Key, currentRelayLog.LogFile, searchEvent, minBinlogCoordinates); err != nil {
			return nil, nil, false, err
		} else if binlogCoordinates != nil && found {
			log.Debugf("Found event in %+v, %+v", instance.Key, *binlogCoordinates)
			return binlogCoordinates, nextCoordinates, found, err
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentRelayLog.LogFile {
			// We tried and failed with the minBinlogCoordinates hint. We no longer require it,
			// and continue with exhaustive search.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic relaylog search failed; continuing exhaustive search")
			// And we do NOT iterate to previous log file: we scan same log faile again, with no heuristic
		} else {
			currentRelayLog, err = currentRelayLog.PreviousFileCoordinates()
		}
	}
	return binlogCoordinates, nextCoordinates, found, err
}

// SearchEntryInBinlog Given a binlog entry text (query), search it in the given binary log of a given instance
func SearchEntryInBinlog(pseudoGTIDRegexp *regexp.Regexp, instanceKey *InstanceKey, binlog string, entryText string, monotonicPseudoGTIDEntries bool, minBinlogCoordinates *BinlogCoordinates) (BinlogCoordinates, bool, error) {
	binlogCoordinates := BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: BinaryLog}
	if binlog == "" {
		return binlogCoordinates, false, log.Errorf("SearchEntryInBinlog: empty binlog file name for %+v", *instanceKey)
	}

	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return binlogCoordinates, false, err
	}

	moreRowsExpected := true
	skipRestOfBinlog := false
	alreadyMatchedAscendingPseudoGTID := false
	var nextPos int64 = 0
	if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == binlog {
		log.Debugf("SearchEntryInBinlog: starting with %+v", *minBinlogCoordinates)
		nextPos = minBinlogCoordinates.LogPos
	}

	for moreRowsExpected {
		query := fmt.Sprintf("show binlog events in '%s' FROM %d LIMIT %d", binlog, nextPos, config.Config.BinlogEventsChunkSize)

		// We don't know in advance when we will hit the end of the binlog. We will implicitly understand it when our
		// `show binlog events` query does not return any row.
		moreRowsExpected = false

		err = sqlutils.QueryRowsMapBuffered(db, query, func(m sqlutils.RowMap) error {
			if binlogCoordinates.LogPos != 0 {
				// Entry found!
				skipRestOfBinlog = true
				return nil
			}
			if skipRestOfBinlog {
				return nil
			}
			moreRowsExpected = true
			nextPos = m.GetInt64("End_log_pos")
			binlogEntryInfo := m.GetString("Info")
			//
			if binlogEntryInfo == entryText {
				// found it!
				binlogCoordinates.LogPos = m.GetInt64("Pos")
			} else if monotonicPseudoGTIDEntries && !alreadyMatchedAscendingPseudoGTID {
				// This part assumes we're searching for Pseudo-GTID.Typically that is the case, however this function can
				// also be used for generic searches through the binary log.
				// More heavyweight computation here. Need to verify whether the binlog entry we have is a pseudo-gtid entry
				// We only want to check for ASCENDING once in the top of the binary log.
				// If we find the first entry to be higher than the searched one, clearly we are done.
				// If not, then by virtue of binary logs, we still have to full-scan the entrie binlog sequentially; we
				// do not check again for ASCENDING (no point), so we save up CPU energy wasted in regexp.
				if pseudoGTIDMatches(pseudoGTIDRegexp, binlogEntryInfo) {
					alreadyMatchedAscendingPseudoGTID = true
					log.Debugf("Matched ascending Pseudo-GTID entry in %+v", binlog)
					if binlogEntryInfo > entryText {
						// Entries ascending, and current entry is larger than the one we are searching for.
						// There is no need to scan further on. We can skip the entire binlog
						log.Debugf(`Pseudo GTID entries are monotonic and we hit "%+v" > "%+v"; skipping binlog %+v`, m.GetString("Info"), entryText, binlogCoordinates.LogFile)
						skipRestOfBinlog = true
						return nil
					}
				}
			}
			return nil
		})
		if err != nil {
			return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
		}
		if skipRestOfBinlog {
			return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
		}
	}

	return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
}

// SearchEntryInInstanceBinlogs will search for a specific text entry within the binary logs of a given instance.
func SearchEntryInInstanceBinlogs(instance *Instance, entryText string, monotonicPseudoGTIDEntries bool, minBinlogCoordinates *BinlogCoordinates) (*BinlogCoordinates, error) {
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, err
	}
	cacheKey := getInstanceBinlogEntryKey(instance, entryText)
	coords, found := instanceBinlogEntryCache.Get(cacheKey)
	if found {
		// This is wonderful. We can skip the tedious GTID search in the binary log
		log.Debugf("Found instance Pseudo GTID entry coordinates in cache: %+v, %+v, %+v", instance.Key, entryText, coords)
		return coords.(*BinlogCoordinates), nil
	}

	// Look for GTID entry in given instance:
	log.Debugf("Searching for given pseudo gtid entry in %+v. monotonicPseudoGTIDEntries=%+v", instance.Key, monotonicPseudoGTIDEntries)
	currentBinlog := instance.SelfBinlogCoordinates
	err = nil
	for {
		log.Debugf("Searching for given pseudo gtid entry in binlog %+v of %+v", currentBinlog.LogFile, instance.Key)
		// loop iteration per binary log. This might turn to be a heavyweight operation. We wish to throttle the operation such that
		// the instance does not suffer. If it is a replica, we will only act as long as it's not lagging too much.
		if instance.ReplicaRunning() {
			for {
				log.Debugf("%+v is a replicating replica. Verifying lag", instance.Key)
				instance, err = ReadTopologyInstance(&instance.Key)
				if err != nil {
					break
				}
				if instance.HasReasonableMaintenanceReplicationLag() {
					// is good to go!
					break
				}
				log.Debugf("lag is too high on %+v. Throttling the search for pseudo gtid entry", instance.Key)
				time.Sleep(time.Duration(config.Config.ReasonableMaintenanceReplicationLagSeconds) * time.Second)
			}
		}
		var resultCoordinates BinlogCoordinates
		var found bool
		resultCoordinates, found, err = SearchEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentBinlog.LogFile, entryText, monotonicPseudoGTIDEntries, minBinlogCoordinates)
		if err != nil {
			break
		}
		if found {
			log.Debugf("Matched entry in %+v: %+v", instance.Key, resultCoordinates)
			instanceBinlogEntryCache.Set(cacheKey, &resultCoordinates, 0)
			return &resultCoordinates, nil
		}
		// Got here? Unfound. Keep looking
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentBinlog.LogFile {
			log.Debugf("Heuristic master binary logs search failed; continuing exhaustive search")
			minBinlogCoordinates = nil
		} else {
			currentBinlog, err = currentBinlog.PreviousFileCoordinates()
			if err != nil {
				break
			}
			log.Debugf("- Will move next to binlog %+v", currentBinlog.LogFile)
		}
	}

	return nil, log.Errorf("Cannot match pseudo GTID entry in binlogs of %+v; err: %+v", instance.Key, err)
}

// Read (as much as possible of) a chunk of binary log events starting the given startingCoordinates
func readBinlogEventsChunk(instanceKey *InstanceKey, startingCoordinates BinlogCoordinates) ([]BinlogEvent, error) {
	events := []BinlogEvent{}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return events, err
	}
	commandToken := math.TernaryString(startingCoordinates.Type == BinaryLog, "binlog", "relaylog")
	if startingCoordinates.LogFile == "" {
		return events, log.Errorf("readBinlogEventsChunk: empty binlog file name for %+v.", *instanceKey)
	}
	query := fmt.Sprintf("show %s events in '%s' FROM %d LIMIT %d", commandToken, startingCoordinates.LogFile, startingCoordinates.LogPos, config.Config.BinlogEventsChunkSize)
	err = sqlutils.QueryRowsMap(db, query, func(m sqlutils.RowMap) error {
		binlogEvent := BinlogEvent{}
		binlogEvent.Coordinates.LogFile = m.GetString("Log_name")
		binlogEvent.Coordinates.LogPos = m.GetInt64("Pos")
		binlogEvent.Coordinates.Type = startingCoordinates.Type
		binlogEvent.NextEventPos = m.GetInt64("End_log_pos")
		binlogEvent.EventType = m.GetString("Event_type")
		binlogEvent.Info = m.GetString("Info")

		events = append(events, binlogEvent)
		return nil
	})
	return events, err
}

// Return the next chunk of binlog events; skip to next binary log file if need be; return empty result only
// if reached end of binary logs
func getNextBinlogEventsChunk(instance *Instance, startingCoordinates BinlogCoordinates, numEmptyBinlogs int) ([]BinlogEvent, error) {
	if numEmptyBinlogs > maxEmptyBinlogFiles {
		log.Debugf("Reached maxEmptyBinlogFiles (%d) at %+v", maxEmptyBinlogFiles, startingCoordinates)
		// Give up and return empty results
		return []BinlogEvent{}, nil
	}
	coordinatesExceededCurrent := false
	switch startingCoordinates.Type {
	case BinaryLog:
		coordinatesExceededCurrent = instance.SelfBinlogCoordinates.FileSmallerThan(&startingCoordinates)
	case RelayLog:
		coordinatesExceededCurrent = instance.RelaylogCoordinates.FileSmallerThan(&startingCoordinates)
	}
	if coordinatesExceededCurrent {
		// We're past the last file. This is a non-error: there are no more events.
		log.Debugf("Coordinates overflow: %+v; terminating search", startingCoordinates)
		return []BinlogEvent{}, nil
	}
	events, err := readBinlogEventsChunk(&instance.Key, startingCoordinates)
	if err != nil {
		return events, err
	}
	if len(events) > 0 {
		log.Debugf("Returning %d events at %+v", len(events), startingCoordinates)
		return events, nil
	}

	// events are empty
	if nextCoordinates, err := instance.GetNextBinaryLog(startingCoordinates); err == nil {
		log.Debugf("Recursing into %+v", nextCoordinates)
		return getNextBinlogEventsChunk(instance, nextCoordinates, numEmptyBinlogs+1)
	}
	// on error
	return events, err
}

// used by GetNextBinlogCoordinatesToMatch to format debug information appropriately
// format the event information in debug output
func formatEventCleanly(event BinlogEvent, length *int) string {
	return fmt.Sprintf("%+v %+v; %+v", rpad(event.Coordinates, length), event.EventType, strings.Split(strings.TrimSpace(event.Info), "\n")[0])
}

// Only do special filtering if instance is MySQL-5.7 and other
// is MySQL-5.6 and in pseudo-gtid mode.
// returns applyInstanceSpecialFiltering, applyOtherSpecialFiltering, err
func special56To57filterProcessing(instance *Instance, other *Instance) (bool, bool, error) {
	// be paranoid
	if instance == nil || other == nil {
		return false, false, fmt.Errorf("special56To57filterProcessing: instance or other is nil. Should not happen")
	}

	filterInstance := instance.FlavorNameAndMajorVersion() == "MySQL-5.7" && // 5.7 replica
		other.FlavorNameAndMajorVersion() == "MySQL-5.6" // replicating under 5.6 master

	// The logic for other is a bit weird and may require us
	// to check the instance's master.  To avoid this do some
	// preliminary checks first to avoid the "master" access
	// unless absolutely needed.
	if instance.LogBinEnabled || // instance writes binlogs (not relay logs)
		instance.FlavorNameAndMajorVersion() != "MySQL-5.7" || // instance NOT 5.7 replica
		other.FlavorNameAndMajorVersion() != "MySQL-5.7" { // new master is NOT 5.7
		return filterInstance, false /* good exit status avoiding checking master */, nil
	}

	// We need to check if the master is 5.6
	// - Do not call GetInstanceMaster() as that requires the
	//   master to be available, and this code may be called
	//   during a master/intermediate master failover when the
	//   master may not actually be reachable.
	master, _, err := ReadInstance(&instance.MasterKey)
	if err != nil {
		return false, false, log.Errorf("special56To57filterProcessing: ReadInstance(%+v) fails: %+v", instance.MasterKey, err)
	}

	filterOther := master.FlavorNameAndMajorVersion() == "MySQL-5.6" // master(instance) == 5.6

	return filterInstance, filterOther, nil
}

// The event type to filter out
const anonymousGTIDNextEvent = "SET @@SESSION.GTID_NEXT= 'ANONYMOUS'"

// check if the event is one we want to skip.
func specialEventToSkip(event *BinlogEvent) bool {
	if event != nil && strings.Contains(event.Info, anonymousGTIDNextEvent) {
		return true
	}
	return false
}

// GetNextBinlogCoordinatesToMatch is given a twin-coordinates couple for a would-be replica (instance) and another
// instance (other).
// This is part of the match-below process, and is the heart of the operation: matching the binlog events starting
// the twin-coordinates (where both share the same Pseudo-GTID) until "instance" runs out of entries, hopefully
// before "other" runs out.
// If "other" runs out that means "instance" is more advanced in replication than "other", in which case we can't
// turn it into a replica of "other".
func GetNextBinlogCoordinatesToMatch(
	instance *Instance,
	instanceCoordinates BinlogCoordinates,
	recordedInstanceRelayLogCoordinates BinlogCoordinates,
	maxBinlogCoordinates *BinlogCoordinates,
	other *Instance,
	otherCoordinates BinlogCoordinates) (*BinlogCoordinates, int, error) {

	const noMatchedEvents int = 0 // to make return statements' intent clearer

	// create instanceCursor for scanning instance binlog events
	fetchNextEvents := func(binlogCoordinates BinlogCoordinates) ([]BinlogEvent, error) {
		return getNextBinlogEventsChunk(instance, binlogCoordinates, 0)
	}
	instanceCursor := NewBinlogEventCursor(instanceCoordinates, fetchNextEvents)

	// create otherCursor for scanning other binlog events
	fetchOtherNextEvents := func(binlogCoordinates BinlogCoordinates) ([]BinlogEvent, error) {
		return getNextBinlogEventsChunk(other, binlogCoordinates, 0)
	}
	otherCursor := NewBinlogEventCursor(otherCoordinates, fetchOtherNextEvents)

	// for 5.6 to 5.7 replication special processing may be needed.
	applyInstanceSpecialFiltering, applyOtherSpecialFiltering, err := special56To57filterProcessing(instance, other)
	if err != nil {
		return nil, noMatchedEvents, log.Errore(err)
	}

	var (
		beautifyCoordinatesLength    int = 0
		countMatchedEvents           int = 0
		lastConsumedEventCoordinates BinlogCoordinates
	)

	for {
		// Exhaust binlogs/relaylogs on instance. While iterating them, also iterate the otherInstance binlogs.
		// We expect entries on both to match, sequentially, until instance's binlogs/relaylogs are exhausted.
		var (
			// the whole event to make things simpler
			instanceEvent BinlogEvent
			otherEvent    BinlogEvent
		)

		{
			// we may need to skip Anonymous GTID Next Events so loop here over any we find
			var event *BinlogEvent
			var err error
			for done := false; !done; {
				// Extract next binlog/relaylog entry from instance:
				event, err = instanceCursor.nextRealEvent(0)
				if err != nil {
					return nil, noMatchedEvents, log.Errore(err)
				}
				if event != nil {
					lastConsumedEventCoordinates = event.Coordinates
				}
				if event == nil || !applyInstanceSpecialFiltering || !specialEventToSkip(event) {
					done = true
				}
			}

			switch instanceCoordinates.Type {
			case BinaryLog:
				if event == nil {
					// end of binary logs for instance:
					otherNextCoordinates, err := otherCursor.getNextCoordinates()
					if err != nil {
						return nil, noMatchedEvents, log.Errore(err)
					}
					instanceNextCoordinates, err := instanceCursor.getNextCoordinates()
					if err != nil {
						return nil, noMatchedEvents, log.Errore(err)
					}
					// sanity check
					if instanceNextCoordinates.SmallerThan(&instance.SelfBinlogCoordinates) {
						return nil, noMatchedEvents, log.Errorf("Unexpected problem: instance binlog iteration ended before self coordinates. Ended with: %+v, self coordinates: %+v", instanceNextCoordinates, instance.SelfBinlogCoordinates)
					}
					// Possible good exit point.
					log.Debugf("Reached end of binary logs for instance, at %+v. Other coordinates: %+v", instanceNextCoordinates, otherNextCoordinates)
					return &otherNextCoordinates, countMatchedEvents, nil
				}
			case RelayLog:
				// Argghhhh! SHOW RELAY LOG EVENTS IN '...' statement returns CRAPPY values for End_log_pos:
				// instead of returning the end log pos of the current statement in the *relay log*, it shows
				// the end log pos of the matching statement in the *master's binary log*!
				// Yes, there's logic to this. But this means the next-ccordinates are meaningless.
				// As result, in the case where we exhaust (following) the relay log, we cannot do our last
				// nice sanity test that we've indeed reached the Relay_log_pos coordinate; we are only at the
				// last statement, which is SMALLER than Relay_log_pos; and there isn't a "Rotate" entry to make
				// a place holder or anything. The log just ends and we can't be absolutely certain that the next
				// statement is indeed (futuristically) as End_log_pos.
				endOfScan := false
				if event == nil {
					// End of relay log...
					endOfScan = true
					log.Debugf("Reached end of relay log at %+v", recordedInstanceRelayLogCoordinates)
				} else if recordedInstanceRelayLogCoordinates.Equals(&event.Coordinates) {
					// We've passed the maxScanInstanceCoordinates (applies for relay logs)
					endOfScan = true
					log.Debugf("Reached replica relay log coordinates at %+v", recordedInstanceRelayLogCoordinates)
				} else if recordedInstanceRelayLogCoordinates.SmallerThan(&event.Coordinates) {
					return nil, noMatchedEvents, log.Errorf("Unexpected problem: relay log scan passed relay log position without hitting it. Ended with: %+v, relay log position: %+v", event.Coordinates, recordedInstanceRelayLogCoordinates)
				}
				if endOfScan {
					// end of binary logs for instance:
					otherNextCoordinates, err := otherCursor.getNextCoordinates()
					if err != nil {
						log.Debugf("otherCursor.getNextCoordinates() failed. otherCoordinates=%+v, cached events in cursor: %d; index=%d", otherCoordinates, len(otherCursor.cachedEvents), otherCursor.currentEventIndex)
						return nil, noMatchedEvents, log.Errore(err)
					}
					// Possible good exit point.
					// No further sanity checks (read the above lengthy explanation)
					log.Debugf("Reached limit of relay logs for instance, just after %+v. Other coordinates: %+v", lastConsumedEventCoordinates, otherNextCoordinates)
					return &otherNextCoordinates, countMatchedEvents, nil
				}
			}

			instanceEvent = *event // make a physical copy
			log.Debugf("> %s", formatEventCleanly(instanceEvent, &beautifyCoordinatesLength))
		}
		{
			// Extract next binlog/relaylog entry from other (intended master):
			// - this must have binlogs. We may need to filter anonymous events if we were processing
			//   a relay log on instance and the instance's master runs 5.6
			var event *BinlogEvent
			var err error
			for done := false; !done; {
				// Extract next binlog entry from other:
				event, err = otherCursor.nextRealEvent(0)
				if err != nil {
					return nil, noMatchedEvents, log.Errore(err)
				}
				if event == nil || !applyOtherSpecialFiltering || !specialEventToSkip(event) {
					done = true
				}
			}

			if event == nil {
				// end of binary logs for otherInstance: this is unexpected and means instance is more advanced
				// than otherInstance
				return nil, noMatchedEvents, log.Errorf("Unexpected end of binary logs for assumed master (%+v). This means the instance which attempted to be a replica (%+v) was more advanced. Try the other way round", other.Key, instance.Key)
			}

			otherEvent = *event // make a physical copy
			log.Debugf("< %s", formatEventCleanly(otherEvent, &beautifyCoordinatesLength))
		}
		// Verify things are sane (the two extracted entries are identical):
		// (not strictly required by the algorithm but adds such a lovely self-sanity-testing essence)
		if instanceEvent.Info != otherEvent.Info {
			return nil, noMatchedEvents, log.Errorf("Mismatching entries, aborting: %+v <-> %+v", instanceEvent.Info, otherEvent.Info)
		}
		countMatchedEvents++
		if maxBinlogCoordinates != nil {
			// Possible good exit point.
			// Not searching till end of binary logs/relay log exec pos. Instead, we're stopping at an instructed position.
			if instanceEvent.Coordinates.Equals(maxBinlogCoordinates) {
				log.Debugf("maxBinlogCoordinates specified as %+v and reached. Stopping", *maxBinlogCoordinates)
				return &otherEvent.Coordinates, countMatchedEvents, nil
			} else if maxBinlogCoordinates.SmallerThan(&instanceEvent.Coordinates) {
				return nil, noMatchedEvents, log.Errorf("maxBinlogCoordinates (%+v) exceeded but not met", *maxBinlogCoordinates)
			}
		}
	}
	// Won't get here
}

func GetPreviousGTIDs(instanceKey *InstanceKey, binlog string) (previousGTIDs *OracleGtidSet, err error) {
	if binlog == "" {
		return nil, log.Errorf("GetPreviousGTIDs: empty binlog file name for %+v", *instanceKey)
	}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("show binlog events in '%s' LIMIT 5", binlog)

	err = sqlutils.QueryRowsMapBuffered(db, query, func(m sqlutils.RowMap) error {
		eventType := m.GetString("Event_type")
		if eventType == "Previous_gtids" {
			var e error
			if previousGTIDs, e = NewOracleGtidSet(m.GetString("Info")); e != nil {
				return e
			}
		}
		return nil
	})
	return previousGTIDs, err
}
