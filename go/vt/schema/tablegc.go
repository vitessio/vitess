/*
Copyright 2020 The Vitess Authors.

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

package schema

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/textutil"
)

// TableGCState provides a state for the type of GC table: HOLD? PURGE? EVAC? DROP? See details below
type TableGCState string

const (
	// HoldTableGCState is the state where table was just renamed away. Data is still in tact,
	// and the user has the option to rename it back. A "safety" period.
	HoldTableGCState TableGCState = "HOLD"
	// PurgeTableGCState is the state where we purge table data. Table in this state is "lost" to the user.
	// if in this state, the table will be fully purged.
	PurgeTableGCState TableGCState = "PURGE"
	// EvacTableGCState is a waiting state, where we merely wait out the table's pages to be
	// gone from InnoDB's buffer pool, adaptive hash index cache, and whatnot.
	EvacTableGCState TableGCState = "EVAC"
	// DropTableGCState is the state where the table is to be dropped. Probably ASAP
	DropTableGCState TableGCState = "DROP"
)

const (
	readableTimeFormat = "20060102150405"
)

var (
	gcTableNameRegexp = regexp.MustCompile(`^_vt_(HOLD|PURGE|EVAC|DROP)_[0-f]{32}_([0-9]{14})$`)

	gcStates = map[string]TableGCState{
		string(HoldTableGCState):  HoldTableGCState,
		string(PurgeTableGCState): PurgeTableGCState,
		string(EvacTableGCState):  EvacTableGCState,
		string(DropTableGCState):  DropTableGCState,
	}
)

// CreateUUID creates a globally unique ID, returned as non-delimited string
// example result: 55d00cdce6ab11eabfe60242ac1c000d
func createUUID() (string, error) {
	u, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	result := u.String()
	result = strings.Replace(result, "-", "", -1)
	return result, nil
}

// ToReadableTimestamp returns a timestamp, in seconds resolution, that is human readable
// (as opposed to unix timestamp which is just a number)
// Example: for Aug 25 2020, 16:04:25 we return "20200825160425"
func ToReadableTimestamp(t time.Time) string {
	return t.Format(readableTimeFormat)
}

func generateGCTableName(state TableGCState, t time.Time) (string, error) {
	uuid, err := createUUID()
	if err != nil {
		return "", err
	}
	timestamp := ToReadableTimestamp(t)
	return fmt.Sprintf("_vt_%s_%s_%s", state, uuid, timestamp), nil
}

// AnalyzeGCTableName analyzes a given table name to see if it's a GC table, and if so, parse out
// its state and timestamp
func AnalyzeGCTableName(tableName string) (isGCTable bool, state TableGCState, t time.Time, err error) {
	submatch := gcTableNameRegexp.FindStringSubmatch(tableName)
	if len(submatch) == 0 {
		return false, state, t, nil
	}
	t, err = time.Parse(readableTimeFormat, submatch[2])
	return true, TableGCState(submatch[1]), t, err
}

// IsGCTableName answers 'true' when the given table name stands for a GC table
func IsGCTableName(tableName string) bool {
	return gcTableNameRegexp.MatchString(tableName)
}

// GenerateRenameStatement generates a "RENAME TABLE" statement, where a table is renamed to a GC table.
func GenerateRenameStatement(fromTableName string, state TableGCState, t time.Time) (statement string, toTableName string, err error) {
	toTableName, err = generateGCTableName(state, t)
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("RENAME TABLE `%s` TO %s", fromTableName, toTableName), toTableName, nil
}

// ParseGCLifecycle parses a comma separated list of gc states and returns a map of indicated states
func ParseGCLifecycle(gcLifecycle string) (states map[TableGCState]bool, err error) {
	states = make(map[TableGCState]bool)
	tokens := textutil.SplitDelimitedList(gcLifecycle)
	for _, token := range tokens {
		token = strings.ToUpper(token)
		state, ok := gcStates[token]
		if !ok {
			return states, fmt.Errorf("Unknown GC state: %s", token)
		}
		states[state] = true
	}
	// DROP is implicitly included.
	states[DropTableGCState] = true
	return states, nil
}
