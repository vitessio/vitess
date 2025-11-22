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
	// TableDroppedGCState is a pseudo state; a hint that the table does not exists anymore
	TableDroppedGCState TableGCState = ""
)

const (
	GCTableNameExpression string = `^_vt_(HOLD|PURGE|EVAC|DROP|hold|purge|evac|drop)_([0-f]{32})_([0-9]{14})$`
	// NewGCTableNameExpression parses new intrnal table name format, e.g. _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_
	NewGCTableNameExpression string = `^_vt_(hld|prg|evc|drp)_([0-f]{32})_([0-9]{14})_$`
)

var (
	gcUUIDRegexp      = regexp.MustCompile(`^[0-f]{32}$`)
	gcTableNameRegexp = regexp.MustCompile(GCTableNameExpression)

	gcStates = map[string]TableGCState{
		string(HoldTableGCState):                  HoldTableGCState,
		strings.ToLower(string(HoldTableGCState)): HoldTableGCState,
		"hld":                     HoldTableGCState,
		string(PurgeTableGCState): PurgeTableGCState,
		strings.ToLower(string(PurgeTableGCState)): PurgeTableGCState,
		"prg":                    PurgeTableGCState,
		string(EvacTableGCState): EvacTableGCState,
		strings.ToLower(string(EvacTableGCState)): EvacTableGCState,
		"evc":                    EvacTableGCState,
		string(DropTableGCState): DropTableGCState,
		strings.ToLower(string(DropTableGCState)): DropTableGCState,
		"drp": DropTableGCState,
	}
)

// IsGCUUID answers 'true' when the given string is an GC UUID, e.g.:
// a0638f6bec7b11ea9bf8000d3a9b8a9a
func IsGCUUID(uuid string) bool {
	return gcUUIDRegexp.MatchString(uuid)
}

// generateGCTableName creates a GC table name, based on desired state and time, and with optional preset UUID.
// If uuid is given, then it must be in GC-UUID format. If empty, the function auto-generates a UUID.
func generateGCTableName(state TableGCState, uuid string, t time.Time) (tableName string, err error) {
	if uuid == "" {
		uuid, err = CreateUUIDWithDelimiter("")
	}
	if err != nil {
		return "", err
	}
	if !IsGCUUID(uuid) {
		return "", fmt.Errorf("Not a valid GC UUID format: %s", uuid)
	}
	timestamp := ToReadableTimestamp(t)
	return fmt.Sprintf("_vt_%s_%s_%s", state, uuid, timestamp), nil
}

// generateGCTableNameNewFormat creates a GC table name, based on desired state and time, and with optional preset UUID.
// If uuid is given, then it must be in GC-UUID format. If empty, the function auto-generates a UUID.
func generateGCTableNameNewFormat(state TableGCState, uuid string, t time.Time) (tableName string, err error) {
	if uuid == "" {
		uuid, err = CreateUUIDWithDelimiter("")
	}
	if err != nil {
		return "", err
	}
	if !IsGCUUID(uuid) {
		return "", fmt.Errorf("Not a valid GC UUID format: %s", uuid)
	}
	timestamp := ToReadableTimestamp(t)
	var hint string
	for k, v := range gcStates {
		if v != state {
			continue
		}
		if len(k) == 3 && k != string(state) { // the "new" format
			hint = k
		}
	}
	return fmt.Sprintf("_vt_%s_%s_%s_", hint, uuid, timestamp), nil
}

// GenerateGCTableName creates a GC table name, based on desired state and time, and with random UUID
func GenerateGCTableName(state TableGCState, t time.Time) (tableName string, err error) {
	return generateGCTableName(state, "", t)
}

// GenerateGCTableNameNewFormat creates a GC table name, based on desired state and time, and with random UUID
func GenerateGCTableNameNewFormat(state TableGCState, t time.Time) (tableName string, err error) {
	return generateGCTableNameNewFormat(state, "", t)
}

// AnalyzeGCTableName analyzes a given table name to see if it's a GC table, and if so, parse out
// its state, uuid, and timestamp
func AnalyzeGCTableName(tableName string) (isGCTable bool, state TableGCState, uuid string, t time.Time, err error) {
	// Try new naming format (e.g. `_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_`):
	// The new naming format is accepted in v19, and actually _used_ in v20
	if isInternal, hint, uuid, t, err := AnalyzeInternalTableName(tableName); isInternal {
		gcState, ok := gcStates[hint]
		return ok, gcState, uuid, t, err
	}
	// Try old naming formats. These names will not be generated in v20.
	// TODO(shlomi): the code below should be remvoed in v21
	submatch := gcTableNameRegexp.FindStringSubmatch(tableName)
	if len(submatch) == 0 {
		return false, state, uuid, t, nil
	}
	gcState, ok := gcStates[submatch[1]]
	if !ok {
		return false, state, uuid, t, nil
	}
	t, err = time.Parse(readableTimeFormat, submatch[3])
	if err != nil {
		return false, state, uuid, t, err
	}
	return true, gcState, submatch[2], t, nil
}

// IsGCTableName answers 'true' when the given table name stands for a GC table
func IsGCTableName(tableName string) bool {
	isGC, _, _, _, _ := AnalyzeGCTableName(tableName)
	return isGC
}

// GenerateRenameStatementWithUUID generates a "RENAME TABLE" statement, where a table is renamed to a GC table, with preset UUID
func GenerateRenameStatementWithUUID(fromTableName string, state TableGCState, uuid string, t time.Time) (statement string, toTableName string, err error) {
	toTableName, err = generateGCTableName(state, uuid, t)
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("RENAME TABLE `%s` TO %s", fromTableName, toTableName), toTableName, nil
}

// GenerateRenameStatementWithUUIDNewFormat generates a "RENAME TABLE" statement, where a table is renamed to a GC table, with preset UUID
func GenerateRenameStatementWithUUIDNewFormat(fromTableName string, state TableGCState, uuid string, t time.Time) (statement string, toTableName string, err error) {
	toTableName, err = generateGCTableNameNewFormat(state, uuid, t)
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("RENAME TABLE `%s` TO %s", fromTableName, toTableName), toTableName, nil
}

// GenerateRenameStatement generates a "RENAME TABLE" statement, where a table is renamed to a GC table.
func GenerateRenameStatement(fromTableName string, state TableGCState, t time.Time) (statement string, toTableName string, err error) {
	return GenerateRenameStatementWithUUID(fromTableName, state, "", t)
}

// GenerateRenameStatement generates a "RENAME TABLE" statement, where a table is renamed to a GC table.
func GenerateRenameStatementNewFormat(fromTableName string, state TableGCState, t time.Time) (statement string, toTableName string, err error) {
	return GenerateRenameStatementWithUUIDNewFormat(fromTableName, state, "", t)
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
