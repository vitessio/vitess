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
)

// TableGCHint provides a hint for the type of GC table: HOLD? PURGE? EVAC? DROP? See details below
type TableGCHint string

const (
	// HoldTableGCHint is the state where table was just renamed away. Data is still in tact,
	// and the user has the option to rename it back. A "safety" period.
	HoldTableGCHint TableGCHint = "HOLD"
	// PurgeTableGCHint is the state where we purge table data. Table in this state is "lost" to the user.
	// if in this state, the table will be fully purged.
	PurgeTableGCHint TableGCHint = "PURGE"
	// EvacTableGCHint is a waiting state, where we merely wait out the table's pages to be
	// gone from InnoDB's buffer pool, adaptive hash index cache, and whatnot.
	EvacTableGCHint TableGCHint = "EVAC"
	// DropTableGCHint is the state where the table is to be dropped. Probably ASAP
	DropTableGCHint TableGCHint = "DROP"
)

const (
	readableTimeFormat = "20060102150405"
)

var (
	gcTableNameRegexp = regexp.MustCompile(`^_vt_(HOLD|PURGE|EVAC|DROP)_[0-f]{32}_([0-9]{14})$`)
)

// CreateUUID creates a globally unique ID, returned as string
// example result: 55d00cdce6ab11eabfe60242ac1c000d
func createUUID() (string, error) {
	u, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	uuid := u.String()
	uuid = strings.Replace(uuid, "-", "", -1)
	return uuid, nil
}

// ToReadableTimestamp returns a timestamp, in seconds resolution, that is human readable
// (as opposed to unix timestamp which is just a number)
// Example: for Aug 25 2020, 16:04:25 we return "20200825160425"
func ToReadableTimestamp(t time.Time) string {
	return t.Format(readableTimeFormat)
}

func generateGCTableName(hint TableGCHint, t time.Time) (string, error) {
	uuid, err := createUUID()
	if err != nil {
		return "", err
	}
	timestamp := ToReadableTimestamp(t)
	return fmt.Sprintf("_vt_%s_%s_%s", hint, uuid, timestamp), nil
}

// AnalyzeGCTableName analyzes a given table name to see if it's a GC table, and if so, parse out
// its hint and timestamp
func AnalyzeGCTableName(tableName string) (isGCTable bool, hint TableGCHint, t time.Time, err error) {
	submatch := gcTableNameRegexp.FindStringSubmatch(tableName)
	if len(submatch) == 0 {
		return false, hint, t, nil
	}
	t, err = time.Parse(readableTimeFormat, submatch[2])
	return true, TableGCHint(submatch[1]), t, err
}

// IsGCTableName answers 'true' when the given table name stands for a GC table
func IsGCTableName(tableName string) bool {
	return gcTableNameRegexp.MatchString(tableName)
}

// GenerateRenameStatement generates a "RENAME TABLE" statement, where a table is renamed to a GC table.
func GenerateRenameStatement(fromTableName string, hint TableGCHint, t time.Time) (statement string, toTableName string, err error) {
	toTableName, err = generateGCTableName(hint, t)
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("RENAME TABLE `%s` TO %s", fromTableName, toTableName), toTableName, nil
}
