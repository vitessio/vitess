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

const (
	readableTimeFormat = "20060102150405"
)

const (
	InternalTableNameExpression string = `^_vt_([a-zA-Z0-9]{3})_([0-f]{32})_([0-9]{14})_$`
)

type InternalTableHint string

const (
	InternalTableUnknownHint      InternalTableHint = "nil"
	InternalTableGCHoldHint       InternalTableHint = "hld"
	InternalTableGCPurgeHint      InternalTableHint = "prg"
	InternalTableGCEvacHint       InternalTableHint = "evc"
	InternalTableGCDropHint       InternalTableHint = "drp"
	InternalTableVreplicationHint InternalTableHint = "vrp"
)

func (h InternalTableHint) String() string {
	return string(h)
}

var (
	// internalTableNameRegexp parses new internal table name format, e.g. _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_
	internalTableNameRegexp = regexp.MustCompile(InternalTableNameExpression)
)

// CreateUUIDWithDelimiter creates a globally unique ID, with a given delimiter
// example results:
// - 1876a01a-354d-11eb-9a79-f8e4e33000bb (delimiter = "-")
// - 7cee19dd_354b_11eb_82cd_f875a4d24e90 (delimiter = "_")
// - 55d00cdce6ab11eabfe60242ac1c000d (delimiter = "")
func CreateUUIDWithDelimiter(delimiter string) (string, error) {
	u, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	result := u.String()
	result = strings.Replace(result, "-", delimiter, -1)
	return result, nil
}

// CreateUUID creates a globally unique ID
// example result "1876a01a-354d-11eb-9a79-f8e4e33000bb"
func CreateUUID() (string, error) {
	return CreateUUIDWithDelimiter("-")
}

// ToReadableTimestamp returns a timestamp, in seconds resolution, that is human readable
// (as opposed to unix timestamp which is just a number)
// Example: for Aug 25 2020, 16:04:25 we return "20200825160425"
func ToReadableTimestamp(t time.Time) string {
	return t.Format(readableTimeFormat)
}

// ReadableTimestamp returns the current timestamp, in seconds resolution, that is human readable
func ReadableTimestamp() string {
	return ToReadableTimestamp(time.Now())
}

func condenseUUID(uuid string) string {
	uuid = strings.ReplaceAll(uuid, "-", "")
	uuid = strings.ReplaceAll(uuid, "_", "")
	return uuid
}

// isCondensedUUID answers 'true' when the given string is a condensed UUID, e.g.:
// a0638f6bec7b11ea9bf8000d3a9b8a9a
func isCondensedUUID(uuid string) bool {
	return condensedUUIDRegexp.MatchString(uuid)
}

// generateGCTableName creates an internal table name, based on desired hint and time, and with optional preset UUID.
// If uuid is given, then it must be in condensed-UUID format. If empty, the function auto-generates a UUID.
func GenerateInternalTableName(hint string, uuid string, t time.Time) (tableName string, err error) {
	if len(hint) != 3 {
		return "", fmt.Errorf("Invalid hint: %s, expected 3 characters", hint)
	}
	if uuid == "" {
		uuid, err = CreateUUIDWithDelimiter("")
	} else {
		uuid = condenseUUID(uuid)
	}
	if err != nil {
		return "", err
	}
	if !isCondensedUUID(uuid) {
		return "", fmt.Errorf("Invalid UUID: %s, expected condensed 32 hexadecimals", uuid)
	}
	timestamp := ToReadableTimestamp(t)
	return fmt.Sprintf("_vt_%s_%s_%s_", hint, uuid, timestamp), nil
}

// IsInternalOperationTableName answers 'true' when the given table name stands for an internal Vitess
// table used for operations such as:
// - Online DDL (gh-ost, pt-online-schema-change)
// - Table GC (renamed before drop)
// Apps such as VStreamer may choose to ignore such tables.
func IsInternalOperationTableName(tableName string) bool {
	if internalTableNameRegexp.MatchString(tableName) {
		return true
	}
	if IsGCTableName(tableName) {
		return true
	}
	if IsOnlineDDLTableName(tableName) {
		return true
	}
	return false
}

// AnalyzeInternalTableName analyzes a table name, and assumign it's a vitess internal table name, extracts
// the hint, uuid and time out of the name.
// An internal table name can be e.g. `_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_`, analyzed like so:
// - hint is `hld`
// - UUID is `6ace8bcef73211ea87e9f875a4d24e90`
// - Time is 2020-09-15 12:04:10
func AnalyzeInternalTableName(tableName string) (isInternalTable bool, hint string, uuid string, t time.Time, err error) {
	submatch := internalTableNameRegexp.FindStringSubmatch(tableName)
	if len(submatch) == 0 {
		return false, hint, uuid, t, nil
	}
	t, err = time.Parse(readableTimeFormat, submatch[3])
	if err != nil {
		return false, hint, uuid, t, err
	}
	return true, submatch[1], submatch[2], t, nil
}
