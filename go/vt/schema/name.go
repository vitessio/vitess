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

var (
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
