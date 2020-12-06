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
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	readableTimeFormat = "20060102150405"
)

// CreateUUID creates a globally unique ID, with a given delimiter
// example results:
// - 1876a01a-354d-11eb-9a79-f8e4e33000bb (delimiter = "-")
// - 7cee19dd_354b_11eb_82cd_f875a4d24e90 (delimiter = "_")
// - 55d00cdce6ab11eabfe60242ac1c000d (delimiter = "")
func createUUID(delimiter string) (string, error) {
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
	return createUUID("-")
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
	if IsGCTableName(tableName) {
		return true
	}
	if IsOnlineDDLTableName(tableName) {
		return true
	}
	return false
}
