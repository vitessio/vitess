/*
Copyright 2017 GitHub Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vitessdriver

import (
	"errors"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
)

// ErrInvalidTime is returned when we fail to parse a datetime
// string from MySQL. This should never happen unless things are
// seriously messed up.
var ErrInvalidTime = errors.New("invalid MySQL time string")

var isoTimeFormat = "2006-01-02 15:04:05.999999"
var isoNullTime = "0000-00-00 00:00:00.000000"
var isoTimeLength = len(isoTimeFormat)

// parseISOTime pases a time string in MySQL's textual datetime format.
// This is very similar to ISO8601, with some differences:
//
// - There is no T separator between the date and time sections;
//   a space is used instead.
// - There is never a timezone section in the string, as these datetimes
//   are not timezone-aware. There isn't a Z value for UTC times for
//   the same reason.
//
// Note that this function can handle both DATE (which should _always_ have
// a length of 10) and DATETIME strings (which have a variable length, 18+
// depending on the number of decimal sub-second places).
//
// Also note that this function handles the case where MySQL returns a NULL
// time (with a string where all sections are zeroes) by returning a zeroed
// out time.Time object. NULL time strings are not considered a parsing error.
//
// See: isoTimeFormat
func parseISOTime(tstr string, loc *time.Location, minLen, maxLen int) (t time.Time, err error) {
	tlen := len(tstr)
	if tlen < minLen || tlen > maxLen {
		err = ErrInvalidTime
		return
	}

	if tstr == isoNullTime[:tlen] {
		// This is what MySQL would send when the date is NULL,
		// so return an empty time.Time instead.
		// This is not a parsing error
		return
	}

	if loc == nil {
		loc = time.UTC
	}

	// Since the time format returned from MySQL never has a Timezone
	// section, ParseInLocation will initialize the time.Time struct
	// with the default `loc` we're passing here.
	return time.ParseInLocation(isoTimeFormat[:tlen], tstr, loc)
}

func checkTimeFormat(t string) (err error) {
	// Valid format string offsets for any ISO time from MySQL:
	//  |DATETIME |10      |19+
	//  |---------|--------|
	// "2006-01-02 15:04:05.999999"
	_, err = parseISOTime(t, time.UTC, 10, isoTimeLength)
	return
}

// DatetimeToNative converts a Datetime Value into a time.Time
func DatetimeToNative(v sqltypes.Value, loc *time.Location) (time.Time, error) {
	// Valid format string offsets for a DATETIME
	//  |DATETIME          |19+
	//  |------------------|------|
	// "2006-01-02 15:04:05.999999"
	return parseISOTime(v.ToString(), loc, 19, isoTimeLength)
}

// DateToNative converts a Date Value into a time.Time.
// Note that there's no specific type in the Go stdlib to represent
// dates without time components, so the returned Time will have
// their hours/mins/seconds zeroed out.
func DateToNative(v sqltypes.Value, loc *time.Location) (time.Time, error) {
	// Valid format string offsets for a DATE
	//  |DATE     |10
	//  |---------|
	// "2006-01-02 00:00:00.000000"
	return parseISOTime(v.ToString(), loc, 10, 10)
}

// NewDatetime builds a Datetime Value
func NewDatetime(t time.Time, defaultLoc *time.Location) sqltypes.Value {
	if t.Location() != defaultLoc {
		t = t.In(defaultLoc)
	}
	return sqltypes.MakeTrusted(sqltypes.Datetime, []byte(t.Format(isoTimeFormat)))
}
