/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var dateFormats = []string{"2006-01-02", "06-01-02", "20060102", "060102"}
var datetimeFormats = []string{"2006-01-02 15:04:05.9", "06-01-02 15:04:05.9", "20060102150405.9", "060102150405.9"}
var timeWithDayFormats = []string{"15:04:05.9", "15:04", "15"}
var timeWithoutDayFormats = []string{"15:04:05.9", "15:04", "150405.9", "0405", "05"}

func ParseDate(in string) (t time.Time, err error) {
	for _, f := range dateFormats {
		t, err = time.Parse(f, in)
		if err == nil {
			return t, nil
		}
	}
	return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect DATE value: '%s'", in)
}

func ParseTime(in string) (t time.Time, err error) {
	// ParseTime is right now only excepting on specific
	// time format and doesn't accept all formats MySQL accepts.
	// Can be improved in the future as needed.
	if in == "" {
		return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect TIME value: '%s'", in)
	}
	start := 0
	neg := in[start] == '-'
	if neg {
		start++
	}

	parts := strings.Split(in[start:], " ")
	if len(parts) > 2 {
		return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect TIME value: '%s'", in)
	}
	days := 0
	hourMinuteSeconds := parts[0]
	if len(parts) == 2 {
		days, err = strconv.Atoi(parts[0])
		if err != nil {
			fmt.Printf("atoi failed: %+v\n", err)
			return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect TIME value: '%s'", in)
		}
		if days < 0 {
			// Double negative which is not allowed
			return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect TIME value: '%s'", in)
		}
		if days > 34 {
			return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect TIME value: '%s'", in)
		}
		for _, f := range timeWithDayFormats {
			t, err = time.Parse(f, parts[1])
			if err == nil {
				break
			}
		}
	} else {
		for _, f := range timeWithoutDayFormats {
			t, err = time.Parse(f, hourMinuteSeconds)
			if err == nil {
				break
			}
		}
	}

	if err != nil {
		return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect TIME value: '%s'", in)
	}

	// setting the date to today's date, because t is "0000-01-01 xx:xx:xx"
	now := time.Now()
	year, month, day := now.Date()
	if neg {
		// If we have a negative time, we start with the start of today
		// and substract the total duration of the parsed time.
		today := time.Date(year, month, day, 0, 0, 0, 0, t.Location())
		duration := time.Duration(days)*24*time.Hour +
			time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second +
			time.Duration(t.Nanosecond())*time.Nanosecond
		t = today.Add(-duration)
	} else {
		// In case of a positive time, we can take a quicker
		// shortcut and add the date of today.
		t = t.AddDate(year, int(month-1), day-1+days)
	}
	return t, nil
}

func ParseDateTime(in string) (t time.Time, err error) {
	for _, f := range datetimeFormats {
		t, err = time.Parse(f, in)
		if err == nil {
			return t, nil
		}
	}
	return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect DATETIME value: '%s'", in)
}
