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

package datetime

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var dateFormats = []string{"2006-01-02", "06-01-02", "20060102", "060102"}
var datetimeFormats = []string{"2006-01-02 15:04:05.9", "06-01-02 15:04:05.9", "20060102150405.9", "060102150405.9"}
var timeWithoutDayFormats = []string{"150405.9", "0405", "05"}

func validMessage(in string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(in, "\x00", ""), "?")
}

func ParseDate(in string) (t time.Time, err error) {
	for _, f := range dateFormats {
		t, err = time.Parse(f, in)
		if err == nil {
			return t, nil
		}
	}
	return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect DATE value: '%s'", validMessage(in))
}

func parseHoursMinutesSeconds(in string) (hours, minutes, seconds, nanoseconds int, err error) {
	parts := strings.Split(in, ":")
	switch len(parts) {
	case 3:
		sub := strings.Split(parts[2], ".")
		if len(sub) > 2 {
			return 0, 0, 0, 0, fmt.Errorf("invalid time format: %s", in)
		}
		seconds, err = strconv.Atoi(sub[0])
		if err != nil {
			return 0, 0, 0, 0, err
		}
		if seconds > 59 {
			return 0, 0, 0, 0, fmt.Errorf("invalid time format: %s", in)
		}
		if len(sub) == 2 {
			nanoseconds, err = strconv.Atoi(sub[1])
			if err != nil {
				return 0, 0, 0, 0, err
			}
			nanoseconds = int(math.Round(float64(nanoseconds)*math.Pow10(9-len(sub[1]))/1000) * 1000)
		}
		fallthrough
	case 2:
		if len(parts[1]) > 2 {
			return 0, 0, 0, 0, fmt.Errorf("invalid time format: %s", in)
		}
		minutes, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, 0, 0, err
		}
		if minutes > 59 {
			return 0, 0, 0, 0, fmt.Errorf("invalid time format: %s", in)
		}
		fallthrough
	case 1:
		hours, err = strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, 0, 0, err
		}
	default:
		return 0, 0, 0, 0, fmt.Errorf("invalid time format: %s", in)
	}

	if hours < 0 || minutes < 0 || seconds < 0 || nanoseconds < 0 {
		return 0, 0, 0, 0, fmt.Errorf("invalid time format: %s", in)
	}
	return
}

func ParseTime(in string) (t time.Time, out string, err error) {
	// ParseTime is right now only excepting on specific
	// time format and doesn't accept all formats MySQL accepts.
	// Can be improved in the future as needed.
	if in == "" {
		return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
	}
	start := 0
	neg := in[start] == '-'
	if neg {
		start++
	}

	parts := strings.Split(in[start:], " ")
	if len(parts) > 2 {
		return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
	}
	var days, hours, minutes, seconds, nanoseconds int
	hourMinuteSeconds := parts[0]
	if len(parts) == 2 {
		days, err = strconv.Atoi(parts[0])
		if err != nil {
			return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
		}
		if days < 0 {
			// Double negative which is not allowed
			return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
		}
		if days > 34 {
			return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
		}
		hours, minutes, seconds, nanoseconds, err = parseHoursMinutesSeconds(parts[1])
		if err != nil {
			return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
		}
		days = days + hours/24
		hours = hours % 24
		t = time.Date(0, 1, 1, hours, minutes, seconds, nanoseconds, time.UTC)
	} else {
		for _, f := range timeWithoutDayFormats {
			t, err = time.Parse(f, hourMinuteSeconds)
			if err == nil {
				break
			}
		}
		if err != nil {
			hours, minutes, seconds, nanoseconds, err = parseHoursMinutesSeconds(parts[0])
			if err != nil {
				return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
			}
			days = hours / 24
			hours = hours % 24
			t = time.Date(0, 1, 1, hours, minutes, seconds, nanoseconds, time.UTC)
			err = nil
		}
	}

	if err != nil {
		return t, "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", validMessage(in))
	}

	// setting the date to today's date, because t is "0000-01-01 xx:xx:xx"
	now := time.Now()
	year, month, day := now.Date()
	b := strings.Builder{}

	if neg {
		b.WriteString("-")
		// If we have a negative time, we start with the start of today
		// and substract the total duration of the parsed time.
		today := time.Date(year, month, day, 0, 0, 0, 0, t.Location())
		duration := time.Duration(days)*24*time.Hour +
			time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second +
			time.Duration(t.Nanosecond())*time.Nanosecond
		fmt.Fprintf(&b, "%02d", days*24+t.Hour())
		fmt.Fprintf(&b, ":%02d", t.Minute())
		fmt.Fprintf(&b, ":%02d", t.Second())
		if t.Nanosecond() > 0 {
			fmt.Fprintf(&b, ".%09d", t.Nanosecond())
		}

		t = today.Add(-duration)
	} else {
		fmt.Fprintf(&b, "%02d", days*24+t.Hour())
		fmt.Fprintf(&b, ":%02d", t.Minute())
		fmt.Fprintf(&b, ":%02d", t.Second())
		if t.Nanosecond() > 0 {
			fmt.Fprintf(&b, ".%09d", t.Nanosecond())
		}

		// In case of a positive time, we can take a quicker
		// shortcut and add the date of today.
		t = t.AddDate(year, int(month-1), day-1+days)
	}

	return t, b.String(), nil
}

func ParseDateTime(in string) (t time.Time, err error) {
	for _, f := range datetimeFormats {
		t, err = time.Parse(f, in)
		if err == nil {
			return t, nil
		}
	}
	return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect DATETIME value: '%s'", validMessage(in))
}
