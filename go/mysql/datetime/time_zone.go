package datetime

import (
	"fmt"
	"strconv"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func unknownTimeZone(tz string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.UnknownTimeZone, "Unknown or incorrect time zone: '%s'", tz)
}

func ParseTimeZone(tz string) (*time.Location, error) {
	// Needs to be checked first since time.LoadLocation("") returns UTC.
	if tz == "" {
		return nil, unknownTimeZone(tz)
	}
	loc, err := time.LoadLocation(tz)
	if err == nil {
		return loc, nil
	}

	// MySQL also handles timezone formats in the form of the
	// offset from UTC, so we'll try that if the above fails.
	// This format is always something in the form of +HH:MM or -HH:MM.
	if len(tz) != 6 {
		return nil, unknownTimeZone(tz)
	}
	if tz[0] != '+' && tz[0] != '-' {
		return nil, unknownTimeZone(tz)
	}
	if tz[3] != ':' {
		return nil, unknownTimeZone(tz)
	}
	neg := tz[0] == '-'
	hours, err := strconv.ParseUint(tz[1:3], 10, 4)
	if err != nil {
		return nil, unknownTimeZone(tz)
	}
	minutes, err := strconv.ParseUint(tz[4:], 10, 6)
	if err != nil {
		return nil, unknownTimeZone(tz)
	}
	if minutes > 59 {
		return nil, unknownTimeZone(tz)
	}

	// MySQL only supports timezones in the range of -13:59 to +14:00.
	if neg && hours > 13 {
		return nil, unknownTimeZone(tz)
	}
	if !neg && (hours > 14 || hours == 14 && minutes > 0) {
		return nil, unknownTimeZone(tz)
	}
	offset := int(hours)*60*60 + int(minutes)*60
	if neg {
		offset = -offset
	}
	return time.FixedZone(fmt.Sprintf("UTC%s", tz), offset), nil
}
