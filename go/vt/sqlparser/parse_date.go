package sqlparser

import (
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var dateFormats = []string{"2006-01-02", "06-01-02", "20060102", "060102"}
var datetimeFormats = []string{"2006-01-02 15:04:05", "06-01-02 15:04:05", "20060102150405", "060102150405"}

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
	t, err = time.Parse("15:04:05", in)
	if err != nil {
		return t, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "incorrect TIME value: '%s'", in)
	}
	now := time.Now()
	// setting the date to today's date, because we use AddDate on t
	// which is "0000-01-01 xx:xx:xx", we do minus one on the month
	// and day to take into account the 01 in both month and day of t
	t = t.AddDate(now.Year(), int(now.Month()-1), now.Day()-1)
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
