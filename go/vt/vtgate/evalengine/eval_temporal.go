package evalengine

import (
	"time"
	"unicode/utf8"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

type evalTemporal struct {
	t sqltypes.Type

	// std is the standard time.Time representation for this temporal value.
	// only set for t == DATE or t == DATETIME
	std time.Time

	// sql is the mysql datetime.Time representation for this temporal value.
	// only set for t == TIME
	sql datetime.Time
}

func (e *evalTemporal) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixDate)
	if e.t == sqltypes.Time {
		h.Write64(uint64(e.sql.ToStdTime().UnixNano()))
	} else {
		h.Write64(uint64(e.std.UnixNano()))
	}
}

func (e *evalTemporal) ToRawBytes() []byte {
	switch e.t {
	case sqltypes.Date:
		return datetime.Date_YYYY_MM_DD.Format(e.std, 6)
	case sqltypes.Datetime:
		return datetime.DateTime_YYYY_MM_DD_hh_mm_ss.Format(e.std, 6)
	case sqltypes.Time:
		return e.sql.AppendFormat(nil, 9)
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) SQLType() sqltypes.Type {
	return e.t
}

func (e *evalTemporal) toInt64() int64 {
	switch e.SQLType() {
	case sqltypes.Date:
		return datetime.Date_YYYYMMDD.FormatNumeric(e.std)
	case sqltypes.Datetime:
		return datetime.DateTime_YYYYMMDDhhmmss.FormatNumeric(e.std)
	case sqltypes.Time:
		return e.sql.FormatInt64()
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toJSON() *evalJSON {
	switch e.SQLType() {
	case sqltypes.Date:
		return json.NewDate(e.ToRawBytes())
	case sqltypes.Datetime:
		return json.NewDateTime(e.ToRawBytes())
	case sqltypes.Time:
		return json.NewTime(e.ToRawBytes())
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toDateTime() *evalTemporal {
	switch e.SQLType() {
	case sqltypes.Datetime:
		return e
	case sqltypes.Date:
		return &evalTemporal{t: sqltypes.Datetime, std: e.std}
	case sqltypes.Time:
		return &evalTemporal{t: sqltypes.Datetime, std: e.sql.ToStdTime()}
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toTime() *evalTemporal {
	switch e.SQLType() {
	case sqltypes.Datetime:
		hour, min, sec := e.std.Clock()
		return &evalTemporal{t: sqltypes.Time, sql: datetime.Time{Hours: int16(hour), Mins: int8(min), Secs: int8(sec), Nsec: int32(e.std.Nanosecond())}}
	case sqltypes.Date:
		// Zero-time
		return &evalTemporal{t: sqltypes.Time}
	case sqltypes.Time:
		return e
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toDate() *evalTemporal {
	switch e.SQLType() {
	case sqltypes.Datetime:
		// TODO: zero out hours/mins/secs?
		return &evalTemporal{t: sqltypes.Date, std: e.std}
	case sqltypes.Date:
		return e
	case sqltypes.Time:
		return &evalTemporal{t: sqltypes.Date}
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) isZero() bool {
	switch e.SQLType() {
	case sqltypes.Datetime, sqltypes.Date:
		return e.std.IsZero()
	case sqltypes.Time:
		return e.sql.IsZero()
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toStdTime() time.Time {
	if e.SQLType() == sqltypes.Time {
		return e.sql.ToStdTime()
	}
	return e.std
}

func newEvalDateTime(time time.Time) *evalTemporal {
	return &evalTemporal{t: sqltypes.Datetime, std: time}
}

func newEvalDate(time time.Time) *evalTemporal {
	return &evalTemporal{t: sqltypes.Date, std: time}
}

func newEvalTime(time datetime.Time) *evalTemporal {
	return &evalTemporal{t: sqltypes.Time, sql: time}
}

func sanitizeErrorValue(s []byte) []byte {
	b := make([]byte, 0, len(s)+1)
	invalid := false // previous byte was from an invalid UTF-8 sequence
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			invalid = false
			if c != 0 {
				b = append(b, c)
			}
			continue
		}
		_, wid := utf8.DecodeRune(s[i:])
		if wid == 1 {
			i++
			if !invalid {
				invalid = true
				b = append(b, '?')
			}
			continue
		}
		invalid = false
		b = append(b, s[i:i+wid]...)
		i += wid
	}
	return b
}

func errIncorrectTemporal(date string, in []byte) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect %s value: '%s'", date, sanitizeErrorValue(in))
}

func parseDate(s []byte) (*evalTemporal, error) {
	t, ok := datetime.ParseDate(hack.String(s))
	if !ok {
		return nil, errIncorrectTemporal("DATE", s)
	}
	return newEvalDate(t), nil
}

func parseDateTime(s []byte) (*evalTemporal, error) {
	t, ok := datetime.ParseDateTime(hack.String(s))
	if !ok {
		return nil, errIncorrectTemporal("DATETIME", s)
	}
	return newEvalDateTime(t), nil
}

func parseTime(s []byte) (*evalTemporal, error) {
	t, ok := datetime.ParseTime(hack.String(s))
	if !ok {
		return nil, errIncorrectTemporal("TIME", s)
	}
	return newEvalTime(t), nil
}

func evalToTime(e eval) (*evalTemporal, error) {
	switch e := e.(type) {
	case *evalTemporal:
		return e.toTime(), nil
	case *evalBytes:
		if t, ok := datetime.ParseTime(e.string()); ok {
			return newEvalTime(t), nil
		}
		return nil, errIncorrectTemporal("TIME", e.bytes)
	case *evalInt64:
		panic("TODO")
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "type %v is not date-like", e.SQLType())
	}
}

var _ eval = (*evalTemporal)(nil)
var _ hashable = (*evalTemporal)(nil)
