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
	t  sqltypes.Type
	dt datetime.DateTime
}

func (e *evalTemporal) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixDate)
	e.dt.Hash(h)
}

func (e *evalTemporal) ToRawBytes() []byte {
	switch e.t {
	case sqltypes.Date:
		return datetime.Date_YYYY_MM_DD.Format(e.dt, 0)
	case sqltypes.Datetime:
		return datetime.DateTime_YYYY_MM_DD_hh_mm_ss.Format(e.dt, 6)
	case sqltypes.Time:
		return e.dt.Time.AppendFormat(nil, 6)
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
		return datetime.Date_YYYYMMDD.FormatNumeric(e.dt)
	case sqltypes.Datetime:
		return datetime.DateTime_YYYYMMDDhhmmss.FormatNumeric(e.dt)
	case sqltypes.Time:
		return e.dt.Time.FormatInt64()
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
		return &evalTemporal{t: sqltypes.Datetime, dt: e.dt}
	case sqltypes.Time:
		return &evalTemporal{t: sqltypes.Time, dt: e.dt.Time.ToDateTime()}
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toTime() *evalTemporal {
	switch e.SQLType() {
	case sqltypes.Datetime:
		dt := datetime.DateTime{Time: e.dt.Time}
		return &evalTemporal{t: sqltypes.Time, dt: dt}
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
		dt := datetime.DateTime{Date: e.dt.Date}
		return &evalTemporal{t: sqltypes.Date, dt: dt}
	case sqltypes.Date:
		return e
	case sqltypes.Time:
		dt := e.dt.Time.ToDateTime()
		dt.Time = datetime.Time{}
		return &evalTemporal{t: sqltypes.Date, dt: dt}
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) isZero() bool {
	return e.dt.IsZero()
}

func (e *evalTemporal) toStdTime(loc *time.Location) time.Time {
	return e.dt.ToStdTime(loc)
}

func newEvalDateTime(dt datetime.DateTime) *evalTemporal {
	return &evalTemporal{t: sqltypes.Datetime, dt: dt}
}

func newEvalDate(d datetime.Date) *evalTemporal {
	return &evalTemporal{t: sqltypes.Date, dt: datetime.DateTime{Date: d}}
}

func newEvalTime(time datetime.Time) *evalTemporal {
	return &evalTemporal{t: sqltypes.Time, dt: datetime.DateTime{Time: time}}
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
