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
		return e.dt.Date.Format()
	case sqltypes.Datetime:
		return e.dt.Format(datetime.DefaultPrecision)
	case sqltypes.Time:
		return e.dt.Time.Format(datetime.DefaultPrecision)
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
		return e.dt.Date.FormatInt64()
	case sqltypes.Datetime:
		return e.dt.FormatInt64()
	case sqltypes.Time:
		return e.dt.Time.FormatInt64()
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toJSON() *evalJSON {
	switch e.SQLType() {
	case sqltypes.Date:
		return json.NewDate(hack.String(e.ToRawBytes()))
	case sqltypes.Datetime:
		return json.NewDateTime(hack.String(e.ToRawBytes()))
	case sqltypes.Time:
		return json.NewTime(hack.String(e.ToRawBytes()))
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
		return &evalTemporal{t: sqltypes.Datetime, dt: e.dt.Time.ToDateTime()}
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

func evalToTime(e eval) *evalTemporal {
	switch e := e.(type) {
	case *evalTemporal:
		return e.toTime()
	case *evalBytes:
		if t, _ := datetime.ParseTime(e.string()); !t.IsZero() {
			return newEvalTime(t)
		}
		if dt, _ := datetime.ParseDateTime(e.string()); !dt.IsZero() {
			return newEvalTime(dt.Time)
		}
	case evalNumeric:
		if t, ok := datetime.ParseTimeInt64(e.toInt64().i); ok {
			return newEvalTime(t)
		}
		if dt, ok := datetime.ParseDateTimeInt64(e.toInt64().i); ok {
			return newEvalTime(dt.Time)
		}
	case *evalJSON:
		if t, ok := e.Time(); ok {
			return newEvalTime(t.RoundForJSON())
		}
	}
	return nil
}

func evalToDateTime(e eval) *evalTemporal {
	switch e := e.(type) {
	case *evalTemporal:
		return e.toDateTime()
	case *evalBytes:
		if t, _ := datetime.ParseDateTime(e.string()); !t.IsZero() {
			return newEvalDateTime(t)
		}
		if d, _ := datetime.ParseDate(e.string()); !d.IsZero() {
			return newEvalDateTime(datetime.DateTime{Date: d})
		}
	case evalNumeric:
		if t, ok := datetime.ParseDateTimeInt64(e.toInt64().i); ok {
			return newEvalDateTime(t)
		}
		if d, ok := datetime.ParseDateInt64(e.toInt64().i); ok {
			return newEvalDateTime(datetime.DateTime{Date: d})
		}
	case *evalJSON:
		if dt, ok := e.DateTime(); ok {
			return newEvalDateTime(dt)
		}
	}
	return nil
}

func evalToDate(e eval) *evalTemporal {
	switch e := e.(type) {
	case *evalTemporal:
		return e.toDate()
	case *evalBytes:
		if t, _ := datetime.ParseDate(e.string()); !t.IsZero() {
			return newEvalDate(t)
		}
		if dt, _ := datetime.ParseDateTime(e.string()); !dt.IsZero() {
			return newEvalDate(dt.Date)
		}
	case evalNumeric:
		if t, ok := datetime.ParseDateInt64(e.toInt64().i); ok {
			return newEvalDate(t)
		}
		if dt, ok := datetime.ParseDateTimeInt64(e.toInt64().i); ok {
			return newEvalDate(dt.Date)
		}
	case *evalJSON:
		if d, ok := e.Date(); ok {
			return newEvalDate(d)
		}
	}
	return nil
}

var _ eval = (*evalTemporal)(nil)
var _ hashable = (*evalTemporal)(nil)
