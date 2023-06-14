package evalengine

import (
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

type evalTemporal struct {
	t    sqltypes.Type
	prec uint8
	dt   datetime.DateTime
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
		return e.dt.Format(e.prec)
	case sqltypes.Time:
		return e.dt.Time.Format(e.prec)
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

func (e *evalTemporal) toFloat() float64 {
	switch e.SQLType() {
	case sqltypes.Date:
		return float64(e.dt.Date.FormatInt64())
	case sqltypes.Datetime:
		return e.dt.FormatFloat64()
	case sqltypes.Time:
		return e.dt.Time.FormatFloat64()
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toDecimal() decimal.Decimal {
	switch e.SQLType() {
	case sqltypes.Date:
		return decimal.NewFromInt(e.dt.Date.FormatInt64())
	case sqltypes.Datetime:
		return e.dt.FormatDecimal()
	case sqltypes.Time:
		return e.dt.Time.FormatDecimal()
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toJSON() *evalJSON {
	switch e.SQLType() {
	case sqltypes.Date:
		return json.NewDate(hack.String(e.dt.Date.Format()))
	case sqltypes.Datetime:
		return json.NewDateTime(hack.String(e.dt.Format(datetime.DefaultPrecision)))
	case sqltypes.Time:
		return json.NewTime(hack.String(e.dt.Time.Format(datetime.DefaultPrecision)))
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toDateTime(l int) *evalTemporal {
	switch e.SQLType() {
	case sqltypes.Datetime, sqltypes.Date:
		return &evalTemporal{t: sqltypes.Datetime, dt: e.dt.Round(l), prec: uint8(l)}
	case sqltypes.Time:
		return &evalTemporal{t: sqltypes.Datetime, dt: e.dt.Time.Round(l).ToDateTime(), prec: uint8(l)}
	default:
		panic("unreachable")
	}
}

func (e *evalTemporal) toTime(l int) *evalTemporal {
	switch e.SQLType() {
	case sqltypes.Datetime:
		dt := datetime.DateTime{Time: e.dt.Time.Round(l)}
		return &evalTemporal{t: sqltypes.Time, dt: dt, prec: uint8(l)}
	case sqltypes.Date:
		// Zero-time
		return &evalTemporal{t: sqltypes.Time, prec: uint8(l)}
	case sqltypes.Time:
		return &evalTemporal{t: sqltypes.Time, dt: e.dt.Round(l), prec: uint8(l)}
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

func (e *evalTemporal) addInterval(interval *datetime.Interval, strcoll collations.TypedCollation) eval {
	var tmp *evalTemporal
	var ok bool

	switch tt := e.SQLType(); {
	case tt == sqltypes.Date && !interval.Unit().HasTimeParts():
		tmp = &evalTemporal{t: e.t}
		tmp.dt.Date, ok = e.dt.Date.AddInterval(interval)
	case tt == sqltypes.Time && !interval.Unit().HasDateParts():
		tmp = &evalTemporal{t: e.t}
		tmp.dt.Time, tmp.prec, ok = e.dt.Time.AddInterval(interval, strcoll.Valid())
	case tt == sqltypes.Datetime || tt == sqltypes.Timestamp || (tt == sqltypes.Date && interval.Unit().HasTimeParts()) || (tt == sqltypes.Time && interval.Unit().HasDateParts()):
		tmp = e.toDateTime(int(e.prec))
		tmp.dt, tmp.prec, ok = e.dt.AddInterval(interval, strcoll.Valid())
	}
	if !ok {
		return nil
	}
	if strcoll.Valid() {
		return newEvalRaw(sqltypes.Char, tmp.ToRawBytes(), strcoll)
	}
	return tmp
}

func newEvalDateTime(dt datetime.DateTime, l int) *evalTemporal {
	return &evalTemporal{t: sqltypes.Datetime, dt: dt.Round(l), prec: uint8(l)}
}

func newEvalDate(d datetime.Date) *evalTemporal {
	return &evalTemporal{t: sqltypes.Date, dt: datetime.DateTime{Date: d}}
}

func newEvalTime(time datetime.Time, l int) *evalTemporal {
	return &evalTemporal{t: sqltypes.Time, dt: datetime.DateTime{Time: time.Round(l)}, prec: uint8(l)}
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
	t, l, ok := datetime.ParseDateTime(hack.String(s), -1)
	if !ok {
		return nil, errIncorrectTemporal("DATETIME", s)
	}
	return newEvalDateTime(t, l), nil
}

func parseTime(s []byte) (*evalTemporal, error) {
	t, l, ok := datetime.ParseTime(hack.String(s), -1)
	if !ok {
		return nil, errIncorrectTemporal("TIME", s)
	}
	return newEvalTime(t, l), nil
}

func precision(req, got int) int {
	if req == -1 {
		return got
	}
	return req
}

func evalToTemporal(e eval) *evalTemporal {
	switch e := e.(type) {
	case *evalTemporal:
		return e
	case *evalBytes:
		if t, l, ok := datetime.ParseDateTime(e.string(), -1); ok {
			return newEvalDateTime(t, l)
		}
		if d, ok := datetime.ParseDate(e.string()); ok {
			return newEvalDate(d)
		}
		if t, l, ok := datetime.ParseTime(e.string(), -1); ok {
			return newEvalTime(t, l)
		}
	case *evalInt64:
		if t, ok := datetime.ParseDateTimeInt64(e.i); ok {
			return newEvalDateTime(t, 0)
		}
		if d, ok := datetime.ParseDateInt64(e.i); ok {
			return newEvalDate(d)
		}
		if t, ok := datetime.ParseTimeInt64(e.i); ok {
			return newEvalTime(t, 0)
		}
	case *evalUint64:
		if t, ok := datetime.ParseDateTimeInt64(int64(e.u)); ok {
			return newEvalDateTime(t, 0)
		}
		if d, ok := datetime.ParseDateInt64(int64(e.u)); ok {
			return newEvalDate(d)
		}
		if t, ok := datetime.ParseTimeInt64(int64(e.u)); ok {
			return newEvalTime(t, 0)
		}
	case *evalFloat:
		if t, l, ok := datetime.ParseDateTimeFloat(e.f, -1); ok {
			return newEvalDateTime(t, l)
		}
		if d, ok := datetime.ParseDateFloat(e.f); ok {
			return newEvalDate(d)
		}
		if t, l, ok := datetime.ParseTimeFloat(e.f, -1); ok {
			return newEvalTime(t, l)
		}
	case *evalDecimal:
		if t, l, ok := datetime.ParseDateTimeDecimal(e.dec, e.length, -1); ok {
			return newEvalDateTime(t, l)
		}
		if d, ok := datetime.ParseDateDecimal(e.dec); ok {
			return newEvalDate(d)
		}
		if d, l, ok := datetime.ParseTimeDecimal(e.dec, e.length, -1); ok {
			return newEvalTime(d, l)
		}
	case *evalJSON:
		if dt, ok := e.DateTime(); ok {
			if dt.Date.IsZero() {
				return newEvalTime(dt.Time, datetime.DefaultPrecision)
			}
			if dt.Time.IsZero() {
				return newEvalDate(dt.Date)
			}
			return newEvalDateTime(dt, datetime.DefaultPrecision)
		}
	}
	return nil
}

func evalToTime(e eval, l int) *evalTemporal {
	switch e := e.(type) {
	case *evalTemporal:
		return e.toTime(precision(l, int(e.prec)))
	case *evalBytes:
		if dt, l, _ := datetime.ParseDateTime(e.string(), l); !dt.IsZero() {
			return newEvalTime(dt.Time, l)
		}
		if t, l, ok := datetime.ParseTime(e.string(), l); ok || !t.IsZero() {
			return newEvalTime(t, l)
		}
	case *evalInt64:
		if t, ok := datetime.ParseTimeInt64(e.i); ok {
			return newEvalTime(t, precision(l, 0))
		}
		if dt, ok := datetime.ParseDateTimeInt64(e.i); ok {
			return newEvalTime(dt.Time, precision(l, 0))
		}
	case *evalUint64:
		if t, ok := datetime.ParseTimeInt64(int64(e.u)); ok {
			return newEvalTime(t, precision(l, 0))
		}
		if dt, ok := datetime.ParseDateTimeInt64(int64(e.u)); ok {
			return newEvalTime(dt.Time, precision(l, 0))
		}
	case *evalFloat:
		if t, l, ok := datetime.ParseTimeFloat(e.f, l); ok {
			return newEvalTime(t, l)
		}
		if dt, l, ok := datetime.ParseDateTimeFloat(e.f, l); ok {
			return newEvalTime(dt.Time, l)
		}
	case *evalDecimal:
		if t, l, ok := datetime.ParseTimeDecimal(e.dec, e.length, l); ok {
			return newEvalTime(t, l)
		}
		if dt, l, ok := datetime.ParseDateTimeDecimal(e.dec, e.length, l); ok {
			return newEvalTime(dt.Time, l)
		}
	case *evalJSON:
		if t, ok := e.Time(); ok {
			return newEvalTime(t.RoundForJSON(), precision(l, datetime.DefaultPrecision))
		}
	}
	return nil
}

func evalToDateTime(e eval, l int) *evalTemporal {
	switch e := e.(type) {
	case *evalTemporal:
		return e.toDateTime(precision(l, int(e.prec)))
	case *evalBytes:
		if t, l, _ := datetime.ParseDateTime(e.string(), l); !t.IsZero() {
			return newEvalDateTime(t, l)
		}
		if d, _ := datetime.ParseDate(e.string()); !d.IsZero() {
			return newEvalDateTime(datetime.DateTime{Date: d}, precision(l, 0))
		}
	case *evalInt64:
		if t, ok := datetime.ParseDateTimeInt64(e.i); ok {
			return newEvalDateTime(t, precision(l, 0))
		}
		if d, ok := datetime.ParseDateInt64(e.i); ok {
			return newEvalDateTime(datetime.DateTime{Date: d}, precision(l, 0))
		}
	case *evalUint64:
		if t, ok := datetime.ParseDateTimeInt64(int64(e.u)); ok {
			return newEvalDateTime(t, precision(l, 0))
		}
		if d, ok := datetime.ParseDateInt64(int64(e.u)); ok {
			return newEvalDateTime(datetime.DateTime{Date: d}, precision(l, 0))
		}
	case *evalFloat:
		if t, l, ok := datetime.ParseDateTimeFloat(e.f, l); ok {
			return newEvalDateTime(t, l)
		}
		if d, ok := datetime.ParseDateFloat(e.f); ok {
			return newEvalDateTime(datetime.DateTime{Date: d}, precision(l, 0))
		}
	case *evalDecimal:
		if t, l, ok := datetime.ParseDateTimeDecimal(e.dec, e.length, l); ok {
			return newEvalDateTime(t, l)
		}
		if d, ok := datetime.ParseDateDecimal(e.dec); ok {
			return newEvalDateTime(datetime.DateTime{Date: d}, precision(l, 0))
		}
	case *evalJSON:
		if dt, ok := e.DateTime(); ok {
			return newEvalDateTime(dt, precision(l, datetime.DefaultPrecision))
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
		if dt, _, _ := datetime.ParseDateTime(e.string(), -1); !dt.IsZero() {
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
