package evalengine

import (
	"time"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

type evalTime struct {
	t    sqltypes.Type
	time datetime.SQLTime
}

func (e *evalTime) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixDate)
	h.Write64(uint64(e.time.Time.UnixNano()))
}

func (e *evalTime) ToRawBytes() []byte {
	switch e.t {
	case sqltypes.Date:
		return datetime.Date_YYYY_MM_DD.Format(e.time.Time, 6)
	case sqltypes.Datetime, sqltypes.Timestamp:
		return datetime.DateTime_YYYY_MM_DD_hh_mm_ss.Format(e.time.Time, 6)
	case sqltypes.Time:
		return e.time.AppendFormat(nil, 9)
	default:
		panic("unreachable")
	}
}

func (e *evalTime) SQLType() sqltypes.Type {
	return e.t
}

func (e *evalTime) toInt64() int64 {
	switch e.SQLType() {
	case sqltypes.Date:
		return datetime.Date_YYYYMMDD.FormatNumeric(e.time.Time)
	case sqltypes.Timestamp, sqltypes.Datetime:
		return datetime.DateTime_YYYYMMDDhhmmss.FormatNumeric(e.time.Time)
	case sqltypes.Time:
		return e.time.FormatInt64()
	default:
		panic("unreachable")
	}
}

func (e *evalTime) toJSON() *evalJSON {
	switch e.SQLType() {
	case sqltypes.Date:
		return json.NewDate(e.ToRawBytes())
	case sqltypes.Timestamp, sqltypes.Datetime:
		return json.NewDateTime(e.ToRawBytes())
	case sqltypes.Time:
		return json.NewTime(e.ToRawBytes())
	default:
		panic("unreachable")
	}
}

func newEvalDateTime(t sqltypes.Type, time time.Time) *evalTime {
	switch t {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp:
		return &evalTime{t: t, time: datetime.SQLTime{Time: time}}
	default:
		panic("bad type for newEvalTime")
	}
}

func newEvalTime(time datetime.SQLTime) *evalTime {
	return &evalTime{t: sqltypes.Time, time: time}
}

var _ eval = (*evalTime)(nil)
var _ hashable = (*evalTime)(nil)
