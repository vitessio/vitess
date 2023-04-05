package datetime

import (
	"time"

	"vitess.io/vitess/go/vt/vthash"
)

const negMask = uint16(1 << 15)

type Time struct {
	hour       uint16
	minute     uint8
	second     uint8
	nanosecond uint32
}

type Date struct {
	year  uint16
	month uint8
	day   uint8
}

type DateTime struct {
	Date Date
	Time Time
}

func (t *Time) AppendFormat(b []byte, prec uint8) []byte {
	if t.Neg() {
		b = append(b, '-')
	}

	b = appendInt(b, t.Hour(), 2)
	b = append(b, ':')
	b = appendInt(b, t.Minute(), 2)
	b = append(b, ':')
	b = appendInt(b, t.Second(), 2)
	if prec > 0 && t.Nanosecond() != 0 {
		b = append(b, '.')
		b = appendNsec(b, t.Nanosecond(), int(prec))
	}
	return b
}

func (t *Time) FormatInt64() (n int64) {
	v := int64(t.Hour())*10000 + int64(t.Minute())*100 + int64(t.Second())
	if t.Neg() {
		return -v
	}
	return v
}

func (t *Time) ToStdTime(loc *time.Location) (out time.Time) {
	year, month, day := time.Now().Date()
	hour, min, sec, nsec := t.Hour(), t.Minute(), t.Second(), t.Nanosecond()
	if t.Neg() {
		duration := time.Duration(hour)*time.Hour +
			time.Duration(min)*time.Minute +
			time.Duration(sec)*time.Second +
			time.Duration(nsec)*time.Nanosecond

		// If we have a negative time, we start with the start of today
		// and substract the total duration of the parsed time.
		out = time.Date(year, month, day, 0, 0, 0, 0, loc)
		out = out.Add(-duration)
	} else {
		out = time.Date(0, 1, 1, hour, min, sec, nsec, loc)
		out = out.AddDate(year, int(month-1), day-1)
	}
	return
}

func (t *Time) ToDateTime() (out DateTime) {
	year, month, day := time.Now().Date()
	dt := DateTime{
		Date: Date{
			year:  uint16(year),
			month: uint8(month),
			day:   uint8(day),
		},
		Time: *t,
	}
	return dt
}

func (t *Time) IsZero() bool {
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

func (t *Time) Hour() int {
	return int(t.hour & ^negMask)
}

func (t *Time) Minute() int {
	return int(t.minute)
}

func (t *Time) Second() int {
	return int(t.second)
}

func (t *Time) Nanosecond() int {
	return int(t.nanosecond)
}

func (t *Time) Neg() bool {
	return t.hour&negMask != 0
}

func (t *Time) Hash(h *vthash.Hasher) {
	h.Write16(t.hour)
	h.Write8(t.minute)
	h.Write8(t.second)
	h.Write32(t.nanosecond)
}

func (d *Date) IsZero() bool {
	return d.Year() == 0 && d.Month() == 0 && d.Day() == 0
}

func (d *Date) Year() int {
	return int(d.year)
}

func (d *Date) Month() int {
	return int(d.month)
}

func (d *Date) Day() int {
	return int(d.day)
}

func (d *Date) Hash(h *vthash.Hasher) {
	h.Write16(d.year)
	h.Write8(d.month)
	h.Write8(d.day)
}

func (dt *Date) Weekday() time.Weekday {
	// TODO: Implement this
	return 0
}

func (dt *Date) Yearday() int {
	// TODO: Implement this
	return 0
}

func (d *Date) ISOWeek() (int, int) {
	return 0, 0
}

func (dt *DateTime) IsZero() bool {
	return dt.Date.IsZero() && dt.Time.IsZero()
}

func (dt *DateTime) Hash(h *vthash.Hasher) {
	dt.Date.Hash(h)
	dt.Time.Hash(h)
}

func (dt *DateTime) ToStdTime(loc *time.Location) (out time.Time) {
	if dt.IsZero() {
		return time.Time{}
	}

	var year, day int
	var month time.Month
	if dt.Date.IsZero() {
		year, month, day = time.Now().Date()
	} else {
		year, month, day = dt.Date.Year(), time.Month(dt.Date.Month()), dt.Date.Day()
	}

	if dt.Time.IsZero() {
		return time.Date(year, month, day, 0, 0, 0, 0, loc)
	}

	hour, min, sec, nsec := dt.Time.Hour(), dt.Time.Minute(), dt.Time.Second(), dt.Time.Nanosecond()
	if dt.Time.Neg() {
		duration := time.Duration(hour)*time.Hour +
			time.Duration(min)*time.Minute +
			time.Duration(sec)*time.Second +
			time.Duration(nsec)*time.Nanosecond

		// If we have a negative time, we start with the start of today
		// and substract the total duration of the parsed time.
		out = time.Date(year, month, day, 0, 0, 0, 0, loc)
		out = out.Add(-duration)
	} else {
		out = time.Date(0, 1, 1, hour, min, sec, nsec, loc)
		out = out.AddDate(year, int(month-1), day-1)
	}
	return
}
