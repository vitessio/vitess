/*
Copyright 2023 The Vitess Authors.

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

const DefaultPrecision = 6

func (t Time) AppendFormat(b []byte, prec uint8) []byte {
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

func (t Time) Format(prec uint8) []byte {
	return t.AppendFormat(make([]byte, 0, 16), prec)
}

func (t Time) FormatInt64() (n int64) {
	v := int64(t.Hour())*10000 + int64(t.Minute())*100 + int64(t.Second())
	if t.Neg() {
		return -v
	}
	return v
}

func (t Time) ToDateTime() (out DateTime) {
	return FromStdTime(t.ToStdTime(time.Local))
}

func (t Time) IsZero() bool {
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

// RoundForJSON rounds the time to the nearest 32nd hour. This is some really
// weird behavior that MySQL does when it casts a JSON time back to a MySQL
// TIME value. We just mimic the behavior here.
func (t Time) RoundForJSON() Time {
	if t.Hour() < 32 {
		return t
	}
	res := t
	res.hour = uint16(t.Hour() % 32)
	if t.Neg() {
		res.hour |= negMask
	}
	return res
}

func (t Time) Hour() int {
	return int(t.hour & ^negMask)
}

func (t Time) Minute() int {
	return int(t.minute)
}

func (t Time) Second() int {
	return int(t.second)
}

func (t Time) Nanosecond() int {
	return int(t.nanosecond)
}

func (t Time) Neg() bool {
	return t.hour&negMask != 0
}

func (t Time) Hash(h *vthash.Hasher) {
	h.Write16(t.hour)
	h.Write8(t.minute)
	h.Write8(t.second)
	h.Write32(t.nanosecond)
}

func (t Time) Compare(t2 Time) int {
	if t.Neg() != t2.Neg() {
		if t.Neg() {
			return -1
		}
		return 1
	}
	// Need to swap if both are negative.
	if t.Neg() {
		t, t2 = t2, t
	}

	h1, h2 := t.Hour(), t2.Hour()
	if h1 < h2 {
		return -1
	}
	if h1 > h2 {
		return 1
	}
	m1, m2 := t.Minute(), t2.Minute()
	if m1 < m2 {
		return -1
	}
	if m1 > m2 {
		return 1
	}
	s1, s2 := t.Second(), t2.Second()
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}
	ns1, ns2 := t.Nanosecond(), t2.Nanosecond()
	if ns1 < ns2 {
		return -1
	}
	if ns1 > ns2 {
		return 1
	}
	return 0
}

func (d Date) IsZero() bool {
	return d.Year() == 0 && d.Month() == 0 && d.Day() == 0
}

func (d Date) Year() int {
	return int(d.year)
}

func (d Date) Month() int {
	return int(d.month)
}

func (d Date) Day() int {
	return int(d.day)
}

func (d Date) Hash(h *vthash.Hasher) {
	h.Write16(d.year)
	h.Write8(d.month)
	h.Write8(d.day)
}

func (dt Date) Weekday() time.Weekday {
	return dt.ToStdTime(time.Local).Weekday()
}

func (dt Date) Yearday() int {
	return dt.ToStdTime(time.Local).YearDay()
}

func (d Date) ISOWeek() (int, int) {
	return d.ToStdTime(time.Local).ISOWeek()
}

// SundayWeek returns the year and week number of the current
// date, when week numbers are defined by starting on the first
// Sunday of the year.
func (d Date) SundayWeek() (int, int) {
	t := d.ToStdTime(time.Local)
	// Since the week numbers always start on a Sunday, we can look
	// at the week number of Sunday itself. So we shift back to last
	// Sunday we saw and compute the week number based on that.
	sun := t.Add(-time.Duration(t.Weekday()) * 24 * time.Hour)
	return sun.Year(), (sun.YearDay()-1)/7 + 1
}

func (dt DateTime) IsZero() bool {
	return dt.Date.IsZero() && dt.Time.IsZero()
}

func (dt DateTime) Hash(h *vthash.Hasher) {
	dt.Date.Hash(h)
	dt.Time.Hash(h)
}

func (t Time) ToDuration() time.Duration {
	duration := time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second +
		time.Duration(t.Nanosecond())*time.Nanosecond
	if t.Neg() {
		return -duration
	}
	return duration
}

func (t Time) toStdTime(year int, month time.Month, day int, loc *time.Location) (out time.Time) {
	return time.Date(year, month, day, 0, 0, 0, 0, loc).Add(t.ToDuration())
}

func (t Time) ToStdTime(loc *time.Location) (out time.Time) {
	year, month, day := time.Now().Date()
	return t.toStdTime(year, month, day, loc)
}

func (d Date) ToStdTime(loc *time.Location) (out time.Time) {
	return time.Date(d.Year(), time.Month(d.Month()), d.Day(), 0, 0, 0, 0, loc)
}

func (dt DateTime) ToStdTime(loc *time.Location) time.Time {
	zerodate := dt.Date.IsZero()
	zerotime := dt.Time.IsZero()

	switch {
	case zerodate && zerotime:
		return time.Time{}
	case zerodate:
		return dt.Time.ToStdTime(loc)
	case zerotime:
		return dt.Date.ToStdTime(loc)
	default:
		year, month, day := dt.Date.Year(), time.Month(dt.Date.Month()), dt.Date.Day()
		return dt.Time.toStdTime(year, month, day, loc)
	}
}

func (dt DateTime) Format(prec uint8) []byte {
	return DateTime_YYYY_MM_DD_hh_mm_ss.Format(dt, prec)
}

func (d Date) Format() []byte {
	return Date_YYYY_MM_DD.Format(DateTime{Date: d}, 0)
}

func (d Date) FormatInt64() int64 {
	return int64(d.Year())*10000 + int64(d.Month())*100 + int64(d.Day())
}

func (d Date) Compare(d2 Date) int {
	y1, y2 := d.Year(), d2.Year()
	if y1 < y2 {
		return -1
	}
	if y1 > y2 {
		return 1
	}
	m1, m2 := d.Month(), d2.Month()
	if m1 < m2 {
		return -1
	}
	if m1 > m2 {
		return 1
	}
	day1, day2 := d.Day(), d2.Day()
	if day1 < day2 {
		return -1
	}
	if day1 > day2 {
		return 1
	}
	return 0
}

func (dt DateTime) FormatInt64() int64 {
	return dt.Date.FormatInt64()*1000000 + dt.Time.FormatInt64()
}

func (dt DateTime) Compare(dt2 DateTime) int {
	zerodate1, zerodate2 := dt.Date.IsZero(), dt2.Date.IsZero()

	switch {
	case zerodate1 && zerodate2:
		return dt.Time.Compare(dt2.Time)
	case zerodate1 || zerodate2:
		// if we're comparing a time to a datetime, we need to normalize them
		// both into datetimes; this normalization is not trivial because negative
		// times result in a date change, to let the standard library handle this
		return dt.ToStdTime(time.Local).Compare(dt2.ToStdTime(time.Local))
	}
	if cmp := dt.Date.Compare(dt2.Date); cmp != 0 {
		return cmp
	}
	return dt.Time.Compare(dt2.Time)
}

func FromStdTime(t time.Time) DateTime {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()

	return DateTime{
		Date: Date{
			year:  uint16(year),
			month: uint8(month),
			day:   uint8(day),
		},
		Time: Time{
			hour:       uint16(hour),
			minute:     uint8(min),
			second:     uint8(sec),
			nanosecond: uint32(nsec),
		},
	}
}
