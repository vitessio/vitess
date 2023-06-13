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

	"vitess.io/vitess/go/mysql/decimal"
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
	if prec > 0 {
		b = append(b, '.')
		b = appendNsec(b, t.Nanosecond(), int(prec))
	}
	return b
}

func (t Time) Format(prec uint8) []byte {
	return t.AppendFormat(make([]byte, 0, 16), prec)
}

func (t Time) FormatInt64() (n int64) {
	tr := t.Round(0)
	v := int64(tr.Hour())*10000 + int64(tr.Minute())*100 + int64(tr.Second())
	if t.Neg() {
		return -v
	}
	return v
}

func (t Time) FormatFloat64() (n float64) {
	v := float64(t.Hour())*10000 + float64(t.Minute())*100 + float64(t.Second()) + float64(t.Nanosecond())/1e9
	if t.Neg() {
		return -v
	}
	return v
}

func (t Time) FormatDecimal() decimal.Decimal {
	v := int64(t.Hour())*10000 + int64(t.Minute())*100 + int64(t.Second())
	dec := decimal.NewFromInt(v)
	dec = dec.Add(decimal.New(int64(t.Nanosecond()), -9))
	if t.Neg() {
		dec = dec.Neg()
	}
	return dec
}

func (t Time) ToDateTime() (out DateTime) {
	return NewDateTimeFromStd(t.ToStdTime(time.Local))
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

var precs = []int{1e9, 1e8, 1e7, 1e6, 1e5, 1e4, 1e3, 1e2, 1e1, 1e0}

func (t Time) Round(p int) (r Time) {
	if t.nanosecond == 0 {
		return t
	}

	n := int(t.nanosecond)
	prec := precs[p]
	s := (n / prec) * prec
	l := s + prec

	if n-s >= l-n {
		n = l
	} else {
		n = s
	}

	r = t
	if n == 1e9 {
		r.second++
		n = 0
		if r.second == 60 {
			r.minute++
			r.second = 0
			if r.minute == 60 {
				r.hour++
				r.minute = 0
			}
		}
	}
	r.nanosecond = uint32(n)
	return r
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
	sun := t.AddDate(0, 0, -int(t.Weekday()))
	return sun.Year(), (sun.YearDay()-1)/7 + 1
}

// MondayWeek returns the year and week number of the current
// date, when week numbers are defined by starting on the first
// Monday of the year.
func (d Date) MondayWeek() (int, int) {
	t := d.ToStdTime(time.Local)
	// Since the week numbers always start on a Monday, we can look
	// at the week number of Monday itself. So we shift back to last
	// Monday we saw and compute the week number based on that.
	wd := (t.Weekday() + 6) % 7
	mon := t.AddDate(0, 0, -int(wd))
	return mon.Year(), (mon.YearDay()-1)/7 + 1
}

// Sunday4DayWeek returns the year and week number of the current
// date, when week numbers are defined by starting on the Sunday
// where week 1 is defined as having at least 4 days in the new
// year.
func (d Date) Sunday4DayWeek() (int, int) {
	t := d.ToStdTime(time.Local)

	// In this format, the first Wednesday of the year is always
	// in the first week. So we can look at the week number of
	// Wednesday in the same week. On days before Wednesday, we need
	// to move the time forward to Wednesday, on days after we need to
	// move it back to Wednesday.
	var wed time.Time

	switch wd := t.Weekday(); {
	case wd == 3:
		wed = t
	case wd < 3:
		wed = t.AddDate(0, 0, int(3-t.Weekday()))
	case wd > 3:
		wed = t.AddDate(0, 0, -int(t.Weekday()-3))
	}

	return wed.Year(), (wed.YearDay()-1)/7 + 1
}

const DefaultWeekMode = 0

func (d Date) Week(mode int) int {
	switch mode & 7 {
	case 0:
		year, week := d.SundayWeek()
		if year < d.Year() {
			return 0
		}
		return week
	case 1:
		year, week := d.ISOWeek()
		if year < d.Year() {
			return 0
		}
		return week
	case 2:
		_, week := d.SundayWeek()
		return week
	case 3:
		_, week := d.ISOWeek()
		return week
	case 4:
		year, week := d.Sunday4DayWeek()
		if year < d.Year() {
			return 0
		}
		return week
	case 5:
		year, week := d.MondayWeek()
		if year < d.Year() {
			return 0
		}
		return week
	case 6:
		_, week := d.Sunday4DayWeek()
		return week
	case 7:
		_, week := d.MondayWeek()
		return week
	default:
		return d.Week(DefaultWeekMode)
	}
}

func (d Date) YearWeek(mode int) int {
	switch mode {
	case 0, 2:
		year, week := d.SundayWeek()
		return year*100 + week
	case 1, 3:
		year, week := d.ISOWeek()
		return year*100 + week
	case 4, 5, 6, 7:
		// TODO
		return 0
	default:
		return d.YearWeek(DefaultWeekMode)
	}
}

func (d Date) Quarter() int {
	switch d.Month() {
	case 0:
		return 0
	case 1, 2, 3:
		return 1
	case 4, 5, 6:
		return 2
	case 7, 8, 9:
		return 3
	case 10, 11, 12:
		return 4
	default:
		panic("unreachable")
	}
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

func (t Time) AddInterval(itv *Interval, stradd bool) (Time, uint8, bool) {
	dt := DateTime{Time: t}
	ok := dt.addInterval(itv)
	return dt.Time, itv.precision(stradd), ok
}

func (t Time) toSeconds() int {
	tsecs := t.Hour()*secondsPerHour + t.Minute()*secondsPerMinute + t.Second()
	if t.Neg() {
		return -tsecs
	}
	return tsecs
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

func (d Date) AddInterval(itv *Interval) (Date, bool) {
	dt := DateTime{Date: d}
	ok := dt.addInterval(itv)
	return dt.Date, ok
}

func (dt DateTime) FormatInt64() int64 {
	d := dt.Round(0)
	return d.Date.FormatInt64()*1000000 + d.Time.FormatInt64()
}

func (dt DateTime) FormatFloat64() float64 {
	return float64(dt.Date.FormatInt64()*1000000) + dt.Time.FormatFloat64()
}

func (dt DateTime) FormatDecimal() decimal.Decimal {
	return decimal.New(dt.Date.FormatInt64(), 6).Add(dt.Time.FormatDecimal())
}

func (dt DateTime) Compare(dt2 DateTime) int {
	zerodate1, zerodate2 := dt.Date.IsZero(), dt2.Date.IsZero()

	switch {
	case zerodate1 && zerodate2:
		return dt.Time.Compare(dt2.Time)
	case zerodate1 || zerodate2:
		// if we're comparing a time to a datetime, we need to normalize them
		// both into datetimes; this normalization is not trivial because negative
		// times result in a date change, so let the standard library handle this
		return dt.ToStdTime(time.Local).Compare(dt2.ToStdTime(time.Local))
	}
	if cmp := dt.Date.Compare(dt2.Date); cmp != 0 {
		return cmp
	}
	return dt.Time.Compare(dt2.Time)
}

func (dt DateTime) AddInterval(itv *Interval, stradd bool) (DateTime, uint8, bool) {
	ok := dt.addInterval(itv)
	return dt, itv.precision(stradd), ok
}

func (dt DateTime) Round(p int) (r DateTime) {
	if dt.Time.nanosecond == 0 {
		return dt
	}

	n := dt.Time.Nanosecond()
	prec := precs[p]
	s := (n / prec) * prec
	l := s + prec

	if n-s >= l-n {
		n = l
	} else {
		n = s
	}

	r = dt
	if n == 1e9 {
		r.Time.nanosecond = 0
		return NewDateTimeFromStd(r.ToStdTime(time.Local).Add(time.Second))
	}
	r.Time.nanosecond = uint32(n)
	return r
}

func (dt DateTime) toSeconds() int {
	return (dt.Date.Day()-1)*secondsPerDay + dt.Time.toSeconds()
}

func (dt *DateTime) addInterval(itv *Interval) bool {
	switch {
	case itv.unit.HasTimeParts():
		if !itv.inRange() {
			return false
		}

		nsec := dt.Time.Nanosecond() + itv.nsec
		sec := dt.toSeconds() + itv.toSeconds() + (nsec / int(time.Second))
		nsec = nsec % int(time.Second)

		if nsec < 0 {
			nsec += int(time.Second)
			sec--
		}

		days := sec / secondsPerDay
		sec -= days * secondsPerDay

		if sec < 0 {
			sec += secondsPerDay
			days--
		}

		dt.Time.nanosecond = uint32(nsec)
		dt.Time.second = uint8(sec % secondsPerMinute)
		dt.Time.minute = uint8((sec / secondsPerMinute) % secondsPerMinute)
		dt.Time.hour = uint16(sec / secondsPerHour)

		daynum := mysqlDayNumber(dt.Date.Year(), dt.Date.Month(), 1) + days
		if daynum < 0 || daynum > maxDay {
			return false
		}

		dt.Date.year, dt.Date.month, dt.Date.day = mysqlDateFromDayNumber(daynum)
		return true

	case itv.unit.HasDayParts():
		daynum := mysqlDayNumber(dt.Date.Year(), dt.Date.Month(), dt.Date.Day())
		daynum += itv.day
		dt.Date.year, dt.Date.month, dt.Date.day = mysqlDateFromDayNumber(daynum)
		return true

	case itv.unit.HasMonthParts():
		months := dt.Date.Year()*12 + itv.year*12 + (dt.Date.Month() - 1) + itv.month
		if months < 0 || months >= 120000 {
			return false
		}

		year := months / 12
		month := (months % 12) + 1

		dt.Date.year = uint16(year)
		dt.Date.month = uint8(month)

		// MySQL quirk: if the original date was in a day that the new month
		// doesn't have, the date is offset backwards to the last day of
		// the new month. This is the opposite to normal date handling where
		// we'd offset days into the next month.
		if dim := daysIn(time.Month(month), year); dt.Date.Day() > dim {
			dt.Date.day = uint8(dim)
		}
		return true

	case itv.unit == IntervalYear:
		if itv.year > 10000 {
			return false
		}

		year := dt.Date.Year() + itv.year
		dt.Date.year = uint16(year)

		// MySQL quirk: if the original date was Feb 29th on a leap year, and
		// the resulting year is not a leap year, the date is offset backwards.
		// This is the opposite to what normal date handling does.
		if dt.Date.Month() == 2 && dt.Date.Day() == 29 && !isLeap(year) {
			dt.Date.day = 28
		}
		return true

	default:
		panic("unexpected IntervalType")
	}
}

func NewDateFromStd(t time.Time) Date {
	year, month, day := t.Date()
	return Date{
		year:  uint16(year),
		month: uint8(month),
		day:   uint8(day),
	}
}

func NewTimeFromStd(t time.Time) Time {
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()
	return Time{
		hour:       uint16(hour),
		minute:     uint8(min),
		second:     uint8(sec),
		nanosecond: uint32(nsec),
	}
}

func NewDateTimeFromStd(t time.Time) DateTime {
	return DateTime{
		Date: NewDateFromStd(t),
		Time: NewTimeFromStd(t),
	}
}
