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
	"math"
	"math/bits"
	"strconv"
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/decimal"
)

// IntervalType represents the temporal elements contained in an Interval.
// Intervals in MySQL can contain more than one temporal element. We define their types as
// a bitset to let us efficiently query the temporal elements that form each interval.
// There are two kinds of IntervalTypes: unary and compound. Unary interval types contain
// a single temporal element (e.g. SECONDS, or DAYS) and hence contain only one bit set.
// Compount interval types are the logical combination of several unary interval types.
type IntervalType uint8

// IntervalType constants.
const (
	// Unary interval types
	IntervalNone        IntervalType = 0
	IntervalMicrosecond IntervalType = 1 << 0
	IntervalSecond      IntervalType = 1 << 1
	IntervalMinute      IntervalType = 1 << 2
	IntervalHour        IntervalType = 1 << 3
	IntervalDay         IntervalType = 1 << 4
	IntervalMonth       IntervalType = 1 << 5
	IntervalYear        IntervalType = 1 << 6
	intervalMulti       IntervalType = 1 << 7

	// IntervalWeek and IntervalQuarter are an exception for unary interval types,
	// which are not unique temporal elements but instead a modifier on a unary element
	// - WEEK is just a count of DAYS multiplied by 7
	// - QUARTER is just a count of MONTHS multiplied by 3
	IntervalWeek    = IntervalDay | intervalMulti
	IntervalQuarter = IntervalMonth | intervalMulti

	// Compound interval types
	IntervalSecondMicrosecond = IntervalSecond | IntervalMicrosecond
	IntervalMinuteMicrosecond = IntervalMinute | IntervalSecond | IntervalMicrosecond
	IntervalMinuteSecond      = IntervalMinute | IntervalSecond
	IntervalHourMicrosecond   = IntervalHour | IntervalMinute | IntervalSecond | IntervalMicrosecond
	IntervalHourSecond        = IntervalHour | IntervalMinute | IntervalSecond
	IntervalHourMinute        = IntervalHour | IntervalMinute
	IntervalDayMicrosecond    = IntervalDay | IntervalHour | IntervalMinute | IntervalSecond | IntervalMicrosecond
	IntervalDaySecond         = IntervalDay | IntervalHour | IntervalMinute | IntervalSecond
	IntervalDayMinute         = IntervalDay | IntervalHour | IntervalMinute
	IntervalDayHour           = IntervalDay | IntervalHour
	IntervalYearMonth         = IntervalYear | IntervalMonth
)

type intervalSetter func(tp *Interval, val int)

var intervalSet = [...]intervalSetter{
	intervalSetMicrosecond,
	intervalSetSecond,
	intervalSetMinute,
	intervalSetHour,
	intervalSetDay,
	intervalSetMonth,
	intervalSetYear,
}

// setter returns the setter method for this interval's type.
// If this is a unary interval, it'll return the setter for the interval's unary type.
// If this is a compound interval, it'll return the setter for the smallest unary type
// in the interval.
func (itv IntervalType) setter() intervalSetter {
	// find the lowest bit set in the interval, this is the smallest unary type
	unary := itv & -itv

	// map from an unary interval type to its offset by counting the trailing
	// zeroes. e.g. for HOUR(1 << 3), this will return 3, which the position
	// for the HOUR setter in intervalSet
	return intervalSet[bits.TrailingZeros8(uint8(unary))]
}

func (itv IntervalType) PartCount() int {
	return bits.OnesCount8(uint8(itv & ^intervalMulti))
}

func (itv IntervalType) HasTimeParts() bool {
	return itv&(IntervalHour|IntervalMinute|IntervalSecond|IntervalMicrosecond) != 0
}

func (itv IntervalType) HasDateParts() bool {
	return itv&(IntervalYear|IntervalMonth|IntervalDay) != 0
}

func (itv IntervalType) HasDayParts() bool {
	return (itv & IntervalDay) != 0
}

func (itv IntervalType) HasMonthParts() bool {
	return (itv & IntervalMonth) != 0
}

func (itv IntervalType) NeedsPrecision() bool {
	return itv&IntervalMicrosecond != 0
}

// ToString returns the type as a string
func (itv IntervalType) ToString() string {
	switch itv {
	case IntervalYear:
		return "year"
	case IntervalQuarter:
		return "quarter"
	case IntervalMonth:
		return "month"
	case IntervalWeek:
		return "week"
	case IntervalDay:
		return "day"
	case IntervalHour:
		return "hour"
	case IntervalMinute:
		return "minute"
	case IntervalSecond:
		return "second"
	case IntervalMicrosecond:
		return "microsecond"
	case IntervalYearMonth:
		return "year_month"
	case IntervalDayHour:
		return "day_hour"
	case IntervalDayMinute:
		return "day_minute"
	case IntervalDaySecond:
		return "day_second"
	case IntervalHourMinute:
		return "hour_minute"
	case IntervalHourSecond:
		return "hour_second"
	case IntervalMinuteSecond:
		return "minute_second"
	case IntervalDayMicrosecond:
		return "day_microsecond"
	case IntervalHourMicrosecond:
		return "hour_microsecond"
	case IntervalMinuteMicrosecond:
		return "minute_microsecond"
	case IntervalSecondMicrosecond:
		return "second_microsecond"
	default:
		return "[unknown IntervalType]"
	}
}

func intervalSetYear(tp *Interval, val int) {
	tp.year = val
}

func intervalSetMonth(tp *Interval, val int) {
	// if the intervalMulti flag is set, this interval expects QUARTERS instead of months
	if tp.unit&intervalMulti != 0 {
		val = val * 3
	}
	tp.month = val
}

func intervalSetDay(tp *Interval, val int) {
	// if the intervalMulti flag is set, this interval expects WEEKS instead of days
	if tp.unit&intervalMulti != 0 {
		val = val * 7
	}
	tp.day = val
}

func intervalSetHour(tp *Interval, val int) {
	tp.hour = val
}

func intervalSetMinute(tp *Interval, val int) {
	tp.min = val
}

func intervalSetSecond(tp *Interval, val int) {
	tp.sec = val
}

func intervalSetMicrosecond(tp *Interval, val int) {
	// if we are setting the Microseconds in this interval, but the
	// interval's type isn't explicitly MICROSECOND (i.e. it's an interval
	// with several values besides MICROSECOND), the value being passed
	// here won't be a fixed number of microseconds, but a fractional part.
	// We need to scale it into microseconds.
	// E.g. when parsing a SECOND:MICROSECOND value of '1.5', the input
	// to this setter will be 5, but the interval doesn't contain 5 microseconds,
	// it contains 500000. We perform the scaling into 6 digits using base10 log.
	if tp.unit != IntervalMicrosecond {
		digits := int(math.Log10(float64(val)) + 1)
		val = val * int(math.Pow10(6-digits))
	}
	// we store nsec internally, so convert from microseconds to nanoseconds
	tp.nsec = val * 1000
}

// parseIntervalFields parses a internal string into separate numeric fields.
// The parsing is extremely lax according to MySQL. Any contiguous run of numbers
// is considered a field, and any non-numeric character is ignored.
func parseIntervalFields(itv string, negate *bool) (fields []int) {
	if len(itv) > 0 && itv[0] == '-' {
		*negate = !*negate
		itv = itv[1:]
	}

	for {
		for len(itv) > 0 && !('0' <= itv[0] && itv[0] <= '9') {
			itv = itv[1:]
		}
		if len(itv) == 0 {
			break
		}

		var n int
		for len(itv) > 0 && '0' <= itv[0] && itv[0] <= '9' {
			n = n*10 + int(itv[0]-'0')
			itv = itv[1:]
		}

		fields = append(fields, n)
	}
	return
}

type Interval struct {
	timeparts
	unit IntervalType
}

func (itv *Interval) Unit() IntervalType {
	return itv.unit
}

const maxDay = 3652424

func (itv *Interval) inRange() bool {
	if itv.day > maxDay {
		return false
	}
	if itv.hour > maxDay*24 {
		return false
	}
	if itv.min > maxDay*24*60 {
		return false
	}
	if itv.sec > maxDay*24*60*60 {
		return false
	}
	return true
}

// setFromFields sets the duration of interval from a slice of fields and
// the given interval type.
// This follow's MySQL's behavior: if there are fewer fields than the ones
// we'd expect to see in the interval's type, we pick the RIGHTMOST as
// the values for the interval.
// E.g. if our interval type wants HOUR:MINUTE:SECOND and we have [1, 1]
// as input fields, the resulting interval is '1min1sec'
func (itv *Interval) setFromFields(fields []int, unit IntervalType) bool {
	parts := unit.PartCount()
	if parts == 1 {
		unit.setter()(itv, fields[0])
		return true
	}
	if len(fields) > 3 && parts < 4 {
		return false
	}

	for f, set := range intervalSet {
		if len(fields) == 0 {
			break
		}
		if unit&(1<<f) == 0 {
			continue
		}
		set(itv, fields[len(fields)-1])
		fields = fields[:len(fields)-1]
	}
	return true
}

// precision returns the precision that will be applied to a datetime
// after adding this Interval to it.
// IF the datetime will be added as a string, the precision is 6 if the
// interval contains nanoseconds, and 0 otherwise.
// IF the datetime will be added as a temporal type, the precision is
// the one we calculated when parsing the Interval.
func (itv *Interval) precision(stradd bool) uint8 {
	if stradd {
		if itv.timeparts.nsec != 0 {
			return DefaultPrecision
		}
		return 0
	}
	return itv.prec
}

func (itv *Interval) negate() {
	itv.year = -itv.year
	itv.month = -itv.month
	itv.day = -itv.day
	itv.hour = -itv.hour
	itv.min = -itv.min
	itv.sec = -itv.sec
	itv.nsec = -itv.nsec
}

func ParseInterval(itv string, unit IntervalType, negate bool) *Interval {
	// Special handling: when parsing a SECOND interval, if it looks like
	// a decimal number, consider the fractional part of the decimal as microseconds
	// even though the IntervalType doesn't expect microseconds.
	if unit == IntervalSecond && strings.Count(itv, ".") == 1 {
		return parseIntervalFraction(itv, DefaultPrecision, unit, negate)
	}

	interval := &Interval{unit: unit}
	if unit.NeedsPrecision() || unit == IntervalSecond {
		interval.prec = DefaultPrecision
	}

	fields := parseIntervalFields(itv, &negate)
	if !interval.setFromFields(fields, unit) {
		return nil
	}
	if negate {
		interval.negate()
	}
	return interval
}

func parseIntervalInt(itv int, prec uint8, unit IntervalType, negate, forcePrecision bool) *Interval {
	interval := &Interval{unit: unit}
	if unit.NeedsPrecision() || forcePrecision {
		interval.prec = prec
	}
	unit.setter()(interval, itv)
	if negate {
		interval.negate()
	}
	return interval
}

func ParseIntervalInt64(itv int64, unit IntervalType, negate bool) *Interval {
	return parseIntervalInt(int(itv), DefaultPrecision, unit, negate, false)
}

func parseIntervalFraction(frac string, prec uint8, unit IntervalType, negate bool) *Interval {
	var fields [2]int
	var ok bool

	fields[0], frac, ok = getnumn(frac)
	if !ok {
		return nil
	}
	if len(frac) == 0 {
		return parseIntervalInt(fields[0], prec, unit, negate, unit == IntervalSecond)
	}
	if frac[0] != '.' {
		return nil
	}
	fields[1], _, ok = getnumn(frac[1:])
	if !ok {
		return nil
	}

	interval := &Interval{unit: unit}
	if unit == IntervalSecond {
		unit |= IntervalMicrosecond
	}
	if unit.NeedsPrecision() {
		interval.prec = prec
	}
	if !interval.setFromFields(fields[:], unit) {
		return nil
	}
	if negate {
		interval.negate()
	}
	return interval
}

func ParseIntervalFloat(f float64, unit IntervalType, negate bool) *Interval {
	if unit.PartCount() == 1 && unit != IntervalSecond {
		return parseIntervalInt(int(math.Round(f)), DefaultPrecision, unit, negate, false)
	}
	return parseIntervalFraction(strconv.FormatFloat(f, 'f', -1, 64), DefaultPrecision, unit, negate)
}

func ParseIntervalDecimal(dec decimal.Decimal, mysqlPrec int32, unit IntervalType, negate bool) *Interval {
	prec := uint8(mysqlPrec)
	if unit.NeedsPrecision() {
		prec = DefaultPrecision
	}
	if unit.PartCount() == 1 && unit != IntervalSecond {
		f, ok := dec.Float64()
		if !ok {
			return nil
		}
		return parseIntervalInt(int(math.Round(f)), prec, unit, negate, false)
	}

	b := dec.FormatMySQL(mysqlPrec)
	return parseIntervalFraction(hack.String(b), prec, unit, negate)
}
