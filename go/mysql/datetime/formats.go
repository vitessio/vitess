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

var DefaultMySQLStrftime = map[byte]Spec{
	'a': fmtWeekdayNameShort{},
	'b': fmtMonthNameShort{},
	'c': fmtMonth{false},
	'D': fmtMonthDaySuffix{},
	'd': fmtDay{true},
	'e': fmtDay{false},
	'f': fmtMicroseconds{},
	'H': fmtHour24{true},
	'h': fmtHour12{true},
	'I': fmtHour12{true},
	'i': fmtMin{true},
	'j': fmtZeroYearDay{},
	'k': fmtHour24{false},
	'l': fmtHour12{false},
	'M': fmtMonthName{},
	'm': fmtMonth{true},
	'p': fmtAMorPM{},
	'r': fmtFullTime12{},
	'S': fmtSecond{true, false},
	's': fmtSecond{true, false},
	'T': fmtFullTime24{},
	'U': fmtWeek0{},
	'u': fmtWeek1{},
	'V': fmtWeek2{},
	'v': fmtWeek3{},
	'W': fmtWeekdayName{},
	'w': fmtWeekday{},
	'X': fmtYearForWeek2{},
	'x': fmtYearForWeek3{},
	'Y': fmtYearLong{},
	'y': fmtYearShort{},
}

var Date_YYYY_MM_DD = &Strftime{
	pattern: "YYYY-MM-DD",
	compiled: []Spec{
		fmtYearLong{},
		fmtSeparator('-'),
		fmtMonth{true},
		fmtSeparator('-'),
		fmtDay{true},
	},
}

var Date_YYYY_M_D = &Strftime{
	pattern: "YYYY-M-D",
	compiled: []Spec{
		fmtYearLong{},
		fmtSeparator('-'),
		fmtMonth{false},
		fmtSeparator('-'),
		fmtDay{false},
	},
}

var Date_YY_M_D = &Strftime{
	pattern: "YY-M-D",
	compiled: []Spec{
		fmtYearShort{},
		fmtSeparator('-'),
		fmtMonth{false},
		fmtSeparator('-'),
		fmtDay{false},
	},
}

var Date_YYYYMMDD = &Strftime{
	pattern: "YYYYMMDD",
	compiled: []Spec{
		fmtYearLong{},
		fmtMonth{true},
		fmtDay{true},
	},
}

var Date_YYMMDD = &Strftime{
	pattern: "YYMMDD",
	compiled: []Spec{
		fmtYearShort{},
		fmtMonth{true},
		fmtDay{true},
	},
}

var DateTime_YYYY_MM_DD_hh_mm_ss = &Strftime{
	pattern: "YYYY-MM-DD hh:mm:ss",
	compiled: []Spec{
		fmtYearLong{},
		fmtSeparator('-'),
		fmtMonth{true},
		fmtSeparator('-'),
		fmtDay{true},
		fmtTimeSeparator{},
		fmtHour24{true},
		fmtSeparator(':'),
		fmtMin{true},
		fmtSeparator(':'),
		fmtSecond{true, true},
	},
}

var DateTime_YYYY_M_D_h_m_s = &Strftime{
	pattern: "YYYY-M-D h:m:s",
	compiled: []Spec{
		fmtYearLong{},
		fmtSeparator('-'),
		fmtMonth{false},
		fmtSeparator('-'),
		fmtDay{false},
		fmtTimeSeparator{},
		fmtHour24{false},
		fmtSeparator(':'),
		fmtMin{false},
		fmtSeparator(':'),
		fmtSecond{false, true},
	},
}

var DateTime_YY_M_D_h_m_s = &Strftime{
	pattern: "YY-M-D h:m:s",
	compiled: []Spec{
		fmtYearShort{},
		fmtSeparator('-'),
		fmtMonth{false},
		fmtSeparator('-'),
		fmtDay{false},
		fmtTimeSeparator{},
		fmtHour24{false},
		fmtSeparator(':'),
		fmtMin{false},
		fmtSeparator(':'),
		fmtSecond{false, true},
	},
}

var DateTime_YYYYMMDDhhmmss = &Strftime{
	pattern: "YYYYMMDDhhmmss",
	compiled: []Spec{
		fmtYearLong{},
		fmtMonth{true},
		fmtDay{true},
		fmtHour24{true},
		fmtMin{true},
		fmtSecond{true, true},
	},
}

var DateTime_YYMMDDhhmmss = &Strftime{
	pattern: "YYMMDDhhmmss",
	compiled: []Spec{
		fmtYearShort{},
		fmtMonth{true},
		fmtDay{},
		fmtHour24{true},
		fmtMin{true},
		fmtSecond{true, true},
	},
}

var Time_hh_mm_ss = &Strftime{
	pattern: "hh:mm:ss",
	compiled: []Spec{
		fmtHour24{true},
		fmtSeparator(':'),
		fmtMin{true},
		fmtSeparator(':'),
		fmtSecond{true, true},
	},
}

var Time_hhmmss = &Strftime{
	pattern: "hhmmss",
	compiled: []Spec{
		fmtHour24{true},
		fmtMin{true},
		fmtSecond{true, true},
	},
}
