/*
Copyright 2015 Google Inc. All Rights Reserved.

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

package bigtable

import (
	"fmt"
	"strings"
	"time"

	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// A Filter represents a row filter.
type Filter interface {
	String() string
	proto() *btpb.RowFilter
}

// ChainFilters returns a filter that applies a sequence of filters.
func ChainFilters(sub ...Filter) Filter { return chainFilter{sub} }

type chainFilter struct {
	sub []Filter
}

func (cf chainFilter) String() string {
	var ss []string
	for _, sf := range cf.sub {
		ss = append(ss, sf.String())
	}
	return "(" + strings.Join(ss, " | ") + ")"
}

func (cf chainFilter) proto() *btpb.RowFilter {
	chain := &btpb.RowFilter_Chain{}
	for _, sf := range cf.sub {
		chain.Filters = append(chain.Filters, sf.proto())
	}
	return &btpb.RowFilter{
		Filter: &btpb.RowFilter_Chain_{chain},
	}
}

// InterleaveFilters returns a filter that applies a set of filters in parallel
// and interleaves the results.
func InterleaveFilters(sub ...Filter) Filter { return interleaveFilter{sub} }

type interleaveFilter struct {
	sub []Filter
}

func (ilf interleaveFilter) String() string {
	var ss []string
	for _, sf := range ilf.sub {
		ss = append(ss, sf.String())
	}
	return "(" + strings.Join(ss, " + ") + ")"
}

func (ilf interleaveFilter) proto() *btpb.RowFilter {
	inter := &btpb.RowFilter_Interleave{}
	for _, sf := range ilf.sub {
		inter.Filters = append(inter.Filters, sf.proto())
	}
	return &btpb.RowFilter{
		Filter: &btpb.RowFilter_Interleave_{inter},
	}
}

// RowKeyFilter returns a filter that matches cells from rows whose
// key matches the provided RE2 pattern.
// See https://github.com/google/re2/wiki/Syntax for the accepted syntax.
func RowKeyFilter(pattern string) Filter { return rowKeyFilter(pattern) }

type rowKeyFilter string

func (rkf rowKeyFilter) String() string { return fmt.Sprintf("row(%s)", string(rkf)) }

func (rkf rowKeyFilter) proto() *btpb.RowFilter {
	return &btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{[]byte(rkf)}}
}

// FamilyFilter returns a filter that matches cells whose family name
// matches the provided RE2 pattern.
// See https://github.com/google/re2/wiki/Syntax for the accepted syntax.
func FamilyFilter(pattern string) Filter { return familyFilter(pattern) }

type familyFilter string

func (ff familyFilter) String() string { return fmt.Sprintf("col(%s:)", string(ff)) }

func (ff familyFilter) proto() *btpb.RowFilter {
	return &btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{string(ff)}}
}

// ColumnFilter returns a filter that matches cells whose column name
// matches the provided RE2 pattern.
// See https://github.com/google/re2/wiki/Syntax for the accepted syntax.
func ColumnFilter(pattern string) Filter { return columnFilter(pattern) }

type columnFilter string

func (cf columnFilter) String() string { return fmt.Sprintf("col(.*:%s)", string(cf)) }

func (cf columnFilter) proto() *btpb.RowFilter {
	return &btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte(cf)}}
}

// ValueFilter returns a filter that matches cells whose value
// matches the provided RE2 pattern.
// See https://github.com/google/re2/wiki/Syntax for the accepted syntax.
func ValueFilter(pattern string) Filter { return valueFilter(pattern) }

type valueFilter string

func (vf valueFilter) String() string { return fmt.Sprintf("value_match(%s)", string(vf)) }

func (vf valueFilter) proto() *btpb.RowFilter {
	return &btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte(vf)}}
}

// LatestNFilter returns a filter that matches the most recent N cells in each column.
func LatestNFilter(n int) Filter { return latestNFilter(n) }

type latestNFilter int32

func (lnf latestNFilter) String() string { return fmt.Sprintf("col(*,%d)", lnf) }

func (lnf latestNFilter) proto() *btpb.RowFilter {
	return &btpb.RowFilter{Filter: &btpb.RowFilter_CellsPerColumnLimitFilter{int32(lnf)}}
}

// StripValueFilter returns a filter that replaces each value with the empty string.
func StripValueFilter() Filter { return stripValueFilter{} }

type stripValueFilter struct{}

func (stripValueFilter) String() string { return "strip_value()" }
func (stripValueFilter) proto() *btpb.RowFilter {
	return &btpb.RowFilter{Filter: &btpb.RowFilter_StripValueTransformer{true}}
}

// TimestampRangeFilter returns a filter that matches any rows whose timestamp is within the given time bounds.  A zero
// time means no bound.
func TimestampRangeFilter(startTime time.Time, endTime time.Time) Filter {
	trf := timestampRangeFilter{}
	if !startTime.IsZero() {
		trf.startTime = Time(startTime)
	}
	if !endTime.IsZero() {
		trf.endTime = Time(endTime)
	}
	return trf
}

// TimestampRangeFilterMicros returns a filter that matches any rows whose timestamp is within the given time bounds,
// specified in units of microseconds since 1 January 1970. A zero value for the end time is interpreted as no bound.
func TimestampRangeFilterMicros(startTime Timestamp, endTime Timestamp) Filter {
	return timestampRangeFilter{startTime, endTime}
}

type timestampRangeFilter struct {
	startTime Timestamp
	endTime   Timestamp
}

func (trf timestampRangeFilter) String() string {
	return fmt.Sprintf("timestamp_range(%s,%s)", trf.startTime, trf.endTime)
}

func (trf timestampRangeFilter) proto() *btpb.RowFilter {
	return &btpb.RowFilter{
		Filter: &btpb.RowFilter_TimestampRangeFilter{&btpb.TimestampRange{int64(trf.startTime), int64(trf.endTime)}}}
}

// ColumnRangeFilter returns a filter that matches a contiguous range of columns within a single
// family, as specified by an inclusive start qualifier and exclusive end qualifier.
func ColumnRangeFilter(family, start, end string) Filter {
	return columnRangeFilter{family, start, end}
}

type columnRangeFilter struct {
	family string
	start  string
	end    string
}

func (crf columnRangeFilter) String() string {
	return fmt.Sprintf("columnRangeFilter(%s,%s,%s)", crf.family, crf.start, crf.end)
}

func (crf columnRangeFilter) proto() *btpb.RowFilter {
	r := &btpb.ColumnRange{FamilyName: crf.family}
	if crf.start != "" {
		r.StartQualifier = &btpb.ColumnRange_StartQualifierClosed{[]byte(crf.start)}
	}
	if crf.end != "" {
		r.EndQualifier = &btpb.ColumnRange_EndQualifierOpen{[]byte(crf.end)}
	}
	return &btpb.RowFilter{&btpb.RowFilter_ColumnRangeFilter{r}}
}

// TODO(dsymonds): More filters: cond, value range, sampling
