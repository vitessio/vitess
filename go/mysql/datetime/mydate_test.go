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
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDayNumber(t *testing.T) {
	td, err := os.Open("testdata/year_to_daynr.json")
	require.NoError(t, err)
	defer td.Close()

	var expected []int
	err = json.NewDecoder(td).Decode(&expected)
	require.NoError(t, err)

	for year, daynr := range expected {
		assert.Equal(t, daynr, MysqlDayNumber(year, 1, 1))
	}
}

func TestDayNumberFields(t *testing.T) {
	td, err := os.Open("testdata/daynr_to_date.json")
	require.NoError(t, err)
	defer td.Close()

	var expected [][4]int
	err = json.NewDecoder(td).Decode(&expected)
	require.NoError(t, err)

	for _, tc := range expected {
		y, m, d := mysqlDateFromDayNumber(tc[0])
		assert.Equal(t, tc[1], int(y))
		assert.Equal(t, tc[2], int(m))
		assert.Equal(t, tc[3], int(d))

		assert.Equalf(t, tc[0], MysqlDayNumber(tc[1], tc[2], tc[3]), "date %d-%d-%d", tc[1], tc[2], tc[3])

		wantDate := Date{
			year:  uint16(tc[1]),
			month: uint8(tc[2]),
			day:   uint8(tc[3]),
		}
		got := DateFromDayNumber(tc[0])
		assert.Equal(t, wantDate, got)
	}
}

func TestValidatePeriod(t *testing.T) {
	testCases := []struct {
		period int64
		want   bool
	}{
		{110112, true},
		{101122, false},
		{-1112212, false},
		{7110, true},
	}

	for _, tc := range testCases {
		got := ValidatePeriod(tc.period)
		assert.Equal(t, tc.want, got)
	}
}

func TestPeriodToMonths(t *testing.T) {
	testCases := []struct {
		period int64
		want   int64
	}{
		{0, 0},
		{110112, 13223},
		{100112, 12023},
		{7112, 23663},
		{200112, 24023},
		{112, 24023},
	}

	for _, tc := range testCases {
		got := PeriodToMonths(tc.period)
		assert.Equal(t, tc.want, got)
	}
}

func TestMonthsToPeriod(t *testing.T) {
	testCases := []struct {
		months int64
		want   int64
	}{
		{0, 0},
		{13223, 110112},
		{12023, 100112},
		{23663, 197112},
		{24023, 200112},
	}

	for _, tc := range testCases {
		got := MonthsToPeriod(tc.months)
		assert.Equal(t, tc.want, got)
	}
}
