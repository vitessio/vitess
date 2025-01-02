/*
Copyright 2024 The Vitess Authors.

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

package schemadiff

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestIsRangePartitioned(t *testing.T) {
	tcases := []struct {
		create string
		expect bool
	}{
		{
			create: "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))",
			expect: true,
		},
		{
			create: "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE COLUMNS (id) (PARTITION p0 VALUES LESS THAN (10))",
			expect: true,
		},
		{
			create: "CREATE TABLE t (id int PRIMARY KEY)",
		},
		{
			create: "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1))",
		},
	}
	env := NewTestEnv()

	for _, tcase := range tcases {
		t.Run(tcase.create, func(t *testing.T) {
			entity, err := NewCreateTableEntityFromSQL(env, tcase.create)
			require.NoError(t, err)

			result := IsRangePartitioned(entity.CreateTable)
			assert.Equal(t, tcase.expect, result)
		})
	}
}

// AnalyzePartitionRotation analyzes a given AlterTable statement to see whether it has partition rotation
// commands, and if so, is the ALTER TABLE statement valid in MySQL. In MySQL, a single ALTER TABLE statement
// cannot apply multiple rotation commands, nor can it mix rotation commands with other types of changes.
func TestAlterTableRotatesRangePartition(t *testing.T) {
	tcases := []struct {
		create string
		alter  string
		expect bool
	}{
		{
			alter:  "ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))",
			expect: true,
		},
		{
			alter:  "ALTER TABLE t DROP PARTITION p1",
			expect: true,
		},
		{
			alter:  "ALTER TABLE t DROP PARTITION p1, p2",
			expect: true,
		},
		{
			alter: "ALTER TABLE t TRUNCATE PARTITION p3",
		},
		{
			alter: "ALTER TABLE t COALESCE PARTITION 3",
		},
		{
			alter: "ALTER TABLE t partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
		},
		{
			alter: "ALTER TABLE t ADD COLUMN c1 INT, DROP COLUMN c2",
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.alter, func(t *testing.T) {
			if tcase.create == "" {
				tcase.create = "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))"
			}
			stmt, err := sqlparser.NewTestParser().ParseStrictDDL(tcase.create)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			stmt, err = sqlparser.NewTestParser().ParseStrictDDL(tcase.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			result, err := AlterTableRotatesRangePartition(createTable, alterTable)
			require.NoError(t, err)
			assert.Equal(t, tcase.expect, result)
		})
	}
}

func TestTemporalFunctions(t *testing.T) {
	tm, err := time.Parse(TimestampFormat, "2024-12-19 09:56:32")
	require.NoError(t, err)
	dt := datetime.NewDateTimeFromStd(tm)
	{
		// Compatible with `select YEAR('2024-12-19 09:56:32');`:
		assert.EqualValues(t, 2024, dt.Date.Year())
		numDays := datetime.MysqlDayNumber(dt.Date.Year(), dt.Date.Month(), dt.Date.Day())
		// Compatible with `select to_days('2024-12-19 09:56:32');`:
		assert.EqualValues(t, 739604, numDays)
		// Compatible with `select to_seconds('2024-12-19 09:56:32');`:
		assert.EqualValues(t, 63901821392, dt.ToSeconds())
	}
	{
		interval := datetime.ParseIntervalInt64(3, datetime.IntervalMonth, false)
		// Should be `2025-03-19 09:56:32`
		dt, _, ok := dt.AddInterval(interval, 0, false)
		require.True(t, ok)
		// Compatible with `select YEAR('2025-03-19 09:56:32');`:
		assert.EqualValues(t, 2025, dt.Date.Year())
		assert.EqualValues(t, 3, dt.Date.Month())
		assert.EqualValues(t, 19, dt.Date.Day())

		numDays := datetime.MysqlDayNumber(dt.Date.Year(), dt.Date.Month(), dt.Date.Day())
		// Compatible with `select to_days('2024-12-19 09:56:32' + INTERVAL 3 MONTH);`:
		assert.EqualValues(t, 739694, numDays)
		// Compatible with `select to_seconds('2024-12-19 09:56:32' + INTERVAL 3 MONTH);`:
		assert.EqualValues(t, 63909597392, dt.ToSeconds())
	}
	{
		yearweek := dt.Date.YearWeek(0)
		assert.EqualValues(t, 202450, yearweek)
	}
	{
		yearweek := dt.Date.YearWeek(1)
		assert.EqualValues(t, 202451, yearweek)
	}
	{
		yearweek := dt.Date.YearWeek(2)
		assert.EqualValues(t, 202450, yearweek)
	}
	{
		yearweek := dt.Date.YearWeek(3)
		assert.EqualValues(t, 202451, yearweek)
	}
	{
		yearweek := dt.Date.YearWeek(4)
		assert.EqualValues(t, 202451, yearweek)
	}
	{
		yearweek := dt.Date.YearWeek(5)
		assert.EqualValues(t, 202451, yearweek)
	}
	{
		yearweek := dt.Date.YearWeek(6)
		assert.EqualValues(t, 202451, yearweek)
	}
	{
		yearweek := dt.Date.YearWeek(7)
		assert.EqualValues(t, 202451, yearweek)
	}
}

func TestTruncateDateTime(t *testing.T) {
	tm, err := time.Parse(TimestampFormat, "2024-12-19 09:56:32")
	require.NoError(t, err)
	dt := datetime.NewDateTimeFromStd(tm)

	tcases := []struct {
		interval  datetime.IntervalType
		weekMode  int
		expect    string
		expectErr error
	}{
		{
			interval: datetime.IntervalYear,
			expect:   "2024-01-01 00:00:00",
		},
		{
			interval: datetime.IntervalMonth,
			expect:   "2024-12-01 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			expect:   "2024-12-15 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			weekMode: 1,
			expect:   "2024-12-16 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			weekMode: 2,
			expect:   "2024-12-15 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			weekMode: 3,
			expect:   "2024-12-16 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			weekMode: 4,
			expect:   "2024-12-15 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			weekMode: 5,
			expect:   "2024-12-16 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			weekMode: 6,
			expect:   "2024-12-15 00:00:00",
		},
		{
			interval: datetime.IntervalWeek,
			weekMode: 7,
			expect:   "2024-12-16 00:00:00",
		}, {
			interval:  datetime.IntervalWeek,
			weekMode:  8,
			expectErr: fmt.Errorf("invalid mode value 8 for WEEK/YEARWEEK function"),
		},
		{
			interval: datetime.IntervalDay,
			expect:   "2024-12-19 00:00:00",
		},
		{
			interval: datetime.IntervalHour,
			expect:   "2024-12-19 09:00:00",
		},
		{
			// No truncating for this resolution
			interval: datetime.IntervalMinute,
			expect:   "2024-12-19 09:56:32",
		},
		{
			// No truncating for this resolution
			interval: datetime.IntervalSecond,
			expect:   "2024-12-19 09:56:32",
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.interval.ToString(), func(t *testing.T) {
			truncated, err := truncateDateTime(dt, tcase.interval, tcase.weekMode)
			if tcase.expectErr != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tcase.expectErr.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tcase.expect, string(truncated.Format(0)))
		})
	}
}

func TestAnalyzeTemporalRangePartitioning(t *testing.T) {
	parseDateTime := func(s string) datetime.DateTime {
		dt, _, ok := datetime.ParseDateTime(s, -1)
		require.True(t, ok)

		require.Equal(t, s, string(dt.Format(0)))
		return dt
	}
	tcases := []struct {
		name      string
		create    string
		env       *Environment
		expect    *TemporalRangePartitioningAnalysis
		expectErr error
	}{
		{
			name:   "not partitioned",
			create: "CREATE TABLE t (id int PRIMARY KEY)",
			expect: &TemporalRangePartitioningAnalysis{
				Reason: "Table does not use PARTITION BY RANGE",
			},
		},
		{
			name:   "unknown column",
			create: "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 09:56:32')))",
			// Error created by validate()
			expectErr: &InvalidColumnInPartitionError{Table: "t", Column: "created_at"},
		},
		{
			name:   "partition by INT column",
			create: "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (1))",
			// Error created by validate()
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: false,
				Reason:                     "column id of type int in table t is not a temporal type for temporal range partitioning",
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("id")},
				},
			},
		},
		{
			name:   "range by unknown column",
			create: "CREATE TABLE t (id int, i INT, PRIMARY KEY (id, i)) PARTITION BY RANGE COLUMNS (i2) (PARTITION p0 VALUES LESS THAN (1))",
			// Error created by validate()
			expectErr: &InvalidColumnInPartitionError{Table: "t", Column: "i2"},
		},
		{
			name:   "range by TIME column",
			create: "CREATE TABLE t (id int, tm TIME, PRIMARY KEY (id, tm)) PARTITION BY RANGE COLUMNS (tm) (PARTITION p0 VALUES LESS THAN (1))",
			// Error created by AnalyzeTemporalRangePartitioning()
			expectErr: &UnsupportedRangeColumnsTypeError{Table: "t", Column: "tm", Type: "time"},
		},
		{
			name:   "range by DATE column",
			create: "CREATE TABLE t (id int, dt DATE, PRIMARY KEY (id, dt)) PARTITION BY RANGE COLUMNS (dt) (PARTITION p0 VALUES LESS THAN ('2025-01-01'))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				IsRangeColumns:             true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("dt")},
				},
				HighestValueDateTime: parseDateTime("2025-01-01 00:00:00"),
			},
		},
		{
			name:   "range by DATE column with MAXVALUE",
			create: "CREATE TABLE t (id int, dt DATE, PRIMARY KEY (id, dt)) PARTITION BY RANGE COLUMNS (dt) (PARTITION p0 VALUES LESS THAN ('2025-01-01'), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				IsRangeColumns:             true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("dt")},
				},
				MaxvaluePartition:    &sqlparser.PartitionDefinition{Name: sqlparser.NewIdentifierCI("pmax")},
				HighestValueDateTime: parseDateTime("2025-01-01 00:00:00"),
			},
		},
		{
			name:   "range by date (lower case) column",
			create: "CREATE TABLE t (id int, dt date, PRIMARY KEY (id, dt)) PARTITION BY RANGE COLUMNS (dt) (PARTITION p0 VALUES LESS THAN ('2025-01-01'))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				IsRangeColumns:             true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("dt")},
				},
				HighestValueDateTime: parseDateTime("2025-01-01 00:00:00"),
			},
		},
		{
			name:   "range by DATETIME column",
			create: "CREATE TABLE t (id int, dt datetime, PRIMARY KEY (id, dt)) PARTITION BY RANGE COLUMNS (dt) (PARTITION p0 VALUES LESS THAN ('2025-01-01'))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				IsRangeColumns:             true,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("dt")},
				},
				HighestValueDateTime: parseDateTime("2025-01-01 00:00:00"),
			},
		},
		{
			name:   "range by nonstandard named DATETIME column",
			create: "CREATE TABLE t (id int, `d-t` datetime, PRIMARY KEY (id, `d-t`)) PARTITION BY RANGE (TO_DAYS(`d-t`)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2025-01-01')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("d-t")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("TO_DAYS"),
				},
				HighestValueDateTime: parseDateTime("2025-01-01 00:00:00"),
			},
		},
		{
			name:   "range by DATETIME column, literal value",
			create: "CREATE TABLE t (id int, dt datetime, PRIMARY KEY (id, dt)) PARTITION BY RANGE (TO_DAYS(dt)) (PARTITION p0 VALUES LESS THAN (739617))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("dt")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("TO_DAYS"),
				},
				HighestValueIntVal:   739617,
				HighestValueDateTime: datetime.DateTime{},
			},
		},
		{
			name:      "range by TO_DAYS(TIMESTAMP)",
			create:    "CREATE TABLE t (id int, created_at TIMESTAMP, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 09:56:32')))",
			expectErr: fmt.Errorf("column type timestamp is unsupported in temporal range partitioning analysis for column created_at in table t"),
		},
		{
			name:   "range by TO_DAYS(DATETIME)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("TO_DAYS"),
				},
				HighestValueDateTime: parseDateTime("2024-12-19 00:00:00"),
			},
		},
		{
			name:   "range by TO_DAYS(DATETIME) with MAXVALUE",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 09:56:32')), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("TO_DAYS"),
				},
				MaxvaluePartition:    &sqlparser.PartitionDefinition{Name: sqlparser.NewIdentifierCI("pmax")},
				HighestValueDateTime: parseDateTime("2024-12-19 00:00:00"),
			},
		},
		{
			name:   "range by to_days(DATETIME) (lower case)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (to_days(created_at)) (PARTITION p0 VALUES LESS THAN (to_days('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalDay,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("to_days"),
				},
				HighestValueDateTime: parseDateTime("2024-12-19 00:00:00"),
			},
		},
		{
			name:   "range by to_seconds(DATETIME)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (to_seconds(created_at)) (PARTITION p0 VALUES LESS THAN (to_seconds('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalSecond,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("to_seconds"),
				},
				HighestValueDateTime: parseDateTime("2024-12-19 09:56:32"),
			},
		}, {
			name:   "range by year(DATETIME)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (year(created_at)) (PARTITION p0 VALUES LESS THAN (year('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalYear,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("year"),
				},
				HighestValueDateTime: parseDateTime("2024-01-01 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-15 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 0)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 0)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-15 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 1)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 1)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-16 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 2)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 2)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-15 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 3)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 3)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-16 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 4)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 4)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-15 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 5)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 5)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-16 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 6)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 6)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-15 00:00:00"),
			},
		},
		{
			name:   "range by YEARWEEK(DATETIME, 7)",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 7)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalWeek,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("YEARWEEK"),
				},
				HighestValueDateTime: parseDateTime("2024-12-16 00:00:00"),
			},
		},
		{
			name:      "unsupported YEARWEEK(DATETIME, 8)",
			create:    "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 8)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expectErr: fmt.Errorf("expression: YEARWEEK(`created_at`, 8) is unsupported in temporal range partitioning analysis in table t"),
		},
		{
			name:      "unsupported YEARWEEK(DATETIME, 'x')",
			create:    "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 'x')) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			expectErr: fmt.Errorf("expected integer literal argument in yearweek(`created_at`, 'x') function"),
		},
		{
			name:      "unsupported function expression",
			create:    "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (ABS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 09:56:32')))",
			expectErr: fmt.Errorf("expression: ABS(`created_at`) is unsupported in temporal range partitioning analysis in table t"),
		},
		{
			name:      "range by compound expression to_days(DATETIME)+1",
			create:    "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (to_days(created_at)+1) (PARTITION p0 VALUES LESS THAN (to_days('2024-12-19 09:56:32')+1))",
			expectErr: fmt.Errorf("expression: to_days(`created_at`) + 1 is unsupported in temporal range partitioning analysis in table t"),
		},
		{
			name:      "range by UNIX_TIMESTAMP(TIMESTAMP)",
			create:    "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2024-12-19 09:56:32')))",
			expectErr: fmt.Errorf("expression: UNIX_TIMESTAMP(`created_at`) is unsupported in temporal range partitioning analysis in table t"),
		},
		{
			name:   "range by UNIX_TIMESTAMP(TIMESTAMP), 8.4",
			create: "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2024-12-19 09:56:32')))",
			env:    New84TestEnv(),
			expect: &TemporalRangePartitioningAnalysis{
				IsRangePartitioned:         true,
				IsTemporalRangePartitioned: true,
				MinimalInterval:            datetime.IntervalSecond,
				Col: &ColumnDefinitionEntity{
					ColumnDefinition: &sqlparser.ColumnDefinition{Name: sqlparser.NewIdentifierCI("created_at")},
				},
				FuncExpr: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("UNIX_TIMESTAMP"),
				},
				HighestValueDateTime: parseDateTime("2024-12-19 09:56:32"),
			},
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			env := NewTestEnv()
			if tcase.env != nil {
				env = tcase.env
			}

			entity, err := NewCreateTableEntityFromSQL(env, tcase.create)
			require.NoError(t, err)

			result, err := AnalyzeTemporalRangePartitioning(entity)
			if tcase.expectErr != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tcase.expectErr.Error())
				return
			}
			assert.NoError(t, err)
			require.NotNil(t, result)
			assert.NoError(t, result.Error)
			require.NotNil(t, tcase.expect)
			assert.Equal(t, tcase.expect.Reason, result.Reason)
			require.NotNil(t, result)

			assert.Equal(t, tcase.expect.IsRangePartitioned, result.IsRangePartitioned, "IsRangePartitioned")
			assert.Equal(t, tcase.expect.IsTemporalRangePartitioned, result.IsTemporalRangePartitioned, "IsTemporalRangePartitioned")
			assert.Equal(t, tcase.expect.IsRangeColumns, result.IsRangeColumns, "IsRangeColumns")
			assert.Equal(t, tcase.expect.MinimalInterval, result.MinimalInterval, "MinimalInterval")
			if tcase.expect.Col != nil {
				require.NotNil(t, result.Col, "column")
				assert.Equal(t, tcase.expect.Col.Name(), result.Col.Name())
			} else {
				assert.Nil(t, result.Col, "column")
			}
			if tcase.expect.FuncExpr != nil {
				require.NotNil(t, result.FuncExpr)
				assert.Equal(t, tcase.expect.FuncExpr.Name.String(), result.FuncExpr.Name.String())
			} else {
				assert.Nil(t, result.FuncExpr, "funcExpr")
			}
			if tcase.expect.MaxvaluePartition != nil {
				require.NotNil(t, result.MaxvaluePartition)
				assert.Equal(t, tcase.expect.MaxvaluePartition.Name.String(), result.MaxvaluePartition.Name.String())
			} else {
				assert.Nil(t, result.MaxvaluePartition, "maxvaluePartition")
			}
			assert.Equal(t, tcase.expect.HighestValueDateTime, result.HighestValueDateTime)
			assert.Equal(t, tcase.expect.HighestValueIntVal, result.HighestValueIntVal)
		})
	}
}

func TestTemporalRangePartitioningNextRotation(t *testing.T) {
	tcases := []struct {
		name              string
		create            string
		interval          datetime.IntervalType
		weekMode          int
		prepareAheadCount int
		expactMaxValue    bool
		expectStatements  []string
		expectErr         error
	}{
		{
			name:              "not partitioned",
			create:            "CREATE TABLE t (id int)",
			interval:          datetime.IntervalHour,
			prepareAheadCount: 7,
			expectErr:         fmt.Errorf("Table does not use PARTITION BY RANGE"),
		},
		{
			name:              "interval too short: hour vs day",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 09:56:32')))",
			interval:          datetime.IntervalHour,
			prepareAheadCount: 7,
			expectErr:         fmt.Errorf("interval hour is less than the minimal interval day for table t"),
		},
		{
			name:              "interval too short: hour vs week",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:56:32')))",
			interval:          datetime.IntervalHour,
			prepareAheadCount: 7,
			expectErr:         fmt.Errorf("interval hour is less than the minimal interval week for table t"),
		},
		{
			name:              "interval too short: hour vs year",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEAR(created_at)) (PARTITION p0 VALUES LESS THAN (YEAR('2024-12-19 09:56:32')))",
			interval:          datetime.IntervalHour,
			prepareAheadCount: 7,
			expectErr:         fmt.Errorf("interval hour is less than the minimal interval year for table t"),
		},
		{
			name:              "interval too short: day vs year",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEAR(created_at)) (PARTITION p0 VALUES LESS THAN (YEAR('2024-12-19 09:56:32')))",
			interval:          datetime.IntervalDay,
			prepareAheadCount: 7,
			expectErr:         fmt.Errorf("interval day is less than the minimal interval year for table t"),
		},
		{
			name:              "interval too short: week vs year",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEAR(created_at)) (PARTITION p0 VALUES LESS THAN (YEAR('2024-12-19 09:56:32')))",
			interval:          datetime.IntervalWeek,
			prepareAheadCount: 7,
			expectErr:         fmt.Errorf("interval week is less than the minimal interval year for table t"),
		},
		{
			name:              "day interval with 7 days, DATE",
			create:            "CREATE TABLE t (id int, created_at DATE, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19')))",
			interval:          datetime.IntervalDay,
			prepareAheadCount: 7,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219` VALUES LESS THAN (739605))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241220` VALUES LESS THAN (739606))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241221` VALUES LESS THAN (739607))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241222` VALUES LESS THAN (739608))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241223` VALUES LESS THAN (739609))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241224` VALUES LESS THAN (739610))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241225` VALUES LESS THAN (739611))",
			},
		},
		{
			name:              "day interval with 7 days, DATETIME",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 09:56:32')))",
			interval:          datetime.IntervalDay,
			prepareAheadCount: 7,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219` VALUES LESS THAN (739605))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241220` VALUES LESS THAN (739606))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241221` VALUES LESS THAN (739607))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241222` VALUES LESS THAN (739608))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241223` VALUES LESS THAN (739609))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241224` VALUES LESS THAN (739610))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241225` VALUES LESS THAN (739611))",
			},
		},
		{
			name: "day interval with 7 days, DATETIME, 2 days covered",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (739604),
									PARTITION p20241219 VALUES LESS THAN (739605),
									PARTITION p20241220 VALUES LESS THAN (739606)
								)`,
			interval:          datetime.IntervalDay,
			prepareAheadCount: 7,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241221` VALUES LESS THAN (739607))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241222` VALUES LESS THAN (739608))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241223` VALUES LESS THAN (739609))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241224` VALUES LESS THAN (739610))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241225` VALUES LESS THAN (739611))",
			},
		},
		{
			name:              "range columns over datetime, day interval with 7 days",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'))",
			interval:          datetime.IntervalDay,
			prepareAheadCount: 7,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219` VALUES LESS THAN ('2024-12-20 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241220` VALUES LESS THAN ('2024-12-21 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241221` VALUES LESS THAN ('2024-12-22 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241222` VALUES LESS THAN ('2024-12-23 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241223` VALUES LESS THAN ('2024-12-24 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241224` VALUES LESS THAN ('2024-12-25 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241225` VALUES LESS THAN ('2024-12-26 00:00:00'))",
			},
		},
		{
			name:              "range columns over datetime, day interval with 7 days and MAXVALUE",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			interval:          datetime.IntervalDay,
			prepareAheadCount: 7,
			expactMaxValue:    true,
			expectStatements: []string{
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241219` VALUES LESS THAN ('2024-12-20 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241220` VALUES LESS THAN ('2024-12-21 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241221` VALUES LESS THAN ('2024-12-22 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241222` VALUES LESS THAN ('2024-12-23 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241223` VALUES LESS THAN ('2024-12-24 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241224` VALUES LESS THAN ('2024-12-25 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241225` VALUES LESS THAN ('2024-12-26 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
			},
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, 2 days covered",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			interval:          datetime.IntervalDay,
			prepareAheadCount: 7,
			expactMaxValue:    true,
			expectStatements: []string{
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241221` VALUES LESS THAN ('2024-12-22 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241222` VALUES LESS THAN ('2024-12-23 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241223` VALUES LESS THAN ('2024-12-24 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241224` VALUES LESS THAN ('2024-12-25 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
				"ALTER TABLE `t` REORGANIZE PARTITION `pmax` INTO (PARTITION `p20241225` VALUES LESS THAN ('2024-12-26 00:00:00'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)",
			},
		},
		{
			name:              "hour interval with 4 hours",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'))",
			interval:          datetime.IntervalHour,
			prepareAheadCount: 4,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219090000` VALUES LESS THAN ('2024-12-19 10:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219100000` VALUES LESS THAN ('2024-12-19 11:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219110000` VALUES LESS THAN ('2024-12-19 12:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219120000` VALUES LESS THAN ('2024-12-19 13:00:00'))",
			},
		},
		{
			name:              "hour interval with 4 hours, 2 of which are covered",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'), PARTITION p20241219100000 VALUES LESS THAN ('2024-12-19 10:00:00'), PARTITION p20241219110000 VALUES LESS THAN ('2024-12-19 11:00:00'))",
			interval:          datetime.IntervalHour,
			prepareAheadCount: 4,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219110000` VALUES LESS THAN ('2024-12-19 12:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241219120000` VALUES LESS THAN ('2024-12-19 13:00:00'))",
			},
		},
		{
			name:              "week(0) interval with 4 weeks",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'))",
			interval:          datetime.IntervalWeek,
			weekMode:          0,
			prepareAheadCount: 4,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241215` VALUES LESS THAN ('2024-12-22 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241222` VALUES LESS THAN ('2024-12-29 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241229` VALUES LESS THAN ('2025-01-05 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250105` VALUES LESS THAN ('2025-01-12 00:00:00'))",
			},
		},
		{
			name:              "week(1) interval with 4 weeks",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'))",
			interval:          datetime.IntervalWeek,
			weekMode:          1,
			prepareAheadCount: 4,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241216` VALUES LESS THAN ('2024-12-23 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241223` VALUES LESS THAN ('2024-12-30 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241230` VALUES LESS THAN ('2025-01-06 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250106` VALUES LESS THAN ('2025-01-13 00:00:00'))",
			},
		},
		{
			name: "week(1) interval with 4 weeks, 2 of which are covered",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
				PARTITION BY RANGE COLUMNS (created_at) (
					PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'),
					PARTITION p20241216 VALUES LESS THAN ('2024-12-23 00:00:00'),
					PARTITION p_somename VALUES LESS THAN ('2024-12-30 00:00:00')
				)`,
			interval:          datetime.IntervalWeek,
			weekMode:          1,
			prepareAheadCount: 4,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241230` VALUES LESS THAN ('2025-01-06 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250106` VALUES LESS THAN ('2025-01-13 00:00:00'))",
			},
		},
		{
			name:              "yearweek(0) function with 4 weeks",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 0)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:00:00', 1)))",
			interval:          datetime.IntervalWeek,
			weekMode:          0,
			prepareAheadCount: 4,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241215` VALUES LESS THAN (202451))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241222` VALUES LESS THAN (202452))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241229` VALUES LESS THAN (202501))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250105` VALUES LESS THAN (202502))",
			},
		},
		{
			name:              "yearweek(1) function with 4 weeks",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 1)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:00:00', 1)))",
			interval:          datetime.IntervalWeek,
			weekMode:          1,
			prepareAheadCount: 4,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241216` VALUES LESS THAN (202452))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241223` VALUES LESS THAN (202501))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241230` VALUES LESS THAN (202502))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250106` VALUES LESS THAN (202503))",
			},
		},
		{
			name:              "incompatible week mode",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (YEARWEEK(created_at, 0)) (PARTITION p0 VALUES LESS THAN (YEARWEEK('2024-12-19 09:00:00', 1)))",
			interval:          datetime.IntervalWeek,
			weekMode:          1,
			prepareAheadCount: 4,
			expectErr:         fmt.Errorf("mode 1 is different from the mode 0 used in table t"),
		},
		{
			name:              "month interval with 3 months",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'))",
			interval:          datetime.IntervalMonth,
			prepareAheadCount: 3,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20241201` VALUES LESS THAN ('2025-01-01 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250101` VALUES LESS THAN ('2025-02-01 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250201` VALUES LESS THAN ('2025-03-01 00:00:00'))",
			},
		},
		{
			name:              "month interval with 3 months, 1 covered",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'), PARTITION p20250101 VALUES LESS THAN ('2025-01-01 00:00:00'))",
			interval:          datetime.IntervalMonth,
			prepareAheadCount: 3,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250101` VALUES LESS THAN ('2025-02-01 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250201` VALUES LESS THAN ('2025-03-01 00:00:00'))",
			},
		}, {
			name: "month interval with 3 months, all covered",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'),
									PARTITION p20250101 VALUES LESS THAN ('2025-01-01 00:00:00'),
									PARTITION p20250201 VALUES LESS THAN ('2025-02-01 00:00:00'),
									PARTITION p20250301 VALUES LESS THAN ('2025-03-01 00:00:00'),
									PARTITION p20250401 VALUES LESS THAN ('2025-04-01 00:00:00')
								)`,
			interval:          datetime.IntervalMonth,
			prepareAheadCount: 3,
			expectStatements:  []string{},
		},
		{
			name:              "year interval with 3 years",
			create:            "CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at) (PARTITION p0 VALUES LESS THAN ('2024-12-19 09:00:00'))",
			interval:          datetime.IntervalYear,
			prepareAheadCount: 3,
			expectStatements: []string{
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20240101` VALUES LESS THAN ('2025-01-01 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20250101` VALUES LESS THAN ('2026-01-01 00:00:00'))",
				"ALTER TABLE `t` ADD PARTITION (PARTITION `p20260101` VALUES LESS THAN ('2027-01-01 00:00:00'))",
			},
		},
		{
			name:              "partition by INT column",
			create:            "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (1))",
			interval:          datetime.IntervalDay,
			prepareAheadCount: 3,
			expectErr:         fmt.Errorf("column id of type int in table t is not a temporal type for temporal range partitioning"),
		},
	}
	reference, err := time.Parse(TimestampFormat, "2024-12-19 09:56:32")
	require.NoError(t, err)
	env := NewTestEnv()
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			entity, err := NewCreateTableEntityFromSQL(env, tcase.create)
			require.NoError(t, err)

			diffs, err := TemporalRangePartitioningNextRotation(entity, tcase.interval, tcase.weekMode, tcase.prepareAheadCount, reference)
			if tcase.expectErr != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tcase.expectErr.Error())
				return
			}
			require.NoError(t, err)

			require.Len(t, diffs, len(tcase.expectStatements))
			for i, diff := range diffs {
				if tcase.expactMaxValue {
					assert.Len(t, diff.alterTable.PartitionSpec.Definitions, 2)
				} else {
					assert.Len(t, diff.alterTable.PartitionSpec.Definitions, 1)
				}
				assert.Equal(t, tcase.expectStatements[i], diff.CanonicalStatementString())
			}
		})
	}
}

func TestTemporalRangePartitioningRetention(t *testing.T) {
	tcases := []struct {
		name                     string
		create                   string
		expire                   string
		expectStatement          string
		expectDistinctStatements []string
		expectErr                error
	}{
		{
			name:      "not partitioned",
			create:    "CREATE TABLE t (id int)",
			expire:    "2024-12-19 09:00:00",
			expectErr: fmt.Errorf("Table does not use PARTITION BY RANGE"),
		},
		{
			name:   "day interval, no impact",
			create: "CREATE TABLE t (id int, created_at DATE, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19')))",
			expire: "2024-12-07 09:00:00",
		},
		{
			name:      "day interval, all partitions impacted",
			create:    "CREATE TABLE t (id int, created_at DATE, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19')))",
			expire:    "2024-12-19 09:00:00",
			expectErr: fmt.Errorf("retention at 2024-12-19 09:00:00 would drop all partitions in table t"),
		},
		{
			name:   "day interval with MAXVALUE, no impact",
			create: "CREATE TABLE t (id int, created_at DATE, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19')), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			expire: "2024-12-07 09:00:00",
		},
		{
			name:      "day interval with MAXVALUE, all partitions impacted",
			create:    "CREATE TABLE t (id int, created_at DATE, PRIMARY KEY(id, created_at)) PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19')), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			expire:    "2024-12-19 09:00:00",
			expectErr: fmt.Errorf("retention at 2024-12-19 09:00:00 would drop all partitions in table t"),
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, no impact",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			expire: "2024-12-18 09:00:00",
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, single partition dropped",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			expire:          "2024-12-19 00:00:00",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
			},
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, single partition dropped, passed threshold",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			expire:          "2024-12-19 01:02:03",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
			},
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, two partitions dropped",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			expire:          "2024-12-20 00:00:00",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`, `p20241220`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
				"ALTER TABLE `t` DROP PARTITION `p20241220`",
			},
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, two partitions dropped, passed threshold",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			expire:          "2024-12-20 23:59:59",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`, `p20241220`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
				"ALTER TABLE `t` DROP PARTITION `p20241220`",
			},
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, error dropping all partitions",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			expire:    "2024-12-21 00:00:00",
			expectErr: fmt.Errorf("retention at 2024-12-21 00:00:00 would drop all partitions in table t"),
		},
		{
			name: "range columns over datetime, day interval with 7 days and MAXVALUE, error dropping all partitions, futuristic",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at)) PARTITION BY RANGE COLUMNS (created_at)
								(
									PARTITION p0 VALUES LESS THAN ('2024-12-19 00:00:00'),
									PARTITION p20241220 VALUES LESS THAN ('2024-12-20 00:00:00'),
									PARTITION p_anyname VALUES LESS THAN ('2024-12-21 00:00:00'),
									PARTITION pmax VALUES LESS THAN MAXVALUE
								)`,
			expire:    "2025-01-01 00:00:00",
			expectErr: fmt.Errorf("retention at 2025-01-01 00:00:00 would drop all partitions in table t"),
		},
		{
			name: "day interval using TO_DAYS, DATETIME, no impact",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (739604),
									PARTITION p20241219 VALUES LESS THAN (739605),
									PARTITION p20241220 VALUES LESS THAN (739606)
								)`,
			expire: "2024-12-18 00:00:00",
		},
		{
			name: "day interval using TO_DAYS, DATETIME, drop 1 partition",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (739604),
									PARTITION p20241219 VALUES LESS THAN (739605),
									PARTITION p20241220 VALUES LESS THAN (739606)
								)`,
			expire:          "2024-12-19 00:00:00",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
			},
		},
		{
			name: "day interval using TO_DAYS, DATETIME, drop 2 partitions",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (739604),
									PARTITION p20241219 VALUES LESS THAN (739605),
									PARTITION p20241220 VALUES LESS THAN (739606)
								)`,
			expire:          "2024-12-20 00:00:00",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`, `p20241219`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
				"ALTER TABLE `t` DROP PARTITION `p20241219`",
			},
		},
		{
			name: "day interval using TO_DAYS, DATETIME, error dropping all partitions",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (739604),
									PARTITION p20241219 VALUES LESS THAN (739605),
									PARTITION p20241220 VALUES LESS THAN (739606)
								)`,
			expire:    "2024-12-21 00:00:00",
			expectErr: fmt.Errorf("retention at 2024-12-21 00:00:00 would drop all partitions in table t"),
		},
		{
			name: "day interval using TO_DAYS in expression, DATETIME, no impact",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 00:00:00')),
									PARTITION p20241219 VALUES LESS THAN (TO_DAYS('2024-12-20 00:00:00')),
									PARTITION p20241220 VALUES LESS THAN (TO_DAYS('2024-12-21 00:00:00'))
								)`,
			expire: "2024-12-18 00:00:00",
		},
		{
			name: "day interval using TO_DAYS in expression, DATETIME, drop 1 partition",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 00:00:00')),
									PARTITION p20241219 VALUES LESS THAN (TO_DAYS('2024-12-20 00:00:00')),
									PARTITION p20241220 VALUES LESS THAN (TO_DAYS('2024-12-21 00:00:00'))
								)`,
			expire:          "2024-12-19 00:00:00",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
			},
		},
		{
			name: "day interval using TO_DAYS in expression, DATETIME, drop 2 partitions",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 00:00:00')),
									PARTITION p20241219 VALUES LESS THAN (TO_DAYS('2024-12-20 00:00:00')),
									PARTITION p20241220 VALUES LESS THAN (TO_DAYS('2024-12-21 00:00:00'))
								)`,
			expire:          "2024-12-20 00:00:00",
			expectStatement: "ALTER TABLE `t` DROP PARTITION `p0`, `p20241219`",
			expectDistinctStatements: []string{
				"ALTER TABLE `t` DROP PARTITION `p0`",
				"ALTER TABLE `t` DROP PARTITION `p20241219`",
			},
		},
		{
			name: "day interval using TO_DAYS in expression, DATETIME, error dropping all partitions",
			create: `CREATE TABLE t (id int, created_at DATETIME, PRIMARY KEY(id, created_at))
							PARTITION BY RANGE (TO_DAYS(created_at))
								(
									PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-12-19 00:00:00')),
									PARTITION p20241219 VALUES LESS THAN (TO_DAYS('2024-12-20 00:00:00')),
									PARTITION p20241220 VALUES LESS THAN (TO_DAYS('2024-12-21 00:00:00'))
								)`,
			expire:    "2024-12-21 00:00:00",
			expectErr: fmt.Errorf("retention at 2024-12-21 00:00:00 would drop all partitions in table t"),
		},
		{
			name:      "partition by INT column",
			create:    "CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (1))",
			expire:    "2024-12-21 00:00:00",
			expectErr: fmt.Errorf("column id of type int in table t is not a temporal type for temporal range partitioning"),
		},
	}
	env := NewTestEnv()
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			expire, err := time.Parse(TimestampFormat, tcase.expire)
			require.NoError(t, err)

			entity, err := NewCreateTableEntityFromSQL(env, tcase.create)
			require.NoError(t, err)

			// Validate test input itself
			if tcase.expectStatement == "" {
				require.Empty(t, tcase.expectDistinctStatements)
			} else {
				require.NotEmpty(t, tcase.expectDistinctStatements)
			}
			t.Run("combined", func(t *testing.T) {
				diffs, err := TemporalRangePartitioningRetention(entity, expire, false)
				if tcase.expectErr != nil {
					require.Error(t, err)
					assert.EqualError(t, err, tcase.expectErr.Error())
					return
				}
				require.NoError(t, err)
				if tcase.expectStatement == "" {
					assert.Empty(t, diffs)
				} else {
					require.Len(t, diffs, 1)
					assert.Equal(t, tcase.expectStatement, diffs[0].CanonicalStatementString())
				}
			})
			t.Run("distinct", func(t *testing.T) {
				diffs, err := TemporalRangePartitioningRetention(entity, expire, true)
				if tcase.expectErr != nil {
					require.Error(t, err)
					assert.EqualError(t, err, tcase.expectErr.Error())
					return
				}
				require.NoError(t, err)
				if len(tcase.expectDistinctStatements) == 0 {
					assert.Empty(t, diffs)
				} else {
					require.Len(t, diffs, len(tcase.expectDistinctStatements))
					for i, diff := range diffs {
						assert.Equal(t, tcase.expectDistinctStatements[i], diff.CanonicalStatementString())
					}
				}
			})
		})
	}
}
