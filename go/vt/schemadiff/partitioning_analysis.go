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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	WeekModeUndefined = math.MinInt
)

// TemporalRangePartitioningAnalysis is the result of analyzing a table for temporal range partitioning.
type TemporalRangePartitioningAnalysis struct {
	IsRangePartitioned         bool                           // Is the table at all partitioned by RANGE?
	IsTemporalRangePartitioned bool                           // Is the table range partitioned using temporal values?
	IsRangeColumns             bool                           // Is RANGE COLUMNS used?
	MinimalInterval            datetime.IntervalType          // The minimal interval that the table is partitioned by (e.g. if partitioned by TO_DAYS, the minimal interval is 1 day)
	Col                        *ColumnDefinitionEntity        // The column used in the RANGE expression
	FuncExpr                   *sqlparser.FuncExpr            // The function used in the RANGE expression, if any
	WeekMode                   int                            // The mode used in the WEEK function, if that's what's used
	MaxvaluePartition          *sqlparser.PartitionDefinition // The partition that has MAXVALUE, if any
	HighestValueDateTime       datetime.DateTime              // The datetime value of the highest partition (excluding MAXVALUE)
	HighestValueIntVal         int64                          // The integer value of the highest partition (excluding MAXVALUE)
	Reason                     string                         // Why IsTemporalRangePartitioned is false
	Error                      error                          // If there was an error during analysis
}

// IsRangePartitioned returns `true` when the given CREATE TABLE statement is partitioned by RANGE.
func IsRangePartitioned(createTable *sqlparser.CreateTable) bool {
	if createTable.TableSpec.PartitionOption == nil {
		return false
	}
	if createTable.TableSpec.PartitionOption.Type != sqlparser.RangeType {
		return false
	}
	return true
}

// AlterTableRotatesRangePartition answers `true` when the given ALTER TABLE statement performs any sort
// of range partition rotation, that is applicable immediately and without moving data.
// Such would be:
// - Dropping any partition(s)
// - Adding a new partition (empty, at the end of the list)
func AlterTableRotatesRangePartition(createTable *sqlparser.CreateTable, alterTable *sqlparser.AlterTable) (bool, error) {
	// Validate original table is partitioned by RANGE
	if !IsRangePartitioned(createTable) {
		return false, nil
	}

	spec := alterTable.PartitionSpec
	if spec == nil {
		return false, nil
	}
	errorResult := func(conflictingNode sqlparser.SQLNode) error {
		return &PartitionSpecNonExclusiveError{
			Table:                alterTable.Table.Name.String(),
			PartitionSpec:        spec,
			ConflictingStatement: sqlparser.CanonicalString(conflictingNode),
		}
	}
	if len(alterTable.AlterOptions) > 0 {
		// This should never happen, unless someone programmatically tampered with the AlterTable AST.
		return false, errorResult(alterTable.AlterOptions[0])
	}
	if alterTable.PartitionOption != nil {
		// This should never happen, unless someone programmatically tampered with the AlterTable AST.
		return false, errorResult(alterTable.PartitionOption)
	}
	switch spec.Action {
	case sqlparser.AddAction:
		if len(spec.Definitions) > 1 {
			// This should never happen, unless someone programmatically tampered with the AlterTable AST.
			return false, errorResult(spec.Definitions[1])
		}
		return true, nil
	case sqlparser.DropAction:
		return true, nil
	default:
		return false, nil
	}
}

// supportedPartitioningScheme checks whether the given expression is supported for temporal range partitioning.
// schemadiff only supports a subset of range partitioning expressions.
func supportedPartitioningScheme(expr sqlparser.Expr, createTableEntity *CreateTableEntity, colName sqlparser.IdentifierCI, is84 bool) (matchFound bool, hasFunction bool, err error) {
	supportedVariations := []string{
		"create table %s (id int) PARTITION BY RANGE (%s)",
		"create table %s (id int) PARTITION BY RANGE (to_seconds(%s))",
		"create table %s (id int) PARTITION BY RANGE (to_days(%s))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 0))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 1))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 2))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 3))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 4))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 5))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 6))",
		"create table %s (id int) PARTITION BY RANGE (yearweek(%s, 7))",
		"create table %s (id int) PARTITION BY RANGE (year(%s))",
	}
	if is84 {
		supportedVariations = append(supportedVariations, "create table %s (id int) PARTITION BY RANGE (unix_timestamp(%s))")
	}
	supportedVariationsEntities := []*CreateTableEntity{}
	for _, supportedVariation := range supportedVariations {
		query := fmt.Sprintf(supportedVariation,
			sqlparser.CanonicalString(createTableEntity.CreateTable.GetTable().Name),
			sqlparser.CanonicalString(colName),
		)
		cte, err := NewCreateTableEntityFromSQL(createTableEntity.Env, query)
		if err != nil {
			return false, false, err
		}
		supportedVariationsEntities = append(supportedVariationsEntities, cte)
	}
	for i, cte := range supportedVariationsEntities {
		if sqlparser.Equals.Expr(expr, cte.CreateTable.TableSpec.PartitionOption.Expr) {
			return true, i > 0, nil
		}
	}
	return false, false, nil
}

// extractFuncWeekMode extracts the mode argument from a WEEK/YEARWEEK function, if applicable.
// It returns a ModeUndefined when not applicable.
func extractFuncWeekMode(funcExpr *sqlparser.FuncExpr) (int, error) {
	switch funcExpr.Name.Lowered() {
	case "week", "yearweek":
	default:
		return WeekModeUndefined, nil
	}
	if len(funcExpr.Exprs) <= 1 {
		return 0, nil
	}
	// There is a `mode` argument in the YEARWEEK function.
	literal, ok := funcExpr.Exprs[1].(*sqlparser.Literal)
	if !ok {
		return 0, fmt.Errorf("expected literal value in %v function", sqlparser.CanonicalString(funcExpr))
	}
	if literal.Type != sqlparser.IntVal {
		return 0, fmt.Errorf("expected integer literal argument in %v function", sqlparser.CanonicalString(funcExpr))
	}
	intval, err := strconv.ParseInt(literal.Val, 0, 64)
	if err != nil {
		return 0, err
	}
	return int(intval), nil
}

// AnalyzeTemporalRangePartitioning analyzes a table for temporal range partitioning.
func AnalyzeTemporalRangePartitioning(createTableEntity *CreateTableEntity) (*TemporalRangePartitioningAnalysis, error) {
	analysis := &TemporalRangePartitioningAnalysis{}
	withReason := func(msg string, args ...any) (*TemporalRangePartitioningAnalysis, error) {
		analysis.Reason = fmt.Sprintf(msg, args...)
		return analysis, nil
	}
	getColumn := func(lowered string) *ColumnDefinitionEntity {
		// The column will exist, because we invoke Validate() before this.
		return createTableEntity.ColumnDefinitionEntitiesMap()[lowered]
	}
	if err := createTableEntity.validate(); err != nil {
		return nil, err
	}
	if !IsRangePartitioned(createTableEntity.CreateTable) {
		return withReason("Table does not use PARTITION BY RANGE")
	}
	analysis.IsRangePartitioned = true
	is84, err := capabilities.ServerVersionAtLeast(createTableEntity.Env.MySQLVersion(), 8, 4)
	if err != nil {
		return nil, err
	}

	partitionOption := createTableEntity.CreateTable.TableSpec.PartitionOption
	if partitionOption.SubPartition != nil {
		return withReason("Table uses sub-partitions")
	}

	analysis.IsRangeColumns = len(partitionOption.ColList) > 0
	analysis.WeekMode = WeekModeUndefined
	switch len(partitionOption.ColList) {
	case 0:
		// This is a PARTITION BY RANGE(expr), where "expr" can be just column name, or a complex expression.
		// Of all the options, we only support the following:
		// - column_name
		// - to_seconds(column_name)
		// - to_days(column_name)
		// - year(column_name)
		// - unix_timestamp(column_name) (MySQL 8.4+)
		// Instead of programmatically validating that the expression is one of the above (a complex task),
		// we create dummy statements with all supported variations, and check for equality.
		var col *ColumnDefinitionEntity
		expr := sqlparser.CloneExpr(partitionOption.Expr)
		err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				col = getColumn(node.Name.Lowered()) // known to be not-nil thanks to validate()
				analysis.Col = col
			case *sqlparser.FuncExpr:
				analysis.FuncExpr = sqlparser.CloneRefOfFuncExpr(node)
				node.Name = sqlparser.NewIdentifierCI(node.Name.Lowered())
				mode, err := extractFuncWeekMode(node)
				if err != nil {
					return false, err
				}
				analysis.WeekMode = mode
			}
			return true, nil
		}, expr)
		if err != nil {
			return nil, err
		}
		matchFound, hasFunction, err := supportedPartitioningScheme(expr, createTableEntity, col.ColumnDefinition.Name, is84)
		if err != nil {
			return nil, err
		}
		if !matchFound {
			return nil, fmt.Errorf("expression: %v is unsupported in temporal range partitioning analysis in table %s", sqlparser.CanonicalString(partitionOption.Expr), createTableEntity.Name())
		}
		if !hasFunction {
			// First variation: no function, just the column
			if col.IsIntegralType() {
				return withReason("column %s of type %s in table %s is not a temporal type for temporal range partitioning", col.Name(), col.Type(), createTableEntity.Name())
			}
			return nil, fmt.Errorf("column type %s is unsupported in column %s in table %s indicated by RANGE expression", col.Type(), col.Name(), createTableEntity.Name())
		}
		// Has a function expression.
		// function must operate on a TIME, DATE, DATETIME column type. See https://dev.mysql.com/doc/refman/8.0/en/partitioning-range.html
		// And we only support DATE, DATETIME (because range rotation over a TIME column is not meaningful).
		// In MySQL 8.4 it is also possible to use a TIMESTAMP column specifically with UNIX_TIMESTAMP() function.
		// See https://dev.mysql.com/doc/refman/8.4/en/partitioning-range.html
		switch strings.ToLower(col.Type()) {
		case "date", "datetime":
			// OK
		case "timestamp":
			if is84 && analysis.FuncExpr != nil && analysis.FuncExpr.Name.Lowered() == "unix_timestamp" {
				analysis.MinimalInterval = datetime.IntervalSecond
			} else {
				return nil, fmt.Errorf("column type %s is unsupported in temporal range partitioning analysis for column %s in table %s", col.Type(), col.Name(), createTableEntity.Name())
			}
		default:
			return nil, fmt.Errorf("column type %s is unsupported in temporal range partitioning analysis for column %s in table %s", col.Type(), col.Name(), createTableEntity.Name())
		}
	case 1:
		// PARTITION BY RANGE COLUMNS (single_column). For temporal range rotation, we only
		// support DATE and DATETIME.
		col := getColumn(partitionOption.ColList[0].Lowered())
		analysis.Col = col
		switch strings.ToLower(col.Type()) {
		case "date":
			analysis.MinimalInterval = datetime.IntervalDay
		case "datetime":
			// Good!
		default:
			// Generally allowed by MySQL, but not considered as "temporal" for our purposes.
			return withReason("%s type in column %s is not temporal for temporal range partitioning purposes", col.Type(), col.Name())
		}
	default:
		// PARTITION BY RANGE COLUMNS (col1, col2, ...)
		// Multiple columns do not depict a temporal range.
		return withReason("Table uses multiple columns in RANGE COLUMNS")
	}
	analysis.IsTemporalRangePartitioned = true
	if analysis.FuncExpr != nil {
		switch analysis.FuncExpr.Name.Lowered() {
		case "unix_timestamp":
			analysis.MinimalInterval = datetime.IntervalSecond
		case "to_seconds":
			analysis.MinimalInterval = datetime.IntervalSecond
		case "to_days":
			analysis.MinimalInterval = datetime.IntervalDay
		case "yearweek":
			analysis.MinimalInterval = datetime.IntervalWeek
		case "year":
			analysis.MinimalInterval = datetime.IntervalYear
		}
	}
	for _, partition := range partitionOption.Definitions {
		if partition.Options == nil {
			continue
		}
		if partition.Options.ValueRange == nil {
			continue
		}
		if partition.Options.ValueRange.Maxvalue {
			analysis.MaxvaluePartition = partition
		} else {
			highestValueDateTime, highestValueIntval, err := computeDateTime(partition.Options.ValueRange.Range[0], analysis.Col.Type(), analysis.FuncExpr)
			if err != nil {
				return analysis, err
			}
			highestValueDateTime, err = truncateDateTime(highestValueDateTime, analysis.MinimalInterval, analysis.WeekMode)
			if err != nil {
				return analysis, err
			}
			analysis.HighestValueDateTime = highestValueDateTime
			analysis.HighestValueIntVal = highestValueIntval
		}
	}
	return analysis, nil
}

func parseDateTime(s string, colType string) (result datetime.DateTime, err error) {
	switch strings.ToLower(colType) {
	case "date":
		d, ok := datetime.ParseDate(s)
		if ok {
			return datetime.DateTime{Date: d}, nil
		}
		return result, fmt.Errorf("invalid date literal %s", s)
	case "datetime":
		result, _, ok := datetime.ParseDateTime(s, -1)
		if ok {
			return result, err
		}
		// It's also OK to parse a datetime out of a date.
		d, ok := datetime.ParseDate(s)
		if ok {
			return datetime.DateTime{Date: d}, nil
		}
		return result, fmt.Errorf("invalid datetime literal %s", s)
	default:
		return result, fmt.Errorf("unsupported column type %s", colType)
	}
}

// computeDateTime computes a datetime value from a given expression.
// We assume AnalyzeTemporalRangePartitioning has already executed, which means we've validated the expression
// to be one of supported variations.
func computeDateTime(expr sqlparser.Expr, colType string, funcExpr *sqlparser.FuncExpr) (dt datetime.DateTime, intval int64, err error) {
	if funcExpr == nil {
		// This is a simple column name, and we only support DATE and DATETIME types. So the value
		// must be a literal date or datetime representation, e.g. '2021-01-05' or '2021-01-05 17:00:00'.
		literal, ok := expr.(*sqlparser.Literal)
		if !ok {
			return dt, 0, fmt.Errorf("expected literal value in %s", sqlparser.CanonicalString(expr))
		}
		if literal.Type != sqlparser.StrVal {
			return dt, 0, fmt.Errorf("expected string literal value in %s", sqlparser.CanonicalString(expr))
		}
		dt, err = parseDateTime(literal.Val, colType)
		return dt, 0, err
	}
	// The table is partitioned using a function.
	// The function may or may not appear in the expression. Normally it will not, since MySQL computes a literal out of a
	// partition definition with function, e.g.
	//   `PARTITION p0 VALUES LESS THAN (TO_DAYS('2021-01-01'))` computes to
	//   `PARTITION p0 VALUES LESS THAN (738156)`.
	// However, schemadiff supports declarative statements, and those may include the function.

	var literal *sqlparser.Literal
	var hasFuncExpr bool
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.FuncExpr:
			if node.Name.Lowered() != funcExpr.Name.Lowered() {
				return false, fmt.Errorf("expected function %s, got %s", funcExpr.Name.Lowered(), node.Name.Lowered())
			}
			hasFuncExpr = true
		case *sqlparser.Literal:
			if literal == nil {
				literal = node
			}
		}
		return true, nil
	}, expr)
	if err != nil {
		return dt, 0, err
	}
	if literal == nil {
		return dt, 0, fmt.Errorf("expected literal value in %s", sqlparser.CanonicalString(expr))
	}
	if hasFuncExpr {
		// e.g. `PARTITION p0 VALUES LESS THAN (TO_DAYS('2021-01-01'))`
		// The literal must be a DATE or DATETIME, on which the function operates.
		if literal.Type != sqlparser.StrVal {
			return dt, 0, fmt.Errorf("expected string literal value in %s", sqlparser.CanonicalString(expr))
		}
		// The literal is the the value we're looking for.
		dt, err = parseDateTime(literal.Val, colType)
		return dt, 0, err
	}
	// No function expression
	// e.g. `PARTITION p0 VALUES LESS THAN (738156)`
	// The literal must be an integer, because the function is not present in the expression.
	if literal.Type != sqlparser.IntVal {
		return dt, 0, fmt.Errorf("expected integer literal value in %s", sqlparser.CanonicalString(expr))
	}
	intval, err = strconv.ParseInt(literal.Val, 0, 64)
	if err != nil {
		return dt, 0, err
	}
	return dt, intval, nil
}

func applyFuncExprToDateTime(dt datetime.DateTime, funcExpr *sqlparser.FuncExpr) (intval int64, err error) {
	switch funcExpr.Name.Lowered() {
	case "unix_timestamp":
		intval = dt.ToStdTime(time.Time{}).UTC().Unix()
	case "to_seconds":
		intval = dt.ToSeconds()
	case "to_days":
		intval = int64(datetime.MysqlDayNumber(dt.Date.Year(), dt.Date.Month(), dt.Date.Day()))
	case "yearweek":
		mode, err := extractFuncWeekMode(funcExpr)
		if err != nil {
			return 0, err
		}
		intval = int64(dt.Date.YearWeek(mode))
	case "year":
		intval = int64(dt.Date.Year())
	default:
		return 0, fmt.Errorf("unsupported funcExpr %s", funcExpr.Name.String())
	}
	return intval, nil
}

// temporalPartitionName returns a name for a partition, based on a given DATETIME and resolution.
// It either returns a short name or a long name, depending on the resolution.
// The name is prepended with "p".
// It's noteworthy that this is naming system is purely conventional. MySQL does not enforce any naming convention.
func temporalPartitionName(dt datetime.DateTime, resolution datetime.IntervalType) (string, error) {
	switch resolution {
	case datetime.IntervalYear,
		datetime.IntervalMonth,
		datetime.IntervalWeek,
		datetime.IntervalDay:
		return "p" + string(datetime.Date_YYYYMMDD.Format(dt, 0)), nil
	case datetime.IntervalHour,
		datetime.IntervalMinute,
		datetime.IntervalSecond:
		return "p" + string(datetime.DateTime_YYYYMMDDhhmmss.Format(dt, 0)), nil
	}
	return "", fmt.Errorf("unsupported resolution %s", resolution.ToString())
}

// truncateDateTime truncates a datetime to a given resolution.
// e.g. if resolution is IntervalDay, the time part is removed.
// If resolution is IntervalMonth, the day part is set to 1.
// etc.
// `weekMode` is used for WEEK calculations, see https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_week
func truncateDateTime(dt datetime.DateTime, interval datetime.IntervalType, weekMode int) (datetime.DateTime, error) {
	if interval >= datetime.IntervalHour {
		// Remove the minutes, seconds, subseconds parts
		hourInterval := datetime.ParseIntervalInt64(int64(dt.Time.Hour()), datetime.IntervalHour, false)
		dt = datetime.DateTime{Date: dt.Date} // Remove the Time part
		var ok bool
		dt, _, ok = dt.AddInterval(hourInterval, 0, false)
		if !ok {
			return dt, fmt.Errorf("failed to add interval %v to reference time %v", hourInterval, dt.Format(0))
		}
	}
	if interval >= datetime.IntervalDay {
		// Remove the Time part:
		dt = datetime.DateTime{Date: dt.Date}
	}
	if interval == datetime.IntervalWeek {
		// IntervalWeek = IntervalDay | intervalMulti, which is larger than IntervalYear, so we interject here
		// Get back to the first day of the week:
		var startOfWeekInterval *datetime.Interval
		switch weekMode {
		case 0, 2, 4, 6:
			startOfWeekInterval = datetime.ParseIntervalInt64(-int64(dt.Date.Weekday()), datetime.IntervalDay, false)
		case 1, 3, 5, 7:
			startOfWeekInterval = datetime.ParseIntervalInt64(-int64(dt.Date.Weekday()-1), datetime.IntervalDay, false)
		default:
			return dt, fmt.Errorf("invalid mode value %d for WEEK/YEARWEEK function", weekMode)
		}
		var ok bool
		dt, _, ok = dt.AddInterval(startOfWeekInterval, 0, false)
		if !ok {
			return dt, fmt.Errorf("failed to add interval %v to reference time %v", startOfWeekInterval, dt.Format(0))
		}
		return dt, nil
	}
	if interval >= datetime.IntervalMonth {
		// Get back to the first day of the month:
		dayInterval := datetime.ParseIntervalInt64(int64(-(dt.Date.Day() - 1)), datetime.IntervalDay, false)
		var ok bool
		dt, _, ok = dt.AddInterval(dayInterval, 0, false)
		if !ok {
			return dt, fmt.Errorf("failed to add interval %v to reference time %v", dayInterval, dt.Format(0))
		}
	}
	if interval >= datetime.IntervalYear {
		// Get back to the first day of the year:
		monthInterval := datetime.ParseIntervalInt64(int64(-(dt.Date.Month() - 1)), datetime.IntervalMonth, false)
		var ok bool
		dt, _, ok = dt.AddInterval(monthInterval, 0, false)
		if !ok {
			return dt, fmt.Errorf("failed to add interval %v to reference time %v", monthInterval, dt.Format(0))
		}
	}
	return dt, nil
}

// TemporalRangePartitioningNextRotation returns a (possibly empty) diffs that, if run sequentially,
// will rotate forward a range partitioned table.
// The table must be range partitioned by temporal values, otherwise an error is returned.
// The input indicates how many rotations to prepare ahead, and the reference time to start from,
// e.g. "prepare 7 days ahead, starting from today".
// The function computes values of existing partitions to determine how many new partitions are actually
// required to satisfy the terms.
func TemporalRangePartitioningNextRotation(createTableEntity *CreateTableEntity, interval datetime.IntervalType, weekMode int, prepareAheadCount int, reference time.Time) (diffs []*AlterTableEntityDiff, err error) {
	analysis, err := AnalyzeTemporalRangePartitioning(createTableEntity)
	if err != nil {
		return nil, err
	}
	if !analysis.IsTemporalRangePartitioned {
		return nil, errors.New(analysis.Reason)
	}
	intervalIsTooSmall := false
	// IntervalWeek = IntervalDay | intervalMulti, which is larger all of the rest of normal intervals,
	// so we need special handling for IntervalWeek comparisons
	switch {
	case analysis.MinimalInterval == datetime.IntervalWeek:
		intervalIsTooSmall = (interval <= datetime.IntervalDay)
	case interval == datetime.IntervalWeek:
		intervalIsTooSmall = (analysis.MinimalInterval >= datetime.IntervalMonth)
	default:
		intervalIsTooSmall = (interval < analysis.MinimalInterval)
	}
	if intervalIsTooSmall {
		return nil, fmt.Errorf("interval %s is less than the minimal interval %s for table %s", interval.ToString(), analysis.MinimalInterval.ToString(), createTableEntity.Name())
	}
	if analysis.WeekMode != WeekModeUndefined && weekMode != analysis.WeekMode {
		return nil, fmt.Errorf("mode %d is different from the mode %d used in table %s", weekMode, analysis.WeekMode, createTableEntity.Name())
	}
	referenceDatetime, err := truncateDateTime(datetime.NewDateTimeFromStd(reference), interval, weekMode)
	if err != nil {
		return nil, err
	}

	for i := range prepareAheadCount {
		aheadInterval := datetime.ParseIntervalInt64(int64(i+1), interval, false)
		aheadDatetime, _, ok := referenceDatetime.AddInterval(aheadInterval, 0, false)
		if !ok {
			return nil, fmt.Errorf("failed to add interval %v to reference time %v", aheadInterval, reference)
		}
		if !analysis.HighestValueDateTime.IsZero() && aheadDatetime.Compare(analysis.HighestValueDateTime) <= 0 {
			// This `LESS THAN` value is already covered by an existing partition.
			continue
		}

		var partitionExpr sqlparser.Literal
		switch {
		case analysis.IsRangeColumns:
			// PARTITION BY RANGE COLUMNS. The column could be DATE or DATETIME
			partitionExpr.Type = sqlparser.StrVal
			switch strings.ToLower(analysis.Col.Type()) {
			case "date":
				partitionExpr.Val = string(aheadDatetime.Date.Format())
			case "datetime":
				partitionExpr.Val = string(aheadDatetime.Format(0))
			default:
				return nil, fmt.Errorf("unsupported partitioning rotation in table %s", createTableEntity.Name())
			}
		case analysis.FuncExpr != nil:
			partitionExpr.Type = sqlparser.IntVal
			intval, err := applyFuncExprToDateTime(aheadDatetime, analysis.FuncExpr)
			if err != nil {
				return nil, err
			}
			if analysis.HighestValueDateTime.IsZero() && intval <= analysis.HighestValueIntVal {
				// This `LESS THAN` value is already covered by an existing partition.
				continue
			}

			partitionExpr.Val = fmt.Sprintf("%d", intval)
		default:
			return nil, fmt.Errorf("unsupported partitioning rotation in table %s", createTableEntity.Name())
		}
		// Compute new partition:
		partitionNameAheadInterval := datetime.ParseIntervalInt64(int64(i), interval, false)
		partitionNameAheadDatetime, _, ok := referenceDatetime.AddInterval(partitionNameAheadInterval, 0, false)
		if !ok {
			return nil, fmt.Errorf("failed to add interval %v to reference time %v", partitionNameAheadInterval, reference)
		}
		partitionName, err := temporalPartitionName(partitionNameAheadDatetime, interval)
		if err != nil {
			return nil, err
		}
		newPartition := &sqlparser.PartitionDefinition{
			Name: sqlparser.NewIdentifierCI(partitionName),
			Options: &sqlparser.PartitionDefinitionOptions{
				ValueRange: &sqlparser.PartitionValueRange{
					Type:     sqlparser.LessThanType,
					Maxvalue: false,
					Range: []sqlparser.Expr{
						&partitionExpr,
					},
				},
			},
		}
		// Build the diff
		var partitionSpec *sqlparser.PartitionSpec
		switch analysis.MaxvaluePartition {
		case nil:
			// ADD PARTITION
			partitionSpec = &sqlparser.PartitionSpec{
				Action:      sqlparser.AddAction,
				Definitions: []*sqlparser.PartitionDefinition{newPartition},
			}
		default:
			// REORGANIZE PARTITION
			// alter table `test`.`quarterly_report_status` reorganize partition `p6` into (partition `p20090701000000` values less than (1246395600) /* 2009-07-01 00:00:00 */ , partition p_maxvalue values less than MAXVALUE)
			partitionSpec = &sqlparser.PartitionSpec{
				Action:      sqlparser.ReorganizeAction,
				Names:       sqlparser.Partitions{analysis.MaxvaluePartition.Name},
				Definitions: []*sqlparser.PartitionDefinition{newPartition, analysis.MaxvaluePartition},
			}
		}
		alterTable := &sqlparser.AlterTable{
			Table:         createTableEntity.Table,
			PartitionSpec: partitionSpec,
		}
		diff := &AlterTableEntityDiff{alterTable: alterTable, from: createTableEntity}
		diffs = append(diffs, diff)
	}
	return diffs, nil
}

// TemporalRangePartitioningRetention generates a ALTER TABLE ... DROP PARTITION diff that drops all temporal partitions
// expired by given time. The function returns nil if no partitions are expired. The functions returns an error if
// all partitions were to be dropped, as this is highly unlikely to be the intended action.
func TemporalRangePartitioningRetention(createTableEntity *CreateTableEntity, expire time.Time, distinctDiffs bool) (diffs []*AlterTableEntityDiff, err error) {
	analysis, err := AnalyzeTemporalRangePartitioning(createTableEntity)
	if err != nil {
		return nil, err
	}
	if !analysis.IsTemporalRangePartitioned {
		return nil, errors.New(analysis.Reason)
	}
	expireDatetime := datetime.NewDateTimeFromStd(expire)
	if err != nil {
		return nil, err
	}

	var droppedPartitionNames sqlparser.Partitions
	countValueRangePartitions := 0
	for _, partition := range createTableEntity.TableSpec.PartitionOption.Definitions {
		if partition.Options == nil {
			continue
		}
		if partition.Options.ValueRange == nil {
			continue
		}
		if partition.Options.ValueRange.Maxvalue {
			break
		}
		countValueRangePartitions++
		dt, intval, err := computeDateTime(partition.Options.ValueRange.Range[0], analysis.Col.Type(), analysis.FuncExpr)
		if err != nil {
			return nil, err
		}
		switch {
		case dt.IsZero() && analysis.FuncExpr != nil:
			// Partition uses an intval, such as in:
			// PARTITION p0 VALUES LESS THAN (738156)
			expireIntval, err := applyFuncExprToDateTime(expireDatetime, analysis.FuncExpr)
			if err != nil {
				return nil, err
			}
			if intval <= expireIntval {
				droppedPartitionNames = append(droppedPartitionNames, partition.Name)
			}
		case !dt.IsZero():
			// Partition uses a datetime, such as in these examples:
			// - PARTITION p0 VALUES LESS THAN ('2021-01-01 00:00:00')
			// - PARTITION p0 VALUES LESS THAN (TO_DAYS('2021-01-01'))
			if dt.Compare(expireDatetime) <= 0 {
				droppedPartitionNames = append(droppedPartitionNames, partition.Name)
			}
		default:
			// Should never get here
			return nil, fmt.Errorf("unsupported partitioning in table %s", createTableEntity.Name())
		}
	}
	if len(droppedPartitionNames) == 0 {
		return nil, nil
	}
	if len(droppedPartitionNames) == countValueRangePartitions {
		// This would DROP all partitions, which is highly unlikely to be the intended action.
		// We reject this operation.
		return nil, fmt.Errorf("retention at %s would drop all partitions in table %s", expireDatetime.Format(0), createTableEntity.Name())
	}
	appendAlterTable := func(names sqlparser.Partitions) {
		alterTable := &sqlparser.AlterTable{
			Table: createTableEntity.Table,
			PartitionSpec: &sqlparser.PartitionSpec{
				Action: sqlparser.DropAction,
				Names:  names,
			},
		}
		diff := &AlterTableEntityDiff{alterTable: alterTable, from: createTableEntity}
		diffs = append(diffs, diff)
	}
	if distinctDiffs {
		// Generate a separate ALTER TABLE for each partition to be dropped.
		for _, name := range droppedPartitionNames {
			appendAlterTable(sqlparser.Partitions{name})
		}
	} else {
		// Generate a single ALTER TABLE for all partitions to be dropped.
		appendAlterTable(droppedPartitionNames)
	}
	return diffs, nil
}
