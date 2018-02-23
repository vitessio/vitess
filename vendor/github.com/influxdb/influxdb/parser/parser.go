package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type From struct {
	TableName string
}

type Operation int

type IntoClause struct {
	Target        *Value
	Backfill      bool
	BackfillValue *Value
}

type BasicQuery struct {
	startTime          time.Time
	endTime            time.Time
	startTimeSpecified bool
}

type SelectDeleteCommonQuery struct {
	BasicQuery
	FromClause *FromClause
	Condition  *WhereCondition
}

type SelectQuery struct {
	SelectDeleteCommonQuery
	ColumnNames   []*Value
	groupByClause *GroupByClause
	IntoClause    *IntoClause
	Limit         int
	Ascending     bool
	Explain       bool
}

type ListType int

const (
	Series ListType = iota
	ContinuousQueries
	SeriesWithRegex
)

type ListQuery struct {
	Type          ListType
	value         *Value
	IncludeSpaces bool
}

type DropQuery struct {
	Id int
}

type DropSeriesQuery struct {
	tableName string
}

func (self *DropSeriesQuery) GetTableName() string {
	return self.tableName
}

type DeleteQuery struct {
	SelectDeleteCommonQuery
}

type Query struct {
	SelectQuery     *SelectQuery
	DeleteQuery     *DeleteQuery
	ListQuery       *ListQuery
	DropSeriesQuery *DropSeriesQuery
	DropQuery       *DropQuery
	qType           QueryType
}

func (self *IntoClause) GetString() string {
	buffer := bytes.NewBufferString("")

	buffer.WriteString(self.Target.GetString())
	if self.BackfillValue != nil {
		fmt.Fprintf(buffer, " backfill(%s)", self.BackfillValue.GetString())
	}
	return buffer.String()
}

func (self *Query) Type() QueryType {
	return self.qType
}

func (self *Query) GetQueryString() string {
	return self.commonGetQueryString(false)
}

func (self *Query) GetQueryStringWithTimeCondition() string {
	return self.commonGetQueryString(true)
}

func (self *Query) commonGetQueryString(withTime bool) string {
	switch self.qType {
	case Select, Continuous:
		if withTime {
			return self.SelectQuery.GetQueryStringWithTimeCondition()
		}
		return self.SelectQuery.GetQueryString()
	case Delete:
		return self.DeleteQuery.GetQueryString(withTime)
	case DropContinuousQuery:
		return fmt.Sprintf("drop continuous query %d", self.DropQuery.Id)
	case ListSeries:
		return "list series"
	case ListContinuousQueries:
		return "list continuous queries"
	case DropSeries:
		return "drop series " + self.DropSeriesQuery.tableName
	default:
		panic(fmt.Errorf("Unknown query type %s", self.qType))
	}
}

func (self *Query) IsListQuery() bool {
	return self.ListQuery != nil
}

func (self *Query) IsExplainQuery() bool {
	return self.SelectQuery != nil && self.SelectQuery.Explain
}

func (self *Query) GetListSeriesQuery() *ListQuery {
	return self.ListQuery
}

func (self *Query) IsListSeriesQuery() bool {
	return self.ListQuery != nil && (self.ListQuery.Type == Series || self.ListQuery.Type == SeriesWithRegex)
}

func (self *Query) IsListContinuousQueriesQuery() bool {
	return self.ListQuery != nil && self.ListQuery.Type == ContinuousQueries
}

func (self *ListQuery) HasRegex() bool {
	return self.Type == SeriesWithRegex
}

func (self *ListQuery) IsCaseSensitive() bool {
	return self.value.IsInsensitive
}

func (self *ListQuery) GetRegex() *regexp.Regexp {
	regex, _ := self.value.GetCompiledRegex()
	return regex
}

func (self *DeleteQuery) GetQueryString(withTime bool) string {
	buffer := bytes.NewBufferString("delete ")
	fmt.Fprintf(buffer, "from %s", self.FromClause.GetString())
	if withTime {
		fmt.Fprintf(buffer, " where %s", self.GetWhereConditionWithTime(self.startTime, self.endTime).GetString())
	} else if condition := self.GetWhereCondition(); condition != nil {
		fmt.Fprintf(buffer, " where %s", condition.GetString())
	}
	return buffer.String()
}

func (self *SelectQuery) GetColumnNames() []*Value {
	return self.ColumnNames
}

func (self *SelectQuery) IsExplainQuery() bool {
	return self.Explain
}

func (self *SelectQuery) GetQueryString() string {
	return self.commonGetQueryStringWithTimes(false, true, self.startTime, self.endTime)
}

func (self *SelectQuery) GetQueryStringWithTimeCondition() string {
	// if this is a single point query then it already has a time (and
	// sequence number) condition; we don't need the extra (time < ???
	// and time > ???) condition in the query string.
	if self.IsSinglePointQuery() {
		return self.GetQueryString()
	}

	return self.commonGetQueryStringWithTimes(true, true, self.startTime, self.endTime)
}

func (self *SelectQuery) GetQueryStringWithTimes(startTime, endTime time.Time) string {
	return self.commonGetQueryStringWithTimes(true, true, startTime, endTime)
}

func (self *SelectQuery) GetQueryStringWithTimesAndNoIntoClause(startTime, endTime time.Time) string {
	return self.commonGetQueryStringWithTimes(true, false, startTime, endTime)
}

func (self *SelectQuery) commonGetQueryStringWithTimes(withTime, withIntoClause bool, startTime, endTime time.Time) string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "select ")

	buffer.WriteString(Values(self.ColumnNames).GetString())

	fmt.Fprintf(buffer, " from %s", self.FromClause.GetString())
	if withTime {
		fmt.Fprintf(buffer, " where %s", self.GetWhereConditionWithTime(startTime, endTime).GetString())
	} else if condition := self.GetWhereCondition(); condition != nil {
		fmt.Fprintf(buffer, " where %s", condition.GetString())
	}
	if self.GetGroupByClause() != nil && len(self.GetGroupByClause().Elems) > 0 {
		fmt.Fprintf(buffer, " group by %s", self.GetGroupByClause().GetString())
	}

	if self.Limit > 0 {
		fmt.Fprintf(buffer, " limit %d", self.Limit)
	}

	if self.Ascending {
		fmt.Fprintf(buffer, " order asc")
	}

	if clause := self.IntoClause; withIntoClause && clause != nil {
		fmt.Fprintf(buffer, " into %s", clause.GetString())
	}

	return buffer.String()
}

func (self *SelectQuery) IsSinglePointQuery() bool {
	w := self.GetWhereCondition()
	if w == nil {
		return false
	}

	leftWhereCondition, ok := w.GetLeftWhereCondition()
	if !ok {
		return false
	}

	leftBoolExpression, ok := leftWhereCondition.GetBoolExpression()
	if !ok {
		return false
	}

	rightBoolExpression, ok := w.Right.GetBoolExpression()
	if !ok {
		return false
	}

	if leftBoolExpression.Name != "=" && rightBoolExpression.Name != "=" {
		return false
	}

	if leftBoolExpression.Elems[0].Name != "time" || rightBoolExpression.Elems[0].Name != "sequence_number" {
		return false
	}

	return true
}

func (self *SelectQuery) GetSinglePointQuerySequenceNumber() (uint64, error) {
	w := self.GetWhereCondition()
	rightBoolExpression, _ := w.Right.GetBoolExpression()
	sequence := rightBoolExpression.Elems[1].Name
	sequence_number, err := strconv.ParseUint(sequence, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("The column sequence_number can only be queried as an integer.")
	}
	return sequence_number, nil
}

func (self *Query) IsContinuousQuery() bool {
	return self.qType == Continuous
}

func (self *SelectQuery) IsValidContinuousQuery() bool {
	groupByClause := self.GetGroupByClause()

	if len(groupByClause.Elems) == 0 {
		return true
	}

	for _, elem := range groupByClause.Elems {
		if elem.Name == "time" {
			return true
		}
	}

	return false
}

func (self *SelectQuery) IsNonRecursiveContinuousQuery() bool {
	fromClause := self.GetFromClause()
	intoClause := self.GetIntoClause()

	for _, from := range fromClause.Names {
		regex, ok := from.Name.GetCompiledRegex()

		if !ok {
			continue
		}

		regexString := regex.String()
		intoTarget := intoClause.Target.Name

		if !strings.Contains(intoTarget, ":series_name") {
			continue
		} else {
			if strings.HasPrefix(regexString, "^") && !strings.HasPrefix(intoTarget, ":series_name") {
				continue
			}

			if strings.HasSuffix(regexString, "$") && !strings.HasSuffix(intoTarget, ":series_name") {
				continue
			}

			return false
		}
	}

	return true
}

func (self *SelectQuery) GetIntoClause() *IntoClause {
	return self.IntoClause
}

func (self *SelectDeleteCommonQuery) GetFromClause() *FromClause {
	return self.FromClause
}

func setupSlice(hdr *reflect.SliceHeader, ptr unsafe.Pointer, size C.size_t) {
	hdr.Cap = int(size)
	hdr.Len = int(size)
	hdr.Data = uintptr(ptr)
}

func GetGroupByClause(groupByClause *C.groupby_clause) (*GroupByClause, error) {
	if groupByClause == nil {
		return &GroupByClause{Elems: nil}, nil
	}

	values, err := GetValueArray((*C.value_array)(groupByClause.elems))
	if err != nil {
		return nil, err
	}

	fillWithZero := false
	var fillValue *Value

	if groupByClause.fill_function != nil {
		fun, err := GetValue(groupByClause.fill_function)
		if err != nil {
			return nil, err
		}
		if fun.Name != "fill" {
			return nil, fmt.Errorf("You can't use %s with group by", fun.Name)
		}

		if len(fun.Elems) != 1 {
			return nil, fmt.Errorf("`fill` accepts one argument only")
		}

		fillValue = fun.Elems[0]
		fillWithZero = true
	}

	return &GroupByClause{
		Elems:        values,
		FillWithZero: fillWithZero,
		FillValue:    fillValue,
	}, nil
}

func GetValueArray(array *C.value_array) ([]*Value, error) {
	if array == nil {
		return nil, nil
	}

	var values []*C.value
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&values))), unsafe.Pointer(array.elems), array.size)

	valuesSlice := make([]*Value, 0, array.size)

	for _, value := range values {
		value, err := GetValue(value)
		if err != nil {
			return nil, err
		}
		valuesSlice = append(valuesSlice, value)
	}
	return valuesSlice, nil
}

func GetStringArray(array *C.array) []string {
	if array == nil {
		return nil
	}

	var values []*C.char
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&values))), unsafe.Pointer(array.elems), array.size)

	stringSlice := make([]string, 0, array.size)

	for _, value := range values {
		stringSlice = append(stringSlice, C.GoString(value))
	}
	return stringSlice
}

func GetValue(value *C.value) (*Value, error) {
	v := &Value{}
	v.Name = C.GoString(value.name)
	var err error
	v.Elems, err = GetValueArray((*C.value_array)(value.args))
	if err != nil {
		return nil, err
	}
	v.Type = ValueType(value.value_type)
	isCaseInsensitive := value.is_case_insensitive != 0
	if v.Type == ValueRegex {
		if isCaseInsensitive {
			v.compiledRegex, err = regexp.Compile("(?i)" + v.Name)
		} else {
			v.compiledRegex, err = regexp.Compile(v.Name)
		}
		v.IsInsensitive = isCaseInsensitive
	}
	if value.alias != nil {
		v.Alias = C.GoString(value.alias)
	}
	return v, err
}

func GetTableName(name *C.table_name) (*TableName, error) {
	value, err := GetValue(name.name)
	if err != nil {
		return nil, err
	}

	table := &TableName{Name: value}
	if name.alias != nil {
		table.Alias = C.GoString(name.alias)
	}

	return table, nil
}

func GetTableNameArray(array *C.table_name_array) ([]*TableName, error) {
	var names []*C.table_name
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&names))), unsafe.Pointer(array.elems), array.size)

	tableNamesSlice := make([]*TableName, 0, array.size)
	for _, name := range names {
		tableName, err := GetTableName(name)
		if err != nil {
			return nil, err
		}
		tableNamesSlice = append(tableNamesSlice, tableName)
	}
	return tableNamesSlice, nil
}

func GetFromClause(fromClause *C.from_clause) (*FromClause, error) {
	t := FromClauseType(fromClause.from_clause_type)
	var arr []*TableName
	var regex *regexp.Regexp

	switch t {
	case FromClauseMergeRegex, FromClauseJoinRegex:
		val, err := GetValue(fromClause.regex_value)
		if err != nil {
			return nil, err
		}
		if val.Type != ValueRegex {
			return nil, fmt.Errorf("merge() accepts regex only")
		}
		regex = val.compiledRegex
	default:
		var err error
		arr, err = GetTableNameArray(fromClause.names)
		if err != nil {
			return nil, err
		}
	}
	return &FromClause{t, arr, regex}, nil
}

func GetIntoClause(intoClause *C.into_clause) (*IntoClause, error) {
	if intoClause == nil {
		return nil, nil
	}

	backfill := true
	var backfillValue *Value = nil

	target, err := GetValue(intoClause.target)
	if err != nil {
		return nil, err
	}

	if intoClause.backfill_function != nil {
		fun, err := GetValue(intoClause.backfill_function)
		if err != nil {
			return nil, err
		}
		if fun.Name != "backfill" {
			return nil, fmt.Errorf("You can't use %s with into", fun.Name)
		}

		if len(fun.Elems) != 1 {
			return nil, fmt.Errorf("`backfill` accepts only one argument")
		}

		backfillValue = fun.Elems[0]
		backfill, err = strconv.ParseBool(backfillValue.GetString())
		if err != nil {
			return nil, fmt.Errorf("`backfill` accepts only bool arguments")
		}
	}

	return &IntoClause{
		Target:        target,
		Backfill:      backfill,
		BackfillValue: backfillValue,
	}, nil
}

func GetWhereCondition(condition *C.condition) (*WhereCondition, error) {
	if condition.is_bool_expression != 0 {
		expr, err := GetValue((*C.value)(condition.left))
		if err != nil {
			return nil, err
		}
		return &WhereCondition{
			isBooleanExpression: true,
			Left:                expr,
			Operation:           "",
			Right:               nil,
		}, nil
	}

	c := &WhereCondition{}
	var err error
	c.Left, err = GetWhereCondition((*C.condition)(condition.left))
	if err != nil {
		return nil, err
	}
	c.Operation = C.GoString(condition.op)
	c.Right, err = GetWhereCondition((*C.condition)(unsafe.Pointer(condition.right)))

	return c, err
}

func (self *SelectDeleteCommonQuery) GetWhereCondition() *WhereCondition {
	return self.Condition
}

func (self *SelectDeleteCommonQuery) GetWhereConditionWithTime(startTime, endTime time.Time) *WhereCondition {
	timeCondition := &WhereCondition{
		isBooleanExpression: false,
		Operation:           "AND",
		Left: &WhereCondition{
			isBooleanExpression: true,
			Left: &Value{
				Name: "<",
				Type: ValueExpression,
				Elems: []*Value{
					{Name: "time", Type: ValueSimpleName},
					{Name: strconv.FormatInt(endTime.UnixNano(), 10), Type: ValueInt},
				},
			},
		},
		Right: &WhereCondition{
			isBooleanExpression: true,
			Left: &Value{
				Name: ">",
				Type: ValueExpression,
				Elems: []*Value{
					{Name: "time", Type: ValueSimpleName},
					{Name: strconv.FormatInt(startTime.UnixNano(), 10), Type: ValueInt},
				},
			},
		},
	}

	if self.Condition == nil {
		return timeCondition
	}

	return &WhereCondition{
		isBooleanExpression: false,
		Left:                self.Condition,
		Right:               timeCondition,
		Operation:           "AND",
	}

}

func (self *SelectQuery) GetGroupByClause() *GroupByClause {
	return self.groupByClause
}

// This is just for backward compatability so we don't have
// to change all the code.
func ParseSelectQuery(query string) (*SelectQuery, error) {
	queries, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("No queries found")
	}

	selectQuery := queries[0].SelectQuery
	if selectQuery == nil {
		return nil, fmt.Errorf("Query isn't a select query: '%s'", queries[0].GetQueryString())
	}

	return selectQuery, nil
}

func parseSingleQuery(q *C.query) (*Query, error) {
	if q.list_series_query != nil {
		var value *Value
		var err error
		t := Series
		if q.list_series_query.has_regex != 0 {
			t = SeriesWithRegex
			value, err = GetValue(q.list_series_query.regex)
			if err != nil {
				return nil, err
			}
		}
		includeSpaces := false
		if q.list_series_query.include_spaces != 0 {
			includeSpaces = true
		}
		return &Query{ListQuery: &ListQuery{Type: t, value: value, IncludeSpaces: includeSpaces}, qType: ListSeries}, nil
	}

	if q.list_continuous_queries_query != 0 {
		return &Query{ListQuery: &ListQuery{Type: ContinuousQueries}, qType: ListContinuousQueries}, nil
	}

	if q.select_query != nil {
		selectQuery, err := parseSelectQuery(q.select_query)
		if err != nil {
			return nil, err
		}

		qType := Select
		if selectQuery.IntoClause != nil {
			qType = Continuous
		}
		return &Query{SelectQuery: selectQuery, qType: qType}, nil
	} else if q.delete_query != nil {
		deleteQuery, err := parseDeleteQuery(q.delete_query)
		if err != nil {
			return nil, err
		}
		return &Query{DeleteQuery: deleteQuery, qType: Delete}, nil
	} else if q.drop_series_query != nil {
		dropSeriesQuery, err := parseDropSeriesQuery(q.drop_series_query)
		if err != nil {
			return nil, err
		}
		return &Query{DropSeriesQuery: dropSeriesQuery, qType: DropSeries}, nil
	} else if q.drop_query != nil {
		return &Query{DropQuery: &DropQuery{Id: int(q.drop_query.id)}, qType: DropContinuousQuery}, nil
	}
	return nil, fmt.Errorf("Unknown query type encountered")
}

func ParseQuery(queryStr string) ([]*Query, error) {
	queryString := C.CString(queryStr)
	defer C.free(unsafe.Pointer(queryString))
	q := C.parse_query(queryString)
	defer C.close_queries(&q)

	if q.error != nil {
		str := C.GoString(q.error.err)
		return nil, &QueryError{
			firstLine:   int(q.error.first_line),
			firstColumn: int(q.error.first_column) - 1,
			lastLine:    int(q.error.last_line),
			lastColumn:  int(q.error.last_column) - 1,
			errorString: str,
			queryString: queryStr,
		}
	}

	var queries []*C.query
	setupSlice((*reflect.SliceHeader)((unsafe.Pointer(&queries))), unsafe.Pointer(q.qs), q.size)

	parsedQueries := make([]*Query, len(queries))
	for i, query := range queries {
		query, err := parseSingleQuery(query)
		if err != nil {
			return nil, err
		}
		parsedQueries[i] = query
	}
	return parsedQueries, nil
}

func parseDropSeriesQuery(dropSeriesQuery *C.drop_series_query) (*DropSeriesQuery, error) {
	name, err := GetValue(dropSeriesQuery.name)
	if err != nil {
		return nil, err
	}

	return &DropSeriesQuery{
		tableName: name.Name,
	}, nil
}

func parseSelectDeleteCommonQuery(fromClause *C.from_clause, whereCondition *C.condition) (SelectDeleteCommonQuery, error) {

	goQuery := SelectDeleteCommonQuery{
		BasicQuery: BasicQuery{
			startTime: time.Unix(math.MinInt64/1000000000, 0).UTC(),
			endTime:   time.Now().UTC(),
		},
	}

	var err error

	// get the from clause
	goQuery.FromClause, err = GetFromClause(fromClause)
	if err != nil {
		return goQuery, err
	}

	// get the where condition
	if whereCondition != nil {
		goQuery.Condition, err = GetWhereCondition(whereCondition)
		if err != nil {
			return goQuery, err
		}
	}

	var startTime, endTime *time.Time
	goQuery.Condition, endTime, err = getTime(goQuery.GetWhereCondition(), false)
	if err != nil {
		return goQuery, err
	}

	if endTime != nil {
		goQuery.endTime = *endTime
	}

	goQuery.Condition, startTime, err = getTime(goQuery.GetWhereCondition(), true)
	if err != nil {
		return goQuery, err
	}

	if startTime != nil {
		goQuery.startTime = *startTime
		goQuery.startTimeSpecified = true
	}

	return goQuery, nil
}

func parseSelectQuery(q *C.select_query) (*SelectQuery, error) {
	limit := q.limit
	if limit == -1 {
		// no limit by default
		limit = 0
	}

	basicQuery, err := parseSelectDeleteCommonQuery(q.from_clause, q.where_condition)
	if err != nil {
		return nil, err
	}

	goQuery := &SelectQuery{
		SelectDeleteCommonQuery: basicQuery,
		Limit:     int(limit),
		Ascending: q.ascending != 0,
		Explain:   q.explain != 0,
	}

	// get the column names
	goQuery.ColumnNames, err = GetValueArray((*C.value_array)(q.c))
	if err != nil {
		return nil, err
	}

	// get the group by clause
	if q.group_by == nil {
		goQuery.groupByClause = &GroupByClause{}
	} else {
		goQuery.groupByClause, err = GetGroupByClause(q.group_by)
		if err != nil {
			return nil, err
		}
	}

	// get the into clause
	goQuery.IntoClause, err = GetIntoClause(q.into_clause)
	if err != nil {
		return goQuery, err
	}

	return goQuery, nil
}

func parseDeleteQuery(query *C.delete_query) (*DeleteQuery, error) {
	basicQuery, err := parseSelectDeleteCommonQuery(query.from_clause, query.where_condition)
	if err != nil {
		return nil, err
	}
	goQuery := &DeleteQuery{
		SelectDeleteCommonQuery: basicQuery,
	}
	if basicQuery.GetWhereCondition() != nil {
		return nil, fmt.Errorf("Delete queries can't have where clause that don't reference time")
	}
	return goQuery, nil
}
