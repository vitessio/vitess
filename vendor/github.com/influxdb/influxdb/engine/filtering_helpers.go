package engine

import (
	"fmt"
	"strconv"

	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

func getExpressionValue(values []*parser.Value, fields []string, point *protocol.Point) ([]*protocol.FieldValue, error) {
	fieldValues := []*protocol.FieldValue{}
	for _, value := range values {
		switch value.Type {
		case parser.ValueFunctionCall:
			return nil, fmt.Errorf("Cannot process function call %s in expression", value.Name)
		case parser.ValueInt:
			value, err := strconv.ParseInt(value.Name, 10, 64)
			if err == nil {
				fieldValues = append(fieldValues, &protocol.FieldValue{Int64Value: &value})
				continue
			}
			// an error will happen only if the integer is out of range
			fallthrough
		case parser.ValueFloat:
			value, _ := strconv.ParseFloat(value.Name, 64)
			fieldValues = append(fieldValues, &protocol.FieldValue{DoubleValue: &value})
		case parser.ValueBool:
			value, _ := strconv.ParseBool(value.Name)
			fieldValues = append(fieldValues, &protocol.FieldValue{BoolValue: &value})
		case parser.ValueString:
			fieldValues = append(fieldValues, &protocol.FieldValue{StringValue: &value.Name})
		case parser.ValueRegex:
			regex, _ := value.GetCompiledRegex()
			regexStr := regex.String()
			fieldValues = append(fieldValues, &protocol.FieldValue{StringValue: &regexStr})

		case parser.ValueTableName, parser.ValueSimpleName:

			// TODO: optimize this so we don't have to lookup the column everytime
			fieldIdx := -1
			for idx, field := range fields {
				if field == value.Name {
					fieldIdx = idx
					break
				}
			}

			if fieldIdx == -1 {
				return nil, fmt.Errorf("Cannot find column %s", value.Name)
			}
			fieldValues = append(fieldValues, point.Values[fieldIdx])

		case parser.ValueExpression:
			v, err := GetValue(value, fields, point)
			if err != nil {
				return nil, err
			}
			fieldValues = append(fieldValues, v)
		default:
			return nil, fmt.Errorf("Cannot evaluate expression")
		}
	}

	return fieldValues, nil
}

func matchesExpression(expr *parser.Value, fields []string, point *protocol.Point) (bool, error) {
	leftValue, err := getExpressionValue(expr.Elems[:1], fields, point)
	if err != nil {
		return false, err
	}
	rightValue, err := getExpressionValue(expr.Elems[1:], fields, point)
	if err != nil {
		return false, err
	}

	operator := registeredOperators[expr.Name]
	if operator == nil {
		return false, fmt.Errorf("Invalid boolean operator %s", expr.Name)
	}

	ok, err := operator(leftValue[0], rightValue)
	return ok == MATCH, err
}

func matches(condition *parser.WhereCondition, fields []string, point *protocol.Point) (bool, error) {
	if expr, ok := condition.GetBoolExpression(); ok {
		return matchesExpression(expr, fields, point)
	}

	left, _ := condition.GetLeftWhereCondition()
	leftResult, err := matches(left, fields, point)
	if err != nil {
		return false, err
	}

	// short circuit
	if !leftResult && condition.Operation == "AND" ||
		leftResult && condition.Operation == "OR" {
		return leftResult, nil
	}

	return matches(condition.Right, fields, point)
}

func getColumns(values []*parser.Value, columns map[string]bool) {
	for _, v := range values {
		switch v.Type {
		case parser.ValueSimpleName:
			columns[v.Name] = true
		case parser.ValueWildcard:
			columns["*"] = true
			return
		case parser.ValueFunctionCall:
			getColumns(v.Elems, columns)
		}
	}
}

func filterColumns(columns map[string]struct{}, fields []string, point *protocol.Point) {
	if _, ok := columns["*"]; ok {
		return
	}

	newValues := []*protocol.FieldValue{}
	newFields := []string{}
	for idx, f := range fields {
		if _, ok := columns[f]; !ok {
			continue
		}

		newValues = append(newValues, point.Values[idx])
		newFields = append(newFields, f)
	}
	point.Values = newValues
}

func Filter(query *parser.SelectQuery, series *protocol.Series) (*protocol.Series, error) {
	if query.GetWhereCondition() == nil {
		return series, nil
	}

	columns := map[string]struct{}{}
	if query.GetFromClause().Type == parser.FromClauseInnerJoin {
	outer:
		for t, cs := range query.GetResultColumns() {
			for _, c := range cs {
				// if this is a wildcard select, then drop all columns and
				// just use '*'
				if c == "*" {
					columns = make(map[string]struct{}, 1)
					columns[c] = struct{}{}
					break outer
				}
				columns[t.Name+"."+c] = struct{}{}
			}
		}
	} else {
		for _, cs := range query.GetResultColumns() {
			for _, c := range cs {
				columns[c] = struct{}{}
			}
		}
	}

	points := series.Points
	series.Points = nil
	for _, point := range points {
		ok, err := matches(query.GetWhereCondition(), series.Fields, point)

		if err != nil {
			return nil, err
		}

		if ok {
			filterColumns(columns, series.Fields, point)
			series.Points = append(series.Points, point)
		}
	}

	if _, ok := columns["*"]; !ok {
		newFields := []string{}
		for _, f := range series.Fields {
			if _, ok := columns[f]; !ok {
				continue
			}

			newFields = append(newFields, f)
		}
		series.Fields = newFields
	}
	return series, nil
}
