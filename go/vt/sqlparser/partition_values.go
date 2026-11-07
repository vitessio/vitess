/*
Copyright 2026 The Vitess Authors.

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

package sqlparser

// The VALUES IN of a partition takes one list of values, as in
// VALUES IN (1, 2), or a list of tuples, as in VALUES IN ((1, 2), (3, 4)).
// MySQL tells them apart by the token after the first '(': another '(' starts
// a list of tuples, so VALUES IN ((1) + 1, 2) is a syntax error. The parser
// reads the values as expressions, in which a value in parentheses, as in
// (4), is the value itself, so it checks the form against the text and
// rejects values that MySQL rejects as a syntax error, or that the formatter
// would write in the other form.
//
// MySQL also checks the values against the number of columns that the
// partitioning takes, where a statement says it, and reports fewer values
// than columns as a syntax error. It reports more values than columns as a
// different error, which it is left to report.

// Messages of the errors that MySQL reports for partition values.
const (
	partitionColumnListError = "Inconsistency in usage of column lists for partitioning"
	rowSinglePartitionError  = "Row expressions in VALUES IN only allowed for multi-field column partitioning"
	partitionRowValueError   = "a row in partition values is not supported"
)

// addPartitionValue records where a value of a partition starts and ends in
// the text, for checkPartitionValues.
func (tkn *Tokenizer) addPartitionValue(start, end int) {
	tkn.partitionValues = append(tkn.partitionValues, [2]int{start, end})
}

// takePartitionValues returns where the last count values of a partition
// start and end in the text, and forgets them.
func (tkn *Tokenizer) takePartitionValues(count int) [][2]int {
	if count > len(tkn.partitionValues) {
		// Every value records where it is, so this cannot happen.
		return nil
	}
	spans := tkn.partitionValues[len(tkn.partitionValues)-count:]
	tkn.partitionValues = tkn.partitionValues[:len(tkn.partitionValues)-count]
	return spans
}

// checkPartitionValues checks the values of VALUES IN, or with in unset,
// of VALUES LESS THAN, against what MySQL takes whatever the partitioning,
// and returns an error message for values that it rejects or that the
// formatter cannot write as they are written.
func (tkn *Tokenizer) checkPartitionValues(values ValTuple, in bool) string {
	spans := tkn.takePartitionValues(len(values))
	if spans == nil {
		return partitionColumnListError
	}
	for i, value := range values {
		if _, ok := value.(ValTuple); ok && tkn.buf[spans[i][0]] != '(' {
			// The formatter writes ROW(1, 2) as (1, 2), which MySQL would
			// read as a tuple of the partition.
			return partitionRowValueError
		}
	}
	if !in || tkn.buf[spans[0][0]] != '(' {
		// One list of values. Its first value cannot start with '(' in
		// VALUES IN, so it is not a tuple.
		if _, ok := values[0].(ValTuple); ok && in {
			return partitionRowValueError
		}
		return ""
	}
	// A list of tuples, each of at least two values, as many in each.
	length := 0
	for i, value := range values {
		tuple, ok := value.(ValTuple)
		if !ok || len(tuple) < 2 {
			return rowSinglePartitionError
		}
		if isParenthesizedTwice(tkn.buf[spans[i][0]:spans[i][1]]) {
			// MySQL reads ((1, 2)) as a tuple of one value, which is a
			// row, but the formatter writes it as (1, 2).
			return partitionRowValueError
		}
		if length != 0 && len(tuple) != length {
			return partitionColumnListError
		}
		length = len(tuple)
	}
	return ""
}

// isParenthesizedTwice reports whether text, a value as written, is in two
// pairs of parentheses, as in ((1, 2)).
func isParenthesizedTwice(text string) bool {
	tkn := &Tokenizer{buf: text}
	var tokens []int
	for {
		typ, _ := tkn.Scan()
		if typ == 0 || typ == LEX_ERROR {
			break
		}
		tokens = append(tokens, typ)
	}
	last := len(tokens) - 1
	if last < 3 || tokens[0] != '(' || tokens[1] != '(' || tokens[last] != ')' || tokens[last-1] != ')' {
		return false
	}
	depth := 0
	for i := 1; i < last; i++ {
		switch tokens[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i == last-1
			}
		}
	}
	return false
}

func hasRow(values ValTuple) bool {
	for _, value := range values {
		if _, ok := value.(ValTuple); ok {
			return true
		}
	}
	return false
}

// checkPartitionDefinitions checks the values of each partition against the
// number of columns that the partitioning takes, and returns an error message
// for values that MySQL rejects as a syntax error.
func checkPartitionDefinitions(option *PartitionOption) string {
	columns := 1
	if len(option.ColList) > 0 {
		columns = len(option.ColList)
	}
	for _, definition := range option.Definitions {
		if definition.Options == nil || definition.Options.ValueRange == nil {
			continue
		}
		values := definition.Options.ValueRange
		switch values.Type {
		case InType:
			_, tuples := values.Range[0].(ValTuple)
			switch {
			case !tuples && columns != 1:
				return partitionColumnListError
			case tuples && columns == 1:
				return rowSinglePartitionError
			case tuples && len(values.Range[0].(ValTuple)) < columns:
				return partitionColumnListError
			}
		case LessThanType:
			if hasRow(values.Range) {
				// MySQL rejects a row, as in ((10, 20)), before it counts
				// the values, and not as a syntax error.
				continue
			}
			if values.Maxvalue && columns > 1 || !values.Maxvalue && len(values.Range) < columns {
				return partitionColumnListError
			}
		default:
			return "unknown partition value range type"
		}
	}
	return ""
}
