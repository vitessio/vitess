/*
   Copyright 2015 Shlomi Noach.

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

package util

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	TabulateLeft  = 0
	TabulateRight = 1
)

// ParseSimpleTime parses input in the format 7s, 55m, 3h, 31d, 4w (second, minute, hour, day, week)
// The time.ParseDuration() function should have done this, but it does not support "d" and "w" extensions.
func SimpleTimeToSeconds(simpleTime string) (int, error) {
	if matched, _ := regexp.MatchString("^[0-9]+s$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+m$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+h$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60 * 60, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+d$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60 * 60 * 24, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+w$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60 * 60 * 24 * 7, nil
	}
	return 0, fmt.Errorf("Cannot parse simple time: %s", simpleTime)
}

func Tabulate(lines []string, separator string, outputSeparator string, directionFlags ...int) (result []string) {
	tokens := make([][]string, 0)
	countColumns := 0
	for _, line := range lines {
		lineTokens := strings.Split(line, separator)
		tokens = append(tokens, lineTokens)
		if len(lineTokens) > countColumns {
			countColumns = len(lineTokens)
		}
	}
	columnWidths := make([]int, countColumns)
	for _, lineTokens := range tokens {
		for col, token := range lineTokens {
			if len(token) > columnWidths[col] {
				columnWidths[col] = len(token)
			}
		}
	}
	for _, lineTokens := range tokens {
		resultRow := ""
		for col := 0; col < countColumns; col++ {
			token := ""
			if col < len(lineTokens) {
				token = lineTokens[col]
			}
			format := fmt.Sprintf("%%-%ds", columnWidths[col]) // format left
			if col < len(directionFlags) && directionFlags[col] == TabulateRight {
				format = fmt.Sprintf("%%%ds", columnWidths[col])
			}
			formattedToken := fmt.Sprintf(format, token)
			if col == 0 {
				resultRow = formattedToken
			} else {
				resultRow = fmt.Sprintf("%s%s%s", resultRow, outputSeparator, formattedToken)
			}
		}
		result = append(result, resultRow)
	}
	return result
}
