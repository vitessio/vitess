/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package inst

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	singleValueInterval = regexp.MustCompile("^([0-9]+)$")
	multiValueInterval  = regexp.MustCompile("^([0-9]+)[-]([0-9]+)$")
)

// OracleGtidSetEntry represents an entry in a set of GTID ranges,
// for example, the entry: "316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-8935:8984-6124596" (may include gaps)
type OracleGtidSetEntry struct {
	UUID   string
	Ranges string
}

// NewOracleGtidSetEntry parses a single entry text
func NewOracleGtidSetEntry(gtidRangeString string) (*OracleGtidSetEntry, error) {
	gtidRangeString = strings.TrimSpace(gtidRangeString)
	tokens := strings.SplitN(gtidRangeString, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("Cannot parse OracleGtidSetEntry from %s", gtidRangeString)
	}
	if tokens[0] == "" {
		return nil, fmt.Errorf("Unexpected UUID: %s", tokens[0])
	}
	if tokens[1] == "" {
		return nil, fmt.Errorf("Unexpected GTID range: %s", tokens[1])
	}
	gtidRange := &OracleGtidSetEntry{UUID: tokens[0], Ranges: tokens[1]}
	return gtidRange, nil
}

// String returns a user-friendly string representation of this entry
func (this *OracleGtidSetEntry) String() string {
	return fmt.Sprintf("%s:%s", this.UUID, this.Ranges)
}

// String returns a user-friendly string representation of this entry
func (this *OracleGtidSetEntry) Explode() (result [](*OracleGtidSetEntry)) {
	intervals := strings.Split(this.Ranges, ":")
	for _, interval := range intervals {
		if submatch := multiValueInterval.FindStringSubmatch(interval); submatch != nil {
			intervalStart, _ := strconv.Atoi(submatch[1])
			intervalEnd, _ := strconv.Atoi(submatch[2])
			for i := intervalStart; i <= intervalEnd; i++ {
				result = append(result, &OracleGtidSetEntry{UUID: this.UUID, Ranges: fmt.Sprintf("%d", i)})
			}
		} else if submatch := singleValueInterval.FindStringSubmatch(interval); submatch != nil {
			result = append(result, &OracleGtidSetEntry{UUID: this.UUID, Ranges: interval})
		}
	}
	return result
}
