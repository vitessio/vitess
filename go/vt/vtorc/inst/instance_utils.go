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
	"regexp"
	"strings"
)

// MajorVersion returns a MySQL major version number (e.g. given "5.5.36" it returns "5.5")
func MajorVersion(version string) []string {
	tokens := strings.Split(version, ".")
	if len(tokens) < 2 {
		return []string{"0", "0"}
	}
	return tokens[:2]
}

// RegexpMatchPatterns returns true if s matches any of the provided regexpPatterns
func RegexpMatchPatterns(s string, regexpPatterns []string) bool {
	for _, filter := range regexpPatterns {
		if matched, err := regexp.MatchString(filter, s); err == nil && matched {
			return true
		}
	}
	return false
}
