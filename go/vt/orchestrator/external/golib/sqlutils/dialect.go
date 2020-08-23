/*
   Copyright 2017 GitHub Inc.

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

package sqlutils

import (
	"regexp"
	"strings"
)

type regexpMap struct {
	r           *regexp.Regexp
	replacement string
}

func (this *regexpMap) process(text string) (result string) {
	return this.r.ReplaceAllString(text, this.replacement)
}

func rmap(regexpExpression string, replacement string) regexpMap {
	return regexpMap{
		r:           regexp.MustCompile(regexpSpaces(regexpExpression)),
		replacement: replacement,
	}
}

func regexpSpaces(statement string) string {
	return strings.Replace(statement, " ", `[\s]+`, -1)
}

func applyConversions(statement string, conversions []regexpMap) string {
	for _, rmap := range conversions {
		statement = rmap.process(statement)
	}
	return statement
}
