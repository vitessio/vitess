/*
Copyright 2020 The Vitess Authors.

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

package textutil

import (
	"regexp"
	"strings"
)

var (
	delimitedListRegexp = regexp.MustCompile(`[ ,;]+`)
)

// SplitDelimitedList splits a given string by comma, semi-colon or space, and returns non-empty strings
func SplitDelimitedList(s string) (list []string) {
	tokens := delimitedListRegexp.Split(s, -1)
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		list = append(list, token)
	}
	return list
}
