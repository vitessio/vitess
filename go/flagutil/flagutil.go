/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package flagutil contains flags that parse string lists and string
// maps.
package flagutil

import (
	"errors"
	"flag"
	"sort"
	"strings"
)

var (
	errInvalidKeyValuePair = errors.New("invalid key:value pair")
)

// StringListValue is a []string flag that accepts a comma separated
// list of elements. To include an element containing a comma, quote
// it with a backslash '\'.
type StringListValue []string

// Get returns the []string value of this flag.
func (value StringListValue) Get() interface{} {
	return []string(value)
}

func parseListWithEscapes(v string, delimiter rune) (value []string) {
	var escaped, lastWasDelimiter bool
	var current []rune

	for _, r := range v {
		lastWasDelimiter = false
		if !escaped {
			switch r {
			case delimiter:
				value = append(value, string(current))
				current = nil
				lastWasDelimiter = true
				continue
			case '\\':
				escaped = true
				continue
			}
		}
		escaped = false
		current = append(current, r)
	}
	if len(current) != 0 || lastWasDelimiter {
		value = append(value, string(current))
	}
	return value
}

// Set sets the value of this flag from parsing the given string.
func (value *StringListValue) Set(v string) error {
	*value = parseListWithEscapes(v, ',')
	return nil
}

// String returns the string representation of this flag.
func (value StringListValue) String() string {
	parts := make([]string, len(value))
	for i, v := range value {
		parts[i] = strings.Replace(strings.Replace(v, "\\", "\\\\", -1), ",", `\,`, -1)
	}
	return strings.Join(parts, ",")

}

// StringListVar defines a []string flag with the specified name, value and usage
// string. The argument 'p' points to a []string in which to store the value of the flag.
func StringListVar(p *[]string, name string, defaultValue []string, usage string) {
	*p = defaultValue
	flag.Var((*StringListValue)(p), name, usage)
}

// StringMapValue is a map[string]string flag. It accepts a
// comma-separated list of key value pairs, of the form key:value. The
// keys cannot contain colons.
type StringMapValue map[string]string

// Set sets the value of this flag from parsing the given string.
func (value *StringMapValue) Set(v string) error {
	dict := make(map[string]string)
	pairs := parseListWithEscapes(v, ',')
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return errInvalidKeyValuePair
		}
		dict[parts[0]] = parts[1]
	}
	*value = dict
	return nil
}

// Get returns the map[string]string value of this flag.
func (value StringMapValue) Get() interface{} {
	return map[string]string(value)
}

// String returns the string representation of this flag.
func (value StringMapValue) String() string {
	parts := make([]string, 0)
	for k, v := range value {
		parts = append(parts, k+":"+strings.Replace(v, ",", `\,`, -1))
	}
	// Generate the string deterministically.
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
