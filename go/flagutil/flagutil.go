/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
)

var (
	errInvalidKeyValuePair = errors.New("invalid key:value pair")
)

// StringListValue is a []string flag that accepts a comma separated
// list of elements. To include an element containing a comma, quote
// it with a backslash '\'.
type StringListValue []string

// Get returns the []string value of this flag.
func (value StringListValue) Get() any {
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

func (value StringListValue) Type() string { return "strings" }

// StringListVar defines a []string flag with the specified name, value and usage
// string. The argument 'p' points to a []string in which to store the value of the flag.
func StringListVar(fs *pflag.FlagSet, p *[]string, name string, defaultValue []string, usage string) {
	*p = defaultValue
	fs.Var((*StringListValue)(p), name, usage)
}

// StringMapValue is a map[string]string flag. It accepts a
// comma-separated list of key value pairs, of the form key:value. The
// keys cannot contain colons.
//
// TODO (andrew): Look into whether there's a native pflag Flag type that we can
// use/transition to instead.
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
func (value StringMapValue) Get() any {
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

// Type is part of the pflag.Value interface.
func (value StringMapValue) Type() string { return "StringMap" }

// DualFormatStringListVar creates a flag which supports both dashes and underscores
func DualFormatStringListVar(fs *pflag.FlagSet, p *[]string, name string, value []string, usage string) {
	dashes := strings.Replace(name, "_", "-", -1)
	underscores := strings.Replace(name, "-", "_", -1)

	StringListVar(fs, p, underscores, value, usage)
	if dashes != underscores {
		StringListVar(fs, p, dashes, *p, fmt.Sprintf("Synonym to -%s", underscores))
	}
}

// DualFormatStringVar creates a flag which supports both dashes and underscores
func DualFormatStringVar(fs *pflag.FlagSet, p *string, name string, value string, usage string) {
	dashes := strings.Replace(name, "_", "-", -1)
	underscores := strings.Replace(name, "-", "_", -1)

	fs.StringVar(p, underscores, value, usage)
	if dashes != underscores {
		fs.StringVar(p, dashes, *p, fmt.Sprintf("Synonym to -%s", underscores))
	}
}

// DualFormatInt64Var creates a flag which supports both dashes and underscores
func DualFormatInt64Var(fs *pflag.FlagSet, p *int64, name string, value int64, usage string) {
	dashes := strings.Replace(name, "_", "-", -1)
	underscores := strings.Replace(name, "-", "_", -1)

	fs.Int64Var(p, underscores, value, usage)
	if dashes != underscores {
		fs.Int64Var(p, dashes, *p, fmt.Sprintf("Synonym to -%s", underscores))
	}
}

// DualFormatIntVar creates a flag which supports both dashes and underscores
func DualFormatIntVar(fs *pflag.FlagSet, p *int, name string, value int, usage string) {
	dashes := strings.Replace(name, "_", "-", -1)
	underscores := strings.Replace(name, "-", "_", -1)

	fs.IntVar(p, underscores, value, usage)
	if dashes != underscores {
		fs.IntVar(p, dashes, *p, fmt.Sprintf("Synonym to -%s", underscores))
	}
}

// DualFormatBoolVar creates a flag which supports both dashes and underscores
func DualFormatBoolVar(fs *pflag.FlagSet, p *bool, name string, value bool, usage string) {
	dashes := strings.Replace(name, "_", "-", -1)
	underscores := strings.Replace(name, "-", "_", -1)

	fs.BoolVar(p, underscores, value, usage)
	if dashes != underscores {
		fs.BoolVar(p, dashes, *p, fmt.Sprintf("Synonym to -%s", underscores))
	}
}

// DurationOrIntVar implements pflag.Value for flags that have historically been
// of type IntVar (and then converted to seconds or some other unit) but are
// now transitioning to a proper DurationVar type.
//
// When parsing a command-line argument, it will first attempt to parse the
// argument using time.ParseDuration; if this fails, it will fallback to
// strconv.ParseInt and multiply that value by the `fallback` unit value to get
// a duration. If the initial ParseDuration fails, it will also log a
// deprecation warning.
type DurationOrIntVar struct {
	name     string
	val      time.Duration
	fallback time.Duration
}

// NewDurationOrIntVar returns a new DurationOrIntVar struct with the given name,
// default value, and fallback unit.
//
// The name is used only when issuing a deprecation warning (so the user knows
// which flag needs its argument format updated).
//
// The `fallback` argument is used when parsing an argument as an int (legacy behavior) as the multiplier
// to get a time.Duration value. As an example, if a flag used to be "the amount
// of time to wait in seconds" with a default of 60, you would do:
//
//	myFlag := flagutil.NewDurationOrIntVar("my-flag", time.Minute /* 60 second default */, time.Second /* fallback unit to multiply by */)
func NewDurationOrIntVar(name string, val time.Duration, fallback time.Duration) *DurationOrIntVar {
	return &DurationOrIntVar{name: name, val: val, fallback: fallback}
}

// Set is part of the pflag.Value interface.
func (v *DurationOrIntVar) Set(s string) error {
	d, derr := time.ParseDuration(s)
	if derr != nil {
		msg := &strings.Builder{}
		fmt.Fprintf(msg, "non-duration value passed to %s (error: %s)", v.name, derr)

		i, ierr := strconv.ParseInt(s, 10, 64)
		if ierr != nil {
			log.Warningf("%s; attempted to parse as int in %s, which failed with %s", msg.String(), v.fallback, ierr)
			return ierr
		}

		d = time.Duration(i) * v.fallback
		log.Warningf("%s; parsed as int to %s, which is deprecated behavior", d)
	}

	v.val = d
	return nil
}

// String is part of the pflag.Value interface.
func (v *DurationOrIntVar) String() string { return v.val.String() }

// Type is part of the pflag.Type interface.
func (v *DurationOrIntVar) Type() string { return "duration" }

// Value returns the underlying Duration value passed to the flag.
func (v *DurationOrIntVar) Value() time.Duration { return v.val }
