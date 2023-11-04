/*
Copyright 2023 The Vitess Authors.

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

package flagutil

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// StringEnum provides a string-like flag value that raises an error if given a
// value not in the set of allowed choices.
//
// This parse-time validation can be case-sensitive or not, depending on which
// constructor (NewStringEnum vs NewCaseInsensitiveStringEnum) was used.
type StringEnum struct {
	name string
	val  string

	caseInsensitive bool
	choices         map[string]struct{}
	choiceNames     []string
	choiceMapper    func(string) string
}

// ErrInvalidChoice is returned when parsing a value that is not a valid choice
// for the StringEnum flag.
var ErrInvalidChoice = errors.New("invalid choice for enum")

// NewStringEnum returns a new string enum flag with the given name, default,
// and choices.
//
// Parse-time validation is case-sensitive.
func NewStringEnum(name string, initialValue string, choices []string) *StringEnum {
	return newStringEnum(name, initialValue, choices, false)
}

// NewCaseInsensitiveStringEnum returns a new string enum flag with the given
// name, default, and choices.
//
// Parse-time validation is case-insensitive.
func NewCaseInsensitiveStringEnum(name string, initialValue string, choices []string) *StringEnum {
	return newStringEnum(name, initialValue, choices, true)
}

func newStringEnum(name string, initialValue string, choices []string, caseInsensitive bool) *StringEnum {
	choiceMapper := func(s string) string { return s }
	choiceMap := map[string]struct{}{}

	if caseInsensitive {
		choiceMapper = strings.ToLower
	}

	for _, choice := range choices {
		choiceMap[choiceMapper(choice)] = struct{}{}
	}

	choiceNames := make([]string, 0, len(choiceMap))
	for choice := range choiceMap {
		choiceNames = append(choiceNames, choice)
	}
	sort.Strings(choiceNames)

	if initialValue != "" {
		if _, ok := choiceMap[choiceMapper(initialValue)]; !ok {
			// This will panic if we've misconfigured something in the source code.
			// It's not a user-error, so it had damn-well be better caught by a test
			// somewhere.
			panic(fmt.Errorf("%w (valid choices: %v)", ErrInvalidChoice, choiceNames))
		}
	}

	return &StringEnum{
		name:         name,
		val:          initialValue,
		choices:      choiceMap,
		choiceNames:  choiceNames,
		choiceMapper: choiceMapper,
	}
}

// Set is part of the pflag.Value interface.
func (s *StringEnum) Set(arg string) error {
	if _, ok := s.choices[s.choiceMapper(arg)]; !ok {
		msg := "%w (valid choices: %v"
		if s.caseInsensitive {
			msg += " [case insensitive]"
		}
		msg += ")"
		return fmt.Errorf(msg, ErrInvalidChoice, s.choiceNames)
	}

	s.val = arg

	return nil
}

// String is part of the pflag.Value interface.
func (s *StringEnum) String() string { return s.val }

// Type is part of the pflag.Value interface.
func (s *StringEnum) Type() string { return "string" }
