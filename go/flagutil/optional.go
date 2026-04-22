/*
Copyright 2021 The Vitess Authors.

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

package flagutil

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/spf13/pflag"
)

// OptionalFlag augments the pflag.Value interface with a method to determine
// if a flag was set explicitly on the command-line.
//
// Though not part of the interface, because the return type would be different
// for each implementation, by convention, each implementation should define a
// Get() method to access the underlying value.
type OptionalFlag interface {
	pflag.Value
	IsSet() bool
}

// OptionalFlagValue is a generic implementation of OptionalFlag for any type T.
// Always construct it via NewOptionalFlag, NewOptionalFloat64, or NewOptionalString.
// A zero-value OptionalFlagValue is safe: String() falls back to fmt.Sprintf and
// Set() returns a descriptive error rather than panicking.
type OptionalFlagValue[T any] struct {
	val      T
	set      bool
	typeName string
	parse    func(string) (T, error)
	stringer func(T) string
}

// OptionalFloat64 is preserved for backward compatibility.
type OptionalFloat64 = OptionalFlagValue[float64]

// OptionalString is preserved for backward compatibility.
type OptionalString = OptionalFlagValue[string]

var (
	_ OptionalFlag = (*OptionalFloat64)(nil)
	_ OptionalFlag = (*OptionalString)(nil)
)

// NewOptionalFlag returns a *OptionalFlagValue[T] with the given default value,
// pflag type name, parse function, and stringer.
// It panics if parse is nil, since a flag without a parse function cannot be set.
func NewOptionalFlag[T any](defaultVal T, typeName string, parse func(string) (T, error), stringer func(T) string) *OptionalFlagValue[T] {
	if parse == nil {
		panic("flagutil: NewOptionalFlag requires a non-nil parse function")
	}
	return &OptionalFlagValue[T]{
		val:      defaultVal,
		typeName: typeName,
		parse:    parse,
		stringer: stringer,
	}
}

func (f *OptionalFlagValue[T]) Set(arg string) error {
	if f.parse == nil {
		return errors.New("flagutil: OptionalFlagValue has no parse function; use a constructor such as NewOptionalFlag")
	}

	v, err := f.parse(arg)
	if err != nil {
		return err
	}

	f.val = v
	f.set = true

	return nil
}

func (f *OptionalFlagValue[T]) String() string {
	if f.stringer == nil {
		return fmt.Sprintf("%v", f.val)
	}

	return f.stringer(f.val)
}

func (f *OptionalFlagValue[T]) Type() string {
	return f.typeName
}

// Get returns the underlying value of this flag. If the flag was not
// explicitly set, this returns the initial value passed to the constructor.
func (f *OptionalFlagValue[T]) Get() T {
	return f.val
}

func (f *OptionalFlagValue[T]) IsSet() bool {
	return f.set
}

// NewOptionalFloat64 returns an *OptionalFloat64 with the specified value as its
// starting value.
func NewOptionalFloat64(val float64) *OptionalFloat64 {
	return NewOptionalFlag(
		val,
		"float64",
		func(s string) (float64, error) {
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return 0, numError(err)
			}
			return v, nil
		},
		func(v float64) string {
			return strconv.FormatFloat(v, 'g', -1, 64)
		},
	)
}

// NewOptionalString returns an *OptionalString with the specified value as its
// starting value.
func NewOptionalString(val string) *OptionalString {
	return NewOptionalFlag(
		val,
		"string",
		func(s string) (string, error) { return s, nil },
		func(v string) string { return v },
	)
}

// lifted directly from package flag to make the behavior of numeric parsing
// consistent with the standard library for our custom optional types.
var (
	errParse = errors.New("parse error")
	errRange = errors.New("value out of range")
)

// lifted directly from package flag to make the behavior of numeric parsing
// consistent with the standard library for our custom optional types.
func numError(err error) error {
	ne, ok := err.(*strconv.NumError)
	if !ok {
		return err
	}

	switch ne.Err {
	case strconv.ErrSyntax:
		return errParse
	case strconv.ErrRange:
		return errRange
	default:
		return err
	}
}
