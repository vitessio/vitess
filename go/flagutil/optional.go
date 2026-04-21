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
	"strconv"

	"github.com/spf13/pflag"
)

// OptionalFlag augments the pflag.Value interface with a method to determine
// if a flag was set explicitly on the command-line, and a method to retrieve
// the underlying value.
type OptionalFlag[T any] struct {
	val      T
	set      bool
	parse    func(string) (T, error)
	format   func(T) string
	typeName string
}

var (
	_ pflag.Value = (*OptionalFlag[float64])(nil)
	_ pflag.Value = (*OptionalFlag[string])(nil)
)

// NewOptionalFlag returns a generic OptionalFlag with the specified initial
// value, a parsing function for Set, a formatting function for String, and a
// type name for Type.
func NewOptionalFlag[T any](val T, parse func(string) (T, error), format func(T) string, typeName string) *OptionalFlag[T] {
	return &OptionalFlag[T]{
		val:      val,
		set:      false,
		parse:    parse,
		format:   format,
		typeName: typeName,
	}
}

// Set is part of the pflag.Value interface.
func (f *OptionalFlag[T]) Set(arg string) error {
	v, err := f.parse(arg)
	if err != nil {
		return err
	}

	f.val = v
	f.set = true

	return nil
}

// String is part of the pflag.Value interface.
func (f *OptionalFlag[T]) String() string {
	return f.format(f.val)
}

// Type is part of the pflag.Value interface.
func (f *OptionalFlag[T]) Type() string {
	return f.typeName
}

// Get returns the underlying value of this flag. If the flag was not
// explicitly set, this will be the initial value passed to the constructor.
func (f *OptionalFlag[T]) Get() T {
	return f.val
}

// IsSet returns true if the flag was explicitly set on the command-line.
func (f *OptionalFlag[T]) IsSet() bool {
	return f.set
}

// NewOptionalFloat64 returns an OptionalFlag[float64] with the specified value
// as its starting value.
func NewOptionalFloat64(val float64) *OptionalFlag[float64] {
	return NewOptionalFlag(
		val,
		func(arg string) (float64, error) {
			v, err := strconv.ParseFloat(arg, 64)
			if err != nil {
				return 0, numError(err)
			}
			return v, nil
		},
		func(v float64) string {
			return strconv.FormatFloat(v, 'g', -1, 64)
		},
		"float64",
	)
}

// NewOptionalString returns an OptionalFlag[string] with the specified value
// as its starting value.
func NewOptionalString(val string) *OptionalFlag[string] {
	return NewOptionalFlag(
		val,
		func(arg string) (string, error) {
			return arg, nil
		},
		func(v string) string {
			return v
		},
		"string",
	)
}

// NewOptionalInt64 returns an OptionalFlag[int64] with the specified value
// as its starting value.
func NewOptionalInt64(val int64) *OptionalFlag[int64] {
	return NewOptionalFlag(
		val,
		func(arg string) (int64, error) {
			v, err := strconv.ParseInt(arg, 10, 64)
			if err != nil {
				return 0, numError(err)
			}
			return v, nil
		},
		func(v int64) string {
			return strconv.FormatInt(v, 10)
		},
		"int64",
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
