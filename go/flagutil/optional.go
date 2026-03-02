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

// Compile-time assertion that OptionalFlag implements pflag.Value.
var _ pflag.Value = (*OptionalFlag[string])(nil)

// OptionalFlag is a generic flag type that wraps any value T and tracks
// whether the flag was explicitly set on the command line.
//
// It implements pflag.Value via Set, String, and Type methods.
// Use Get to retrieve the underlying value, and IsSet to check
// whether the flag was provided by the caller.
type OptionalFlag[T any] struct {
	val      T
	set      bool
	parse    func(string) (T, error)
	typeName string
	stringer func(T) string
}

// NewOptionalFlag constructs an OptionalFlag[T] with the given default value,
// type name, parse function, and stringer function.
//
// parse must convert a raw string argument to T, returning an error on failure.
// stringer must convert a T back to its string representation.
func NewOptionalFlag[T any](val T, typeName string, parse func(string) (T, error), stringer func(T) string) *OptionalFlag[T] {
	if parse == nil {
		panic("flagutil: NewOptionalFlag requires a non-nil parse function")
	}
	if stringer == nil {
		stringer = func(v T) string { return fmt.Sprint(v) }
	}
	return &OptionalFlag[T]{
		val:      val,
		typeName: typeName,
		parse:    parse,
		stringer: stringer,
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
	return f.stringer(f.val)
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

// IsSet returns true if the flag was explicitly set on the command line.
func (f *OptionalFlag[T]) IsSet() bool {
	return f.set
}

// NewOptionalFloat64 returns an OptionalFlag for float64 values with the
// given default. It is a convenience constructor over NewOptionalFlag.
func NewOptionalFloat64(val float64) *OptionalFlag[float64] {
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

// NewOptionalString returns an OptionalFlag for string values with the
// given default. It is a convenience constructor over NewOptionalFlag.
func NewOptionalString(val string) *OptionalFlag[string] {
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
