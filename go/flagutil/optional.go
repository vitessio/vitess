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

// OptionalFlag augements the pflag.Value interface with a method to determine
// if a flag was set explicitly on the comand-line.
//
// Though not part of the interface, because the return type would be different
// for each implementation, by convention, each implementation should define a
// Get() method to access the underlying value.
type OptionalFlag interface {
	pflag.Value
	IsSet() bool
}

var (
	_ OptionalFlag = (*OptionalFloat64)(nil)
	_ OptionalFlag = (*OptionalString)(nil)
)

// OptionalFloat64 implements OptionalFlag for float64 values.
type OptionalFloat64 struct {
	val float64
	set bool
}

// NewOptionalFloat64 returns an OptionalFloat64 with the specified value as its
// starting value.
func NewOptionalFloat64(val float64) *OptionalFloat64 {
	return &OptionalFloat64{
		val: val,
		set: false,
	}
}

// Set is part of the pflag.Value interface.
func (f *OptionalFloat64) Set(arg string) error {
	v, err := strconv.ParseFloat(arg, 64)
	if err != nil {
		return numError(err)
	}

	f.val = v
	f.set = true

	return nil
}

// String is part of the pflag.Value interface.
func (f *OptionalFloat64) String() string {
	return strconv.FormatFloat(f.val, 'g', -1, 64)
}

// Type is part of the pflag.Value interface.
func (f *OptionalFloat64) Type() string {
	return "float64"
}

// Get returns the underlying float64 value of this flag. If the flag was not
// explicitly set, this will be the initial value passed to the constructor.
func (f *OptionalFloat64) Get() float64 {
	return f.val
}

// IsSet is part of the OptionalFlag interface.
func (f *OptionalFloat64) IsSet() bool {
	return f.set
}

// OptionalString implements OptionalFlag for string values.
type OptionalString struct {
	val string
	set bool
}

// NewOptionalString returns an OptionalString with the specified value as its
// starting value.
func NewOptionalString(val string) *OptionalString {
	return &OptionalString{
		val: val,
		set: false,
	}
}

// Set is part of the pflag.Value interface.
func (f *OptionalString) Set(arg string) error {
	f.val = arg
	f.set = true
	return nil
}

// String is part of the pflag.Value interface.
func (f *OptionalString) String() string {
	return f.val
}

// Type is part of the pflag.Value interface.
func (f *OptionalString) Type() string {
	return "string"
}

// Get returns the underlying string value of this flag. If the flag was not
// explicitly set, this will be the initial value passed to the constructor.
func (f *OptionalString) Get() string {
	return f.val
}

// IsSet is part of the OptionalFlag interface.
func (f *OptionalString) IsSet() bool {
	return f.set
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
