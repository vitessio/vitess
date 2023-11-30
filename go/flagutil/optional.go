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
	"reflect"
	"strconv"

	"github.com/spf13/pflag"
)

// OptionalFlag augements the pflag.Value interface with a method to determine
// if a flag was set explicitly on the comand-line.
//
// Though not part of the interface, because the return type would be different
// for each implementation, by convention, each implementation should define a
// Get() method to access the underlying value.
//
// TODO (ajm188) - replace this interface with a generic type.
// c.f. https://github.com/vitessio/vitess/issues/11154.
type OptionalFlag interface {
	pflag.Value
	IsSet() bool
}

type Optional[T any] struct {
	val T
	set bool
}

func (Optional[T]) NewOptionalType(val T) *Optional[T] {
	return &Optional[T]{
		val: val,
		set: false,
	}
}

// Set is part of the pflag.Value interface.
func (t *Optional[T]) Set(arg string) error {
	var val any
	switch kind := reflect.TypeOf(t.val).Kind(); kind {
	case reflect.Float64:
		num, err := strconv.ParseFloat(arg, 64)
		if err != nil {
			return numError(err)
		}
		val = num
	case reflect.String:
		val = arg
	default:
		panic(fmt.Sprintf("%v not supported", kind))
	}
	t.val = val.(T)
	t.set = true
	return nil
}

// String is part of the pflag.Value interface.
func (t *Optional[T]) String() string {
	return fmt.Sprintf("%v", t.val)
}

// Type is part of the pflag.Value interface.
func (t *Optional[T]) Type() string {
	return fmt.Sprintf("%T", t.val)
}

// Get returns the underlying float64 value of this flag. If the flag was not
// explicitly set, this will be the initial value passed to the constructor.
func (t *Optional[T]) Get() any {
	return t.val
}

// IsSet is part of the OptionalFlag interface.
func (t *Optional[T]) IsSet() bool {
	return t.set
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
