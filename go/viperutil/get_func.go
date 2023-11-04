/*
Copyright 2023 The Vitess Authors.

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

package viperutil

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

// GetFuncForType returns the default getter function for a given type T. A
// getter function is a function which takes a viper and returns a function that
// takes a key and (finally!) returns a value of type T.
//
// For example, the default getter for a value of type string is a function that
// takes a viper instance v and calls v.GetString with the provided key.
//
// In most cases, callers of Configure should be able to rely on the defaults
// provided here (and may refer to get_func_test.go for an up-to-date example
// of the provided functionalities), but if more fine-grained control is needed
// (this should be an **exceptional** circumstance), they may provide their own
// GetFunc as an option to Configure.
//
// This function may panic if called for an unsupported type. This is captured
// in the test code as well.
func GetFuncForType[T any]() func(v *viper.Viper) func(key string) T {
	var (
		t T
		f any
	)

	typ := reflect.TypeOf(t)
	switch typ.Kind() {
	case reflect.Bool:
		f = func(v *viper.Viper) func(key string) bool {
			return v.GetBool
		}
	case reflect.Int:
		f = func(v *viper.Viper) func(key string) int {
			return v.GetInt
		}
	case reflect.Int8:
		f = getCastedInt[int8]()
	case reflect.Int16:
		f = getCastedInt[int16]()
	case reflect.Int32:
		f = func(v *viper.Viper) func(key string) int32 {
			return v.GetInt32
		}
	case reflect.Int64:
		switch typ {
		case reflect.TypeOf(time.Duration(0)):
			f = func(v *viper.Viper) func(key string) time.Duration {
				return v.GetDuration
			}
		default:
			f = func(v *viper.Viper) func(key string) int64 {
				return v.GetInt64
			}
		}
	case reflect.Uint:
		f = func(v *viper.Viper) func(key string) uint {
			return v.GetUint
		}
	case reflect.Uint8:
		f = getCastedUint[uint8]()
	case reflect.Uint16:
		f = getCastedUint[uint16]()
	case reflect.Uint32:
		f = func(v *viper.Viper) func(key string) uint32 {
			return v.GetUint32
		}
	case reflect.Uint64:
		f = func(v *viper.Viper) func(key string) uint64 {
			return v.GetUint64
		}
	case reflect.Uintptr:
		// Unupported, fallthrough to `if f == nil` check below switch.
	case reflect.Float32:
		f = func(v *viper.Viper) func(key string) float32 {
			return func(key string) float32 {
				return float32(v.GetFloat64(key))
			}
		}
	case reflect.Float64:
		f = func(v *viper.Viper) func(key string) float64 {
			return v.GetFloat64
		}
	case reflect.Complex64:
		f = getComplex[complex64](64)
	case reflect.Complex128:
		f = getComplex[complex128](128)
	case reflect.Array:
		// Even though the code would be extremely similar to slice types, we
		// cannot support arrays because there's no way to write a function that
		// returns, say, [N]int, for some value of N which we only know at
		// runtime.
		panic("GetFuncForType does not support array types")
	case reflect.Chan:
		panic("GetFuncForType does not support channel types")
	case reflect.Func:
		panic("GetFuncForType does not support function types")
	case reflect.Interface:
		panic("GetFuncForType does not support interface types (specify a specific implementation type instead)")
	case reflect.Map:
		switch typ.Key().Kind() {
		case reflect.String:
			switch val := typ.Elem(); val.Kind() {
			case reflect.String:
				f = func(v *viper.Viper) func(key string) map[string]string {
					return v.GetStringMapString
				}
			case reflect.Slice:
				switch val.Elem().Kind() {
				case reflect.String:
					f = func(v *viper.Viper) func(key string) map[string][]string {
						return v.GetStringMapStringSlice
					}
				}
			case reflect.Interface:
				f = func(v *viper.Viper) func(key string) map[string]interface{} {
					return v.GetStringMap
				}
			}
		}
	case reflect.Pointer:
		switch typ.Elem().Kind() {
		case reflect.Struct:
			f = unmarshalFunc[T]()
		}
	case reflect.Slice:
		switch typ.Elem().Kind() {
		case reflect.Int:
			f = func(v *viper.Viper) func(key string) []int {
				return v.GetIntSlice
			}
		case reflect.String:
			f = func(v *viper.Viper) func(key string) []string {
				return v.GetStringSlice
			}
		}
	case reflect.String:
		f = func(v *viper.Viper) func(key string) string {
			return v.GetString
		}
	case reflect.Struct:
		switch typ {
		case reflect.TypeOf(time.Time{}):
			f = func(v *viper.Viper) func(key string) time.Time {
				return v.GetTime
			}
		default:
			f2 := unmarshalFunc[*T]()
			f = func(v *viper.Viper) func(key string) T {
				getPointer := f2(v)
				return func(key string) T {
					return *(getPointer(key))
				}
			}
		}
	}

	if f == nil {
		panic(fmt.Sprintf("no default GetFunc for type %T; call Configure with a custom GetFunc", t))
	}
	return f.(func(v *viper.Viper) func(key string) T)
}

func unmarshalFunc[T any]() func(v *viper.Viper) func(key string) T {
	return func(v *viper.Viper) func(key string) T {
		return func(key string) T {
			t := new(T)
			_ = v.UnmarshalKey(key, t) // TODO: panic on this error
			return *t
		}
	}
}

func getCastedInt[T int8 | int16]() func(v *viper.Viper) func(key string) T {
	return func(v *viper.Viper) func(key string) T {
		return func(key string) T {
			return T(v.GetInt(key))
		}
	}
}

func getCastedUint[T uint8 | uint16]() func(v *viper.Viper) func(key string) T {
	return func(v *viper.Viper) func(key string) T {
		return func(key string) T {
			return T(v.GetUint(key))
		}
	}
}

func getComplex[T complex64 | complex128](bitSize int) func(v *viper.Viper) func(key string) T {
	return func(v *viper.Viper) func(key string) T {
		return func(key string) T {
			x, err := strconv.ParseComplex(v.GetString(key), bitSize)
			if err != nil {
				panic(err) // TODO: wrap with more details (key, type (64 vs 128), etc)
			}

			return T(x)
		}
	}
}
