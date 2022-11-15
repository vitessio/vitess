/*
Copyright 2022 The Vitess Authors.

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
	"time"

	"github.com/spf13/viper"
)

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
	case reflect.Complex128:
	case reflect.Array:
	case reflect.Chan:
		panic("GetFuncForType does not support channel types")
	case reflect.Func:
		panic("GetFuncForType does not support function types")
	case reflect.Interface:
		// TODO: unwrap to see if struct-ish
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
