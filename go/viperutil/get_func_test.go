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
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

type myStruct struct {
	Foo string
	Bar int
}

type myNestedStruct struct {
	MyStruct *myStruct
	Baz      bool
}

func TestGetFuncForType(t *testing.T) {
	now := time.Now()

	v := viper.New()
	v.Set("foo.bool", true)
	v.Set("foo.int", 5)
	v.Set("foo.duration", time.Second)
	v.Set("foo.float", 5.1)
	v.Set("foo.complex", fmt.Sprintf("%v", complex(1, 2)))
	v.Set("foo.intslice", []int{1, 2, 3})
	v.Set("foo.stringslice", []string{"a", "b", "c"})
	v.Set("foo.string", "hello")
	v.Set("foo.time", now)

	assert := assert.New(t)

	// Bool types
	assert.Equal(true, get[bool](t, v, "foo.bool"), "GetFuncForType[bool](foo.bool)")

	// Int types
	assert.Equal(5, get[int](t, v, "foo.int"), "GetFuncForType[int](foo.int)")
	assert.Equal(int8(5), get[int8](t, v, "foo.int"), "GetFuncForType[int8](foo.int)")
	assert.Equal(int16(5), get[int16](t, v, "foo.int"), "GetFuncForType[int16](foo.int)")
	assert.Equal(int32(5), get[int32](t, v, "foo.int"), "GetFuncForType[int32](foo.int)")
	assert.Equal(int64(5), get[int64](t, v, "foo.int"), "GetFuncForType[int64](foo.int)")

	// Duration types
	assert.Equal(time.Second, get[time.Duration](t, v, "foo.duration"), "GetFuncForType[time.Duration](foo.duration)")

	// Uint types
	assert.Equal(uint(5), get[uint](t, v, "foo.int"), "GetFuncForType[uint](foo.int)")
	assert.Equal(uint8(5), get[uint8](t, v, "foo.int"), "GetFuncForType[uint8](foo.int)")
	assert.Equal(uint16(5), get[uint16](t, v, "foo.int"), "GetFuncForType[uint16](foo.int)")
	assert.Equal(uint32(5), get[uint32](t, v, "foo.int"), "GetFuncForType[uint32](foo.int)")
	assert.Equal(uint64(5), get[uint64](t, v, "foo.int"), "GetFuncForType[uint64](foo.int)")

	// Float types
	assert.Equal(5.1, get[float64](t, v, "foo.float"), "GetFuncForType[float64](foo.float)")
	assert.Equal(float32(5.1), get[float32](t, v, "foo.float"), "GetFuncForType[float32](foo.float)")
	assert.Equal(float64(5), get[float64](t, v, "foo.int"), "GetFuncForType[float64](foo.int)")

	// Complex types
	assert.Equal(complex(5, 0), get[complex128](t, v, "foo.int"), "GetFuncForType[complex128](foo.int)")
	assert.Equal(complex(5.1, 0), get[complex128](t, v, "foo.float"), "GetFuncForType[complex128](foo.float)")
	assert.Equal(complex(1, 2), get[complex128](t, v, "foo.complex"), "GetFuncForType[complex128](foo.complex)")
	assert.Equal(complex64(complex(5, 0)), get[complex64](t, v, "foo.int"), "GetFuncForType[complex64](foo.int)")
	assert.Equal(complex64(complex(5.1, 0)), get[complex64](t, v, "foo.float"), "GetFuncForType[complex64](foo.float)")
	assert.Equal(complex64(complex(1, 2)), get[complex64](t, v, "foo.complex"), "GetFuncForType[complex64](foo.complex)")

	// Slice types
	assert.ElementsMatch([]int{1, 2, 3}, get[[]int](t, v, "foo.intslice"), "GetFuncForType[[]int](foo.intslice)")
	assert.ElementsMatch([]string{"a", "b", "c"}, get[[]string](t, v, "foo.stringslice"), "GetFuncForType[[]string](foo.stringslice)")

	// String types
	assert.Equal("hello", get[string](t, v, "foo.string"), "GetFuncForType[string](foo.string)")

	// Struct types
	assert.Equal(now, get[time.Time](t, v, "foo.time"), "GetFuncForType[time.Time](foo.time)")
	{
		s := &myStruct{
			Foo: "hello",
			Bar: 3,
		}
		v.Set("mystruct.foo", s.Foo)
		v.Set("mystruct.bar", s.Bar)

		assert.Equal(s, get[*myStruct](t, v, "mystruct"), "GetFuncForType[*myStruct](mystruct)")
		assert.IsType(&myStruct{}, get[*myStruct](t, v, "mystruct"), "GetFuncForType[*myStruct](mystruct) should return a pointer")
		assert.Equal(*s, get[myStruct](t, v, "mystruct"), "GetFuncForType[myStruct](mystruct)")
		assert.IsType(myStruct{}, get[myStruct](t, v, "mystruct"), "GetFuncForType[myStruct](mystruct) should return a struct (not pointer to struct)")

		s2 := &myNestedStruct{
			MyStruct: s,
			Baz:      true,
		}
		v.Set("mynestedstruct.mystruct.foo", s2.MyStruct.Foo)
		v.Set("mynestedstruct.mystruct.bar", s2.MyStruct.Bar)
		v.Set("mynestedstruct.baz", s2.Baz)
		assert.Equal(*s2, get[myNestedStruct](t, v, "mynestedstruct"), "GetFuncForType[myNestedStruct](mynestedstruct)")
		assert.IsType(myNestedStruct{}, get[myNestedStruct](t, v, "mynestedstruct"), "GetFuncForType[myNestedStruct](mynestedstruct) should return a struct (not pointer to struct)")
	}

	// Map types.
	v.Set("stringmap", map[string]string{
		"a": "A",
		"b": "B",
	})
	assert.Equal(map[string]string{"a": "A", "b": "B"}, get[map[string]string](t, v, "stringmap"), "GetFuncForType[map[string]string](stringmap)")

	v.Set("stringslicemap", map[string][]string{
		"uppers": strings.Split("ABCDEFG", ""),
		"lowers": strings.Split("abcdefg", ""),
	})
	assert.Equal(map[string][]string{"uppers": strings.Split("ABCDEFG", ""), "lowers": strings.Split("abcdefg", "")}, get[map[string][]string](t, v, "stringslicemap"), "GetFuncForType[map[string][]string](stringslicemap)")

	v.Set("anymap", map[string]any{
		"int":    5,
		"bool":   true,
		"string": "hello",
	})
	assert.Equal(map[string]any{"int": 5, "bool": true, "string": "hello"}, get[map[string]any](t, v, "anymap"), "GetFuncForType[map[string]any](anymap)")

	// Unsupported types.
	t.Run("uintptr", func(t *testing.T) {
		testPanic(t, GetFuncForType[uintptr], "GetFuncForType[uintptr]")
	})
	t.Run("arrays", func(t *testing.T) {
		testPanic(t, GetFuncForType[[5]int], "GetFuncForType[[5]int]")
		testPanic(t, GetFuncForType[[3]string], "GetFuncForType[[3]string]")
	})
	t.Run("channels", func(t *testing.T) {
		testPanic(t, GetFuncForType[chan struct{}], "GetFuncForType[chan struct{}]")
		testPanic(t, GetFuncForType[chan bool], "GetFuncForType[chan bool]")
		testPanic(t, GetFuncForType[chan int], "GetFuncForType[chan int]")
		testPanic(t, GetFuncForType[chan chan string], "GetFuncForType[chan chan string]")
	})
	t.Run("funcs", func(t *testing.T) {
		testPanic(t, GetFuncForType[func()], "GetFuncForType[func()]")
	})
}

func testPanic[T any](t testing.TB, f func() func(v *viper.Viper) func(key string) T, fnName string) {
	t.Helper()

	defer func() {
		err := recover()
		assert.NotNil(t, err, "%s should panic", fnName)
	}()

	fn := f()
	assert.Failf(t, fmt.Sprintf("%s should panic", fnName), "%s should panic; got %+v", fnName, fn)
}

func get[T any](t testing.TB, v *viper.Viper, key string) T {
	t.Helper()
	return GetFuncForType[T]()(v)(key)
}
