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
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetFuncForType(t *testing.T) {
	v := viper.New()
	v.Set("foo", true)
	v.Set("bar", 5)
	v.Set("baz", []int{1, 2, 3})

	getBool := GetFuncForType[bool]()
	assert.True(t, getBool(v)("foo"))

	assert.Equal(t, 5, GetFuncForType[int]()(v)("bar"))
}
