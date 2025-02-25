/*
Copyright 2024 The Vitess Authors.

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

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScopePascalCase(t *testing.T) {
	expectCases := map[Scope]string{
		UndefinedScope: "",
		ShardScope:     "Shard",
		SelfScope:      "Self",
	}
	for scope, expect := range expectCases {
		t.Run(scope.String(), func(t *testing.T) {
			pascal := scope.Pascal()
			assert.Equal(t, expect, pascal)
		})
	}
}
