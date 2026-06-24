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
	"golang.org/x/exp/maps"
)

func TestTabletResultMapSplit(t *testing.T) {
	tabletResultMap := TabletResultMap{
		"a": make(MetricResultMap),
		"b": make(MetricResultMap),
		"c": make(MetricResultMap),
	}
	{
		withAlias, all := tabletResultMap.Split("b")

		assert.Len(t, withAlias, 1)
		assert.Equal(t, []string{"b"}, maps.Keys(withAlias))
		assert.Len(t, all, 3)
		assert.ElementsMatch(t, maps.Keys(all), []string{"a", "b", "c"})
	}
	{
		withAlias, all := tabletResultMap.Split("x")

		assert.Empty(t, withAlias)
		assert.Len(t, all, 3)
		assert.ElementsMatch(t, maps.Keys(all), []string{"a", "b", "c"})
	}
}
