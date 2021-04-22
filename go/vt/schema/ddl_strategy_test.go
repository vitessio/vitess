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

package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsDirect(t *testing.T) {
	assert.True(t, DDLStrategyDirect.IsDirect())
	assert.False(t, DDLStrategyOnline.IsDirect())
	assert.False(t, DDLStrategyGhost.IsDirect())
	assert.False(t, DDLStrategyPTOSC.IsDirect())
	assert.True(t, DDLStrategy("").IsDirect())
	assert.False(t, DDLStrategy("gh-ost").IsDirect())
	assert.False(t, DDLStrategy("pt-osc").IsDirect())
	assert.True(t, DDLStrategy("something").IsDirect())
}

func TestParseDDLStrategy(t *testing.T) {
	tt := []struct {
		strategyVariable string
		strategy         DDLStrategy
		options          string
		isDeclarative    bool
		isSingleton      bool
		runtimeOptions   string
		err              error
	}{
		{
			strategyVariable: "direct",
			strategy:         DDLStrategyDirect,
		},
		{
			strategyVariable: "online",
			strategy:         DDLStrategyOnline,
		},
		{
			strategyVariable: "gh-ost",
			strategy:         DDLStrategyGhost,
		},
		{
			strategyVariable: "pt-osc",
			strategy:         DDLStrategyPTOSC,
		},
		{
			strategy: DDLStrategyDirect,
		},
		{
			strategyVariable: "gh-ost --max-load=Threads_running=100 --allow-master",
			strategy:         DDLStrategyGhost,
			options:          "--max-load=Threads_running=100 --allow-master",
			runtimeOptions:   "--max-load=Threads_running=100 --allow-master",
		},
		{
			strategyVariable: "gh-ost --max-load=Threads_running=100 -declarative",
			strategy:         DDLStrategyGhost,
			options:          "--max-load=Threads_running=100 -declarative",
			runtimeOptions:   "--max-load=Threads_running=100",
			isDeclarative:    true,
		},
		{
			strategyVariable: "gh-ost --declarative --max-load=Threads_running=100",
			strategy:         DDLStrategyGhost,
			options:          "--declarative --max-load=Threads_running=100",
			runtimeOptions:   "--max-load=Threads_running=100",
			isDeclarative:    true,
		},
		{
			strategyVariable: "pt-osc -singleton",
			strategy:         DDLStrategyPTOSC,
			options:          "-singleton",
			runtimeOptions:   "",
			isSingleton:      true,
		},
	}
	for _, ts := range tt {
		setting, err := ParseDDLStrategy(ts.strategyVariable)
		assert.NoError(t, err)
		assert.Equal(t, ts.strategy, setting.Strategy)
		assert.Equal(t, ts.options, setting.Options)
		assert.Equal(t, ts.isDeclarative, setting.IsDeclarative())
		assert.Equal(t, ts.isSingleton, setting.IsSingleton())

		runtimeOptions := strings.Join(setting.RuntimeOptions(), " ")
		assert.Equal(t, ts.runtimeOptions, runtimeOptions)
	}
	{
		_, err := ParseDDLStrategy("other")
		assert.Error(t, err)
	}
}
