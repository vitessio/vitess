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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestIsDirect(t *testing.T) {
	assert.True(t, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_DIRECT, "").IsDirect())
	assert.False(t, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_VITESS, "").IsDirect())
	assert.False(t, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_ONLINE, "").IsDirect())
	assert.False(t, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_GHOST, "").IsDirect())
	assert.False(t, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_PTOSC, "").IsDirect())
	assert.False(t, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_MYSQL, "").IsDirect())

	for name, direct := range map[string]bool{
		"":       true,
		"vitess": false,
		"online": false,
		"gh-ost": false,
		"pt-osc": false,
		"mysql":  false,
	} {
		strategy, err := ParseDDLStrategyName(name)
		require.NoError(t, err)

		var assertion func(t assert.TestingT, value bool, msgAndArgs ...any) bool
		switch direct {
		case true:
			assertion = assert.True
		case false:
			assertion = assert.False
		}

		assertion(t, NewDDLStrategySetting(strategy, "").IsDirect())
	}
}

func TestIsCutOverThresholdFlag(t *testing.T) {
	tt := []struct {
		s      string
		expect bool
		val    string
		d      time.Duration
	}{
		{
			s: "something",
		},
		{
			s: "-cut-over-threshold",
		},
		{
			s: "--cut-over-threshold",
		},
		{
			s:      "--cut-over-threshold=",
			expect: true,
		},
		{
			s:      "--cut-over-threshold=0",
			expect: true,
			val:    "0",
			d:      0,
		},
		{
			s:      "-cut-over-threshold=0",
			expect: true,
			val:    "0",
			d:      0,
		},
		{
			s:      "--cut-over-threshold=1m",
			expect: true,
			val:    "1m",
			d:      time.Minute,
		},
		{
			s:      `--cut-over-threshold="1m"`,
			expect: true,
			val:    `"1m"`,
			d:      time.Minute,
		},
	}
	for _, ts := range tt {
		t.Run(ts.s, func(t *testing.T) {
			setting, err := ParseDDLStrategy("online " + ts.s)
			assert.NoError(t, err)

			val, isCutOver := isCutOverThresholdFlag((ts.s))
			assert.Equal(t, ts.expect, isCutOver)
			assert.Equal(t, ts.val, val)

			if ts.expect {
				d, err := setting.CutOverThreshold()
				assert.NoError(t, err)
				assert.Equal(t, ts.d, d)
			}
		})
	}
}

func TestParseDDLStrategy(t *testing.T) {
	tt := []struct {
		strategyVariable     string
		strategy             tabletmanagerdatapb.OnlineDDL_Strategy
		options              string
		isDeclarative        bool
		isSingleton          bool
		isPostponeLaunch     bool
		isPostponeCompletion bool
		isInOrderCompletion  bool
		isAllowConcurrent    bool
		fastOverRevertible   bool
		fastRangeRotation    bool
		allowForeignKeys     bool
		analyzeTable         bool
		cutOverThreshold     time.Duration
		runtimeOptions       string
		err                  error
	}{
		{
			strategyVariable: "direct",
			strategy:         tabletmanagerdatapb.OnlineDDL_DIRECT,
		},
		{
			strategyVariable: "vitess",
			strategy:         tabletmanagerdatapb.OnlineDDL_VITESS,
		},
		{
			strategyVariable: "online",
			strategy:         tabletmanagerdatapb.OnlineDDL_ONLINE,
		},
		{
			strategyVariable: "gh-ost",
			strategy:         tabletmanagerdatapb.OnlineDDL_GHOST,
		},
		{
			strategyVariable: "pt-osc",
			strategy:         tabletmanagerdatapb.OnlineDDL_PTOSC,
		},
		{
			strategyVariable: "mysql",
			strategy:         tabletmanagerdatapb.OnlineDDL_MYSQL,
		},
		{
			strategy: tabletmanagerdatapb.OnlineDDL_DIRECT,
		},
		{
			strategyVariable: "gh-ost --max-load=Threads_running=100 --allow-master",
			strategy:         tabletmanagerdatapb.OnlineDDL_GHOST,
			// These are gh-ost options. Nothing we can do until that changes upstream
			options:        "--max-load=Threads_running=100 --allow-master",
			runtimeOptions: "--max-load=Threads_running=100 --allow-master",
		},
		{
			strategyVariable: "gh-ost --max-load=Threads_running=100 -declarative",
			strategy:         tabletmanagerdatapb.OnlineDDL_GHOST,
			options:          "--max-load=Threads_running=100 -declarative",
			runtimeOptions:   "--max-load=Threads_running=100",
			isDeclarative:    true,
		},
		{
			strategyVariable: "gh-ost --declarative --max-load=Threads_running=100",
			strategy:         tabletmanagerdatapb.OnlineDDL_GHOST,
			options:          "--declarative --max-load=Threads_running=100",
			runtimeOptions:   "--max-load=Threads_running=100",
			isDeclarative:    true,
		},
		{
			strategyVariable: "pt-osc -singleton",
			strategy:         tabletmanagerdatapb.OnlineDDL_PTOSC,
			options:          "-singleton",
			runtimeOptions:   "",
			isSingleton:      true,
		},
		{
			strategyVariable: "online -postpone-launch",
			strategy:         tabletmanagerdatapb.OnlineDDL_ONLINE,
			options:          "-postpone-launch",
			runtimeOptions:   "",
			isPostponeLaunch: true,
		},
		{
			strategyVariable:     "online -postpone-completion",
			strategy:             tabletmanagerdatapb.OnlineDDL_ONLINE,
			options:              "-postpone-completion",
			runtimeOptions:       "",
			isPostponeCompletion: true,
		},
		{
			strategyVariable:    "online --in-order-completion",
			strategy:            tabletmanagerdatapb.OnlineDDL_ONLINE,
			options:             "--in-order-completion",
			runtimeOptions:      "",
			isInOrderCompletion: true,
		},
		{
			strategyVariable:  "online -allow-concurrent",
			strategy:          tabletmanagerdatapb.OnlineDDL_ONLINE,
			options:           "-allow-concurrent",
			runtimeOptions:    "",
			isAllowConcurrent: true,
		},
		{
			strategyVariable:  "vitess -allow-concurrent",
			strategy:          tabletmanagerdatapb.OnlineDDL_VITESS,
			options:           "-allow-concurrent",
			runtimeOptions:    "",
			isAllowConcurrent: true,
		},
		{
			strategyVariable:   "vitess --prefer-instant-ddl",
			strategy:           tabletmanagerdatapb.OnlineDDL_VITESS,
			options:            "--prefer-instant-ddl",
			runtimeOptions:     "",
			fastOverRevertible: true,
		},
		{
			strategyVariable:  "vitess --fast-range-rotation",
			strategy:          tabletmanagerdatapb.OnlineDDL_VITESS,
			options:           "--fast-range-rotation",
			runtimeOptions:    "",
			fastRangeRotation: true,
		},
		{
			strategyVariable: "vitess --unsafe-allow-foreign-keys",
			strategy:         tabletmanagerdatapb.OnlineDDL_VITESS,
			options:          "--unsafe-allow-foreign-keys",
			runtimeOptions:   "",
			allowForeignKeys: true,
		},
		{
			strategyVariable: "vitess --cut-over-threshold=5m",
			strategy:         tabletmanagerdatapb.OnlineDDL_VITESS,
			options:          "--cut-over-threshold=5m",
			runtimeOptions:   "",
			cutOverThreshold: 5 * time.Minute,
		},
		{
			strategyVariable: "vitess --analyze-table",
			strategy:         tabletmanagerdatapb.OnlineDDL_VITESS,
			options:          "--analyze-table",
			runtimeOptions:   "",
			analyzeTable:     true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.strategyVariable, func(t *testing.T) {
			setting, err := ParseDDLStrategy(ts.strategyVariable)
			assert.NoError(t, err)
			assert.Equal(t, ts.strategy, setting.Strategy)
			assert.Equal(t, ts.options, setting.Options)
			assert.Equal(t, ts.isDeclarative, setting.IsDeclarative())
			assert.Equal(t, ts.isSingleton, setting.IsSingleton())
			assert.Equal(t, ts.isPostponeCompletion, setting.IsPostponeCompletion())
			assert.Equal(t, ts.isPostponeLaunch, setting.IsPostponeLaunch())
			assert.Equal(t, ts.isAllowConcurrent, setting.IsAllowConcurrent())
			assert.Equal(t, ts.fastOverRevertible, setting.IsPreferInstantDDL())
			assert.Equal(t, ts.fastRangeRotation, setting.IsFastRangeRotationFlag())
			assert.Equal(t, ts.allowForeignKeys, setting.IsAllowForeignKeysFlag())
			assert.Equal(t, ts.analyzeTable, setting.IsAnalyzeTableFlag())
			cutOverThreshold, err := setting.CutOverThreshold()
			assert.NoError(t, err)
			assert.Equal(t, ts.cutOverThreshold, cutOverThreshold)

			runtimeOptions := strings.Join(setting.RuntimeOptions(), " ")
			assert.Equal(t, ts.runtimeOptions, runtimeOptions)
		})
	}
	{
		_, err := ParseDDLStrategy("other")
		assert.Error(t, err)
	}
	{
		_, err := ParseDDLStrategy("online --cut-over-threshold=X")
		assert.Error(t, err)
	}
	{
		_, err := ParseDDLStrategy("online --cut-over-threshold=3")
		assert.Error(t, err)
	}
}
