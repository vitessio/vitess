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
)

func TestIsDirect(t *testing.T) {
	assert.True(t, DDLStrategyDirect.IsDirect())
	assert.False(t, DDLStrategyVitess.IsDirect())
	assert.False(t, DDLStrategyOnline.IsDirect())
	assert.False(t, DDLStrategyGhost.IsDirect())
	assert.False(t, DDLStrategyPTOSC.IsDirect())
	assert.True(t, DDLStrategy("").IsDirect())
	assert.False(t, DDLStrategy("vitess").IsDirect())
	assert.False(t, DDLStrategy("online").IsDirect())
	assert.False(t, DDLStrategy("gh-ost").IsDirect())
	assert.False(t, DDLStrategy("pt-osc").IsDirect())
	assert.True(t, DDLStrategy("something").IsDirect())
}

func TestIsExpireArtifactsFlag(t *testing.T) {
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
			s: "-retain-artifacts",
		},
		{
			s: "--retain-artifacts",
		},
		{
			s:      "--retain-artifacts=",
			expect: true,
		},
		{
			s:      "--retain-artifacts=0",
			expect: true,
			val:    "0",
			d:      0,
		},
		{
			s:      "-retain-artifacts=0",
			expect: true,
			val:    "0",
			d:      0,
		},
		{
			s:      "--retain-artifacts=1m",
			expect: true,
			val:    "1m",
			d:      time.Minute,
		},
		{
			s:      `--retain-artifacts="1m"`,
			expect: true,
			val:    `"1m"`,
			d:      time.Minute,
		},
	}
	for _, ts := range tt {
		t.Run(ts.s, func(t *testing.T) {
			setting, err := ParseDDLStrategy("online " + ts.s)
			assert.NoError(t, err)

			val, isRetainArtifacts := isRetainArtifactsFlag(ts.s)
			assert.Equal(t, ts.expect, isRetainArtifacts)
			assert.Equal(t, ts.val, val)

			if ts.expect {
				d, err := setting.RetainArtifactsDuration()
				assert.NoError(t, err)
				assert.Equal(t, ts.d, d)
			}
		})
	}
}

func TestParseDDLStrategy(t *testing.T) {
	tt := []struct {
		strategyVariable     string
		strategy             DDLStrategy
		options              string
		isDeclarative        bool
		isSingleton          bool
		isPostponeLaunch     bool
		isPostponeCompletion bool
		isAllowConcurrent    bool
		fastOverRevertible   bool
		fastRangeRotation    bool
		expireArtifacts      time.Duration
		runtimeOptions       string
		err                  error
	}{
		{
			strategyVariable: "direct",
			strategy:         DDLStrategyDirect,
		},
		{
			strategyVariable: "vitess",
			strategy:         DDLStrategyVitess,
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
			// These are gh-ost options. Nothing we can do until that changes upstream
			options:        "--max-load=Threads_running=100 --allow-master",
			runtimeOptions: "--max-load=Threads_running=100 --allow-master",
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
		{
			strategyVariable: "online -postpone-launch",
			strategy:         DDLStrategyOnline,
			options:          "-postpone-launch",
			runtimeOptions:   "",
			isPostponeLaunch: true,
		},
		{
			strategyVariable:     "online -postpone-completion",
			strategy:             DDLStrategyOnline,
			options:              "-postpone-completion",
			runtimeOptions:       "",
			isPostponeCompletion: true,
		},
		{
			strategyVariable:  "online -allow-concurrent",
			strategy:          DDLStrategyOnline,
			options:           "-allow-concurrent",
			runtimeOptions:    "",
			isAllowConcurrent: true,
		},
		{
			strategyVariable:  "vitess -allow-concurrent",
			strategy:          DDLStrategyVitess,
			options:           "-allow-concurrent",
			runtimeOptions:    "",
			isAllowConcurrent: true,
		},
		{
			strategyVariable:   "vitess --fast-over-revertible",
			strategy:           DDLStrategyVitess,
			options:            "--fast-over-revertible",
			runtimeOptions:     "",
			fastOverRevertible: true,
		},
		{
			strategyVariable:  "vitess --fast-range-rotation",
			strategy:          DDLStrategyVitess,
			options:           "--fast-range-rotation",
			runtimeOptions:    "",
			fastRangeRotation: true,
		},
		{
			strategyVariable: "vitess --retain-artifacts=4m",
			strategy:         DDLStrategyVitess,
			options:          "--retain-artifacts=4m",
			runtimeOptions:   "",
			expireArtifacts:  4 * time.Minute,
		},
	}
	for _, ts := range tt {
		setting, err := ParseDDLStrategy(ts.strategyVariable)
		assert.NoError(t, err)
		assert.Equal(t, ts.strategy, setting.Strategy)
		assert.Equal(t, ts.options, setting.Options)
		assert.Equal(t, ts.isDeclarative, setting.IsDeclarative())
		assert.Equal(t, ts.isSingleton, setting.IsSingleton())
		assert.Equal(t, ts.isPostponeCompletion, setting.IsPostponeCompletion())
		assert.Equal(t, ts.isPostponeLaunch, setting.IsPostponeLaunch())
		assert.Equal(t, ts.isAllowConcurrent, setting.IsAllowConcurrent())
		assert.Equal(t, ts.fastOverRevertible, setting.IsFastOverRevertibleFlag())
		assert.Equal(t, ts.fastRangeRotation, setting.IsFastRangeRotationFlag())

		runtimeOptions := strings.Join(setting.RuntimeOptions(), " ")
		assert.Equal(t, ts.runtimeOptions, runtimeOptions)
	}
	{
		_, err := ParseDDLStrategy("other")
		assert.Error(t, err)
	}
	{
		_, err := ParseDDLStrategy("online --retain-artifacts=3")
		assert.Error(t, err)
	}
}
