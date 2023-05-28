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
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/google/shlex"
)

var (
	strategyParserRegexp       = regexp.MustCompile(`^([\S]+)\s+(.*)$`)
	cutOverThresholdFlagRegexp = regexp.MustCompile(fmt.Sprintf(`^[-]{1,2}%s=(.*?)$`, cutOverThresholdFlag))
)

const (
	declarativeFlag        = "declarative"
	skipTopoFlag           = "skip-topo" // legacy. Kept for backwards compatibility, but unused
	singletonFlag          = "singleton"
	singletonContextFlag   = "singleton-context"
	allowZeroInDateFlag    = "allow-zero-in-date"
	postponeLaunchFlag     = "postpone-launch"
	postponeCompletionFlag = "postpone-completion"
	inOrderCompletionFlag  = "in-order-completion"
	allowConcurrentFlag    = "allow-concurrent"
	preferInstantDDL       = "prefer-instant-ddl"
	fastRangeRotationFlag  = "fast-range-rotation"
	cutOverThresholdFlag   = "cut-over-threshold"
	vreplicationTestSuite  = "vreplication-test-suite"
	allowForeignKeysFlag   = "unsafe-allow-foreign-keys"
)

// DDLStrategy suggests how an ALTER TABLE should run (e.g. "direct", "online", "gh-ost" or "pt-osc")
type DDLStrategy string

const (
	// DDLStrategyDirect means not an online-ddl migration; unmanaged. Just a normal MySQL `ALTER TABLE`
	DDLStrategyDirect DDLStrategy = "direct"
	// DDLStrategyVitess requests vreplication to run the migration; new name for DDLStrategyOnline
	DDLStrategyVitess DDLStrategy = "vitess"
	// DDLStrategyOnline requests vreplication to run the migration
	DDLStrategyOnline DDLStrategy = "online"
	// DDLStrategyGhost requests gh-ost to run the migration
	DDLStrategyGhost DDLStrategy = "gh-ost"
	// DDLStrategyPTOSC requests pt-online-schema-change to run the migration
	DDLStrategyPTOSC DDLStrategy = "pt-osc"
	// DDLStrategyMySQL is a managed migration (queued and executed by the scheduler) but runs through a MySQL `ALTER TABLE`
	DDLStrategyMySQL DDLStrategy = "mysql"
)

// IsDirect returns true if this strategy is a direct strategy
// A strategy is direct if it's not explciitly one of the online DDL strategies
func (s DDLStrategy) IsDirect() bool {
	switch s {
	case DDLStrategyVitess, DDLStrategyOnline, DDLStrategyGhost, DDLStrategyPTOSC, DDLStrategyMySQL:
		return false
	}
	return true
}

// DDLStrategySetting is a formal breakdown of the @@ddl_strategy variable, into strategy and options
type DDLStrategySetting struct {
	Strategy DDLStrategy `json:"strategy,omitempty"`
	Options  string      `json:"options,omitempty"`
}

// NewDDLStrategySetting instantiates a new setting
func NewDDLStrategySetting(strategy DDLStrategy, options string) *DDLStrategySetting {
	return &DDLStrategySetting{
		Strategy: strategy,
		Options:  options,
	}
}

// ParseDDLStrategy parses and validates the value of @@ddl_strategy or -ddl_strategy variables
func ParseDDLStrategy(strategyVariable string) (*DDLStrategySetting, error) {
	setting := &DDLStrategySetting{}
	strategyName := strategyVariable
	if submatch := strategyParserRegexp.FindStringSubmatch(strategyVariable); len(submatch) > 0 {
		strategyName = submatch[1]
		setting.Options = submatch[2]
	}

	switch strategy := DDLStrategy(strategyName); strategy {
	case "": // backward compatiblity and to handle unspecified values
		setting.Strategy = DDLStrategyDirect
	case DDLStrategyVitess, DDLStrategyOnline, DDLStrategyGhost, DDLStrategyPTOSC, DDLStrategyMySQL, DDLStrategyDirect:
		setting.Strategy = strategy
	default:
		return nil, fmt.Errorf("Unknown online DDL strategy: '%v'", strategy)
	}
	if _, err := setting.CutOverThreshold(); err != nil {
		return nil, err
	}
	return setting, nil
}

// isFlag return true when the given string is a CLI flag of the given name
func isFlag(s string, name string) bool {
	if s == fmt.Sprintf("-%s", name) {
		return true
	}
	if s == fmt.Sprintf("--%s", name) {
		return true
	}
	return false
}

// hasFlag returns true when Options include named flag
func (setting *DDLStrategySetting) hasFlag(name string) bool {
	opts, _ := shlex.Split(setting.Options)
	for _, opt := range opts {
		if isFlag(opt, name) {
			return true
		}
	}
	return false
}

// IsDeclarative checks if strategy options include --declarative
func (setting *DDLStrategySetting) IsDeclarative() bool {
	return setting.hasFlag(declarativeFlag)
}

// IsSingleton checks if strategy options include --singleton
func (setting *DDLStrategySetting) IsSingleton() bool {
	return setting.hasFlag(singletonFlag)
}

// IsSingletonContext checks if strategy options include --singleton-context
func (setting *DDLStrategySetting) IsSingletonContext() bool {
	return setting.hasFlag(singletonContextFlag)
}

// IsAllowZeroInDateFlag checks if strategy options include --allow-zero-in-date
func (setting *DDLStrategySetting) IsAllowZeroInDateFlag() bool {
	return setting.hasFlag(allowZeroInDateFlag)
}

// IsPostponeLaunch checks if strategy options include --postpone-launch
func (setting *DDLStrategySetting) IsPostponeLaunch() bool {
	return setting.hasFlag(postponeLaunchFlag)
}

// IsPostponeCompletion checks if strategy options include --postpone-completion
func (setting *DDLStrategySetting) IsPostponeCompletion() bool {
	return setting.hasFlag(postponeCompletionFlag)
}

// IsInOrderCompletion checks if strategy options include --in-order-completion
func (setting *DDLStrategySetting) IsInOrderCompletion() bool {
	return setting.hasFlag(inOrderCompletionFlag)
}

// IsAllowConcurrent checks if strategy options include --allow-concurrent
func (setting *DDLStrategySetting) IsAllowConcurrent() bool {
	return setting.hasFlag(allowConcurrentFlag)
}

// IsPreferInstantDDL checks if strategy options include --prefer-instant-ddl
func (setting *DDLStrategySetting) IsPreferInstantDDL() bool {
	return setting.hasFlag(preferInstantDDL)
}

// IsFastRangeRotationFlag checks if strategy options include --fast-range-rotation
func (setting *DDLStrategySetting) IsFastRangeRotationFlag() bool {
	return setting.hasFlag(fastRangeRotationFlag)
}

// isCutOverThresholdFlag returns true when given option denotes a `--cut-over-threshold=[...]` flag
func isCutOverThresholdFlag(opt string) (string, bool) {
	submatch := cutOverThresholdFlagRegexp.FindStringSubmatch(opt)
	if len(submatch) == 0 {
		return "", false
	}
	return submatch[1], true
}

// CutOverThreshold returns a list of shards specified in '--shards=...', or an empty slice if unspecified
func (setting *DDLStrategySetting) CutOverThreshold() (d time.Duration, err error) {
	// We do some ugly manual parsing of --cut-over-threshold value
	opts, _ := shlex.Split(setting.Options)
	for _, opt := range opts {
		if val, isCutOver := isCutOverThresholdFlag(opt); isCutOver {
			// value is possibly quoted
			if s, err := strconv.Unquote(val); err == nil {
				val = s
			}
			if val != "" {
				d, err = time.ParseDuration(val)
			}
		}
	}
	return d, err
}

// IsVreplicationTestSuite checks if strategy options include --vreplicatoin-test-suite
func (setting *DDLStrategySetting) IsVreplicationTestSuite() bool {
	return setting.hasFlag(vreplicationTestSuite)
}

// IsAllowForeignKeysFlag checks if strategy options include --unsafe-allow-foreign-keys
func (setting *DDLStrategySetting) IsAllowForeignKeysFlag() bool {
	return setting.hasFlag(allowForeignKeysFlag)
}

// RuntimeOptions returns the options used as runtime flags for given strategy, removing any internal hint options
func (setting *DDLStrategySetting) RuntimeOptions() []string {
	opts, _ := shlex.Split(setting.Options)
	validOpts := []string{}
	for _, opt := range opts {
		if _, ok := isCutOverThresholdFlag(opt); ok {
			continue
		}
		switch {
		case isFlag(opt, declarativeFlag):
		case isFlag(opt, skipTopoFlag):
		case isFlag(opt, singletonFlag):
		case isFlag(opt, singletonContextFlag):
		case isFlag(opt, allowZeroInDateFlag):
		case isFlag(opt, postponeLaunchFlag):
		case isFlag(opt, postponeCompletionFlag):
		case isFlag(opt, inOrderCompletionFlag):
		case isFlag(opt, allowConcurrentFlag):
		case isFlag(opt, preferInstantDDL):
		case isFlag(opt, fastRangeRotationFlag):
		case isFlag(opt, vreplicationTestSuite):
		case isFlag(opt, allowForeignKeysFlag):
		default:
			validOpts = append(validOpts, opt)
		}
	}
	return validOpts
}

// ToString returns a simple string representation of this instance
func (setting *DDLStrategySetting) ToString() string {
	return fmt.Sprintf("DDLStrategySetting: strategy=%v, options=%s", setting.Strategy, setting.Options)
}
