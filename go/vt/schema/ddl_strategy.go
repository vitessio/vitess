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
	"strings"
	"time"

	"github.com/google/shlex"

	"vitess.io/vitess/go/vt/log"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
	analyzeTableFlag       = "analyze-table"
)

// DDLStrategySetting is a formal breakdown of the @@ddl_strategy variable, into strategy and options
type DDLStrategySetting struct {
	Strategy tabletmanagerdatapb.OnlineDDL_Strategy `json:"strategy,omitempty"`
	Options  string                                 `json:"options,omitempty"`
}

// NewDDLStrategySetting instantiates a new setting
func NewDDLStrategySetting(strategy tabletmanagerdatapb.OnlineDDL_Strategy, options string) *DDLStrategySetting {
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

	var err error
	setting.Strategy, err = ParseDDLStrategyName(strategyName)
	if err != nil {
		return nil, err
	}
	if _, err := setting.CutOverThreshold(); err != nil {
		return nil, err
	}
	return setting, nil
}

func ParseDDLStrategyName(name string) (tabletmanagerdatapb.OnlineDDL_Strategy, error) {
	if name == "" {
		// backward compatiblity and to handle unspecified values
		return tabletmanagerdatapb.OnlineDDL_DIRECT, nil
	}

	lowerName := strings.ToUpper(name)
	switch lowerName {
	case "GH-OST", "PT-OSC":
		// more backward compatibility since the protobuf message names don't
		// have the dash.
		lowerName = strings.ReplaceAll(lowerName, "-", "")
	default:
	}

	strategy, ok := tabletmanagerdatapb.OnlineDDL_Strategy_value[lowerName]
	if !ok {
		return 0, fmt.Errorf("unknown online DDL strategy: '%v'", name)
	}

	if lowerName != name {
		// TODO (andrew): Remove special handling for lower/uppercase and
		// gh-ost=>ghost/pt-osc=>ptosc support. (file issue for this).
		log.Warningf("detected legacy strategy name syntax; parsed %q as %q. this break in the next version.", lowerName, name)
	}

	return tabletmanagerdatapb.OnlineDDL_Strategy(strategy), nil
}

// OnlineDDLStrategyName returns the text-based form of the strategy.
func OnlineDDLStrategyName(strategy tabletmanagerdatapb.OnlineDDL_Strategy) string {
	name, ok := tabletmanagerdatapb.OnlineDDL_Strategy_name[int32(strategy)]
	if !ok {
		return "unknown"
	}

	return strings.ToLower(name)
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

// IsDirect returns true if this strategy is a direct strategy
// A strategy is direct if it's not explciitly one of the online DDL strategies
func (setting *DDLStrategySetting) IsDirect() bool {
	switch setting.Strategy {
	case tabletmanagerdatapb.OnlineDDL_VITESS,
		// N.B. duplicate value as OnlineDDL_VITESS; comment left here deliberately
		// so future readers know it was not omitted by mistake.
		/* tabletmanagerdatapb.OnlineDDL_ONLINE, */
		tabletmanagerdatapb.OnlineDDL_GHOST,
		tabletmanagerdatapb.OnlineDDL_PTOSC,
		tabletmanagerdatapb.OnlineDDL_MYSQL:

		return false
	}
	return true
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

// IsAnalyzeTableFlag checks if strategy options include --analyze-table
func (setting *DDLStrategySetting) IsAnalyzeTableFlag() bool {
	return setting.hasFlag(analyzeTableFlag)
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
		case isFlag(opt, analyzeTableFlag):
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
