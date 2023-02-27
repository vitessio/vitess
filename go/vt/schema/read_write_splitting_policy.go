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
)

var (
	readWriteSplittingPolicyParserRegexp = regexp.MustCompile(`^([\S]+)\s+(.*)$`)
)

type ReadWriteSplittingPolicy string

const (
	// ReadWriteSplittingPolicyDisable disables read write splitting
	ReadWriteSplittingPolicyDisable ReadWriteSplittingPolicy = "disable"
	// ReadWriteSplittingPolicyRandom enables read write splitting using random policy
	ReadWriteSplittingPolicyRandom ReadWriteSplittingPolicy = "random"
)

// IsRandom returns true if the strategy is random
func (s ReadWriteSplittingPolicy) IsRandom() bool {
	return s == ReadWriteSplittingPolicyRandom
}

type ReadWriteSplittingPolicySetting struct {
	Strategy ReadWriteSplittingPolicy `json:"strategy,omitempty"`
	Options  string                   `json:"options,omitempty"`
}

func NewReadWriteSplittingPolicySettingSetting(strategy ReadWriteSplittingPolicy, options string) *ReadWriteSplittingPolicySetting {
	return &ReadWriteSplittingPolicySetting{
		Strategy: strategy,
		Options:  options,
	}
}

func ParseReadWriteSplittingPolicy(strategyVariable string) (*ReadWriteSplittingPolicySetting, error) {
	setting := &ReadWriteSplittingPolicySetting{}
	strategyName := strategyVariable
	if submatch := readWriteSplittingPolicyParserRegexp.FindStringSubmatch(strategyVariable); len(submatch) > 0 {
		strategyName = submatch[1]
		setting.Options = submatch[2]
	}

	switch strategy := ReadWriteSplittingPolicy(strategyName); strategy {
	case "": // backward compatiblity and to handle unspecified values
		setting.Strategy = ReadWriteSplittingPolicyDisable
	case ReadWriteSplittingPolicyRandom, ReadWriteSplittingPolicyDisable:
		setting.Strategy = strategy
	default:
		return nil, fmt.Errorf("Unknown ReadWriteSplittingPolicy: '%v'", strategy)
	}
	return setting, nil
}

// ToString returns a simple string representation of this instance
func (setting *ReadWriteSplittingPolicySetting) ToString() string {
	return fmt.Sprintf("ReadWriteSplittingPolicySetting: strategy=%v, options=%s", setting.Strategy, setting.Options)
}
