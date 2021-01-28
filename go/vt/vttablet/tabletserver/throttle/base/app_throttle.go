/*
Copyright 2020 The Vitess Authors.

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
	"time"
)

// AppThrottle is the definition for an app throttling instruction
// - Ratio: [0..1], 0 == no throttle, 1 == fully throttle
type AppThrottle struct {
	AppName  string
	ExpireAt time.Time
	Ratio    float64
}

// NewAppThrottle creates an AppThrottle struct
func NewAppThrottle(appName string, expireAt time.Time, ratio float64) *AppThrottle {
	result := &AppThrottle{
		AppName:  appName,
		ExpireAt: expireAt,
		Ratio:    ratio,
	}
	return result
}
