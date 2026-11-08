/*
Copyright 2026 The Vitess Authors.

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

package tabletthrottler

import (
	"context"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// ThrottlerClientWrapper defines the methods we use from a Throttler client.
// This is used to make the code testable.
type ThrottlerClientWrapper interface {
	ThrottleCheckOK(ctx context.Context, overrideAppName throttlerapp.Name) (checkResult *throttle.CheckResult, throttleCheckOK bool)
}

// assert that throttle.Client implements ThrottlerClientWrapper.
var _ ThrottlerClientWrapper = (*throttle.Client)(nil)
