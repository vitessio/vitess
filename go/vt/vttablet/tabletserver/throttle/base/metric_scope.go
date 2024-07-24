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
	"fmt"
)

// Scope defines the tablet range from which a metric is collected. This can be the local tablet
// ("self") or the entire shard ("shard")
type Scope string

const (
	UndefinedScope Scope = ""
	ShardScope     Scope = "shard"
	SelfScope      Scope = "self"
)

func (s Scope) String() string {
	return string(s)
}

func ScopeFromString(s string) (Scope, error) {
	switch scope := Scope(s); scope {
	case UndefinedScope, ShardScope, SelfScope:
		return scope, nil
	default:
		return "", fmt.Errorf("unknown scope: %s", s)
	}
}
