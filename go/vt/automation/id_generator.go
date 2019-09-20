/*
Copyright 2019 The Vitess Authors.

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

package automation

import (
	"strconv"
	"sync/atomic"
)

// IDGenerator generates unique task and cluster operation IDs.
type IDGenerator struct {
	counter int64
}

// GetNextID returns an ID which wasn't returned before.
func (ig *IDGenerator) GetNextID() string {
	return strconv.FormatInt(atomic.AddInt64(&ig.counter, 1), 10)
}
