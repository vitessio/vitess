/*
Copyright 2022 The Vitess Authors.

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

// Package rand provides functions mirroring math/rand that are safe for
// concurrent use, seeded independently of math/rand's global source.
package rand

/*
- TODO: (ajm188) implement the rest of the global functions vtadmin uses.
- TODO: (ajm188) consider moving to go/internal at the top-level for use in
		more places.
*/

import (
	"math/rand"
	"sync"
	"time"
)

var (
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	m sync.Mutex
)

// Float64 implements rand.Float64 on the vtrand global source.
func Float64() float64 {
	m.Lock()
	defer m.Unlock()

	return r.Float64()
}
