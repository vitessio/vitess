/*
Copyright 2023 The Vitess Authors.

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

package viperutil

import (
	"vitess.io/vitess/go/viperutil/internal/sync"
	"vitess.io/vitess/go/viperutil/internal/value"
)

var (
	// ErrDuplicateWatch is returned when Watch is called multiple times on a
	// single synced viper. Viper only supports reading/watching a single
	// config file.
	ErrDuplicateWatch = sync.ErrDuplicateWatch
	// ErrNoFlagDefined is returned from Value's Flag method when the value was
	// configured to bind to a given FlagName but the provided flag set does not
	// define a flag with that name.
	ErrNoFlagDefined = value.ErrNoFlagDefined
)
