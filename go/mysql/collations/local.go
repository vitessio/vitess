//go:build !collations_library

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

package collations

import (
	"sync"

	"vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/vt/servenv"
)

var defaultEnv *Environment
var defaultEnvInit sync.Once

// Local is the default collation Environment for Vitess. This depends
// on the value of the `mysql_server_version` flag passed to this Vitess process.
func Local() *Environment {
	defaultEnvInit.Do(func() {
		if !flag.Parsed() {
			panic("collations.Local() called too early")
		}
		defaultEnv = NewEnvironment(servenv.MySQLServerVersion())
	})
	return defaultEnv
}

// Default returns the default collation for this Vitess process.
// This is based on the local collation environment, which is based on the user's configured
// MySQL version for this Vitess deployment.
func Default() ID {
	return ID(Local().DefaultConnectionCharset())
}
