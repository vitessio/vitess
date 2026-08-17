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

package schemadiff

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtenv"
)

type Environment struct {
	*vtenv.Environment
	DefaultColl collations.ID
}

func NewTestEnv() *Environment {
	return &Environment{
		Environment: vtenv.NewTestEnv(),
		DefaultColl: collations.MySQL8().DefaultConnectionCharset(),
	}
}

func NewEnv(env *vtenv.Environment, defaultColl collations.ID) *Environment {
	return &Environment{
		Environment: env,
		DefaultColl: defaultColl,
	}
}
