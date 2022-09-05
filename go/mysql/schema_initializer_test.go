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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestSchemaInitializerRegisterInitializer(t *testing.T) {

	func1 := func(conn *Conn) error { return nil }
	func2 := func(conn *Conn) error { return vterrors.Errorf(vtrpc.Code_ABORTED, "test aborted ...") }

	SchemaInitializer.RegisterSchemaInitializer("func1", func1, false, true)
	SchemaInitializer.RegisterSchemaInitializer("func2", func2, false, true)
	// duplication of same name is ignored
	SchemaInitializer.RegisterSchemaInitializer("func1", func1, false, true)

	require.EqualValues(t, len(SchemaInitializer.getAllRegisteredFunctions()), 2)
}

func TestSchemaInitializerOrderInitializer(t *testing.T) {

	func1 := func(conn *Conn) error { return nil }
	func2 := func(conn *Conn) error { return vterrors.Errorf(vtrpc.Code_ABORTED, "test aborted ...") }

	SchemaInitializer.RegisterSchemaInitializer("func1", func1, false, true)
	SchemaInitializer.RegisterSchemaInitializer("func2", func2, false, true)
	// duplication of same name is ignored
	SchemaInitializer.RegisterSchemaInitializer("func1", func1, false, true)

	functions := SchemaInitializer.getAllRegisteredFunctions()
	require.EqualValues(t, len(functions), 2)
	require.EqualValues(t, functions[0], "func1")
	require.EqualValues(t, functions[1], "func2")

	// now add another one at head
	SchemaInitializer.RegisterSchemaInitializer("func3", func1, true, true)
	functions = SchemaInitializer.getAllRegisteredFunctions()
	require.EqualValues(t, len(functions), 3)
	require.EqualValues(t, functions[0], "func3")
	require.EqualValues(t, functions[1], "func1")
	require.EqualValues(t, functions[2], "func2")
}
