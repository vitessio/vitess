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

package tableacl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoleName(t *testing.T) {
	require.Equalf(t, roleNames[READER], READER.Name(), "role READER does not return expected name, expected: %s, actual: %s", roleNames[READER], READER.Name())
	require.Equalf(t, roleNames[WRITER], WRITER.Name(), "role WRITER does not return expected name, expected: %s, actual: %s", roleNames[WRITER], WRITER.Name())
	require.Equalf(t, roleNames[ADMIN], ADMIN.Name(), "role ADMIN does not return expected name, expected: %s, actual: %s", roleNames[ADMIN], ADMIN.Name())

	unknownRole := Role(-1)
	require.Emptyf(t, unknownRole.Name(), "role is not defined, expected to get an empty string but got: %s", unknownRole.Name())
}

func TestRoleByName(t *testing.T) {
	role, ok := RoleByName("unknown")
	require.Falsef(t, ok, "should not find a valid role for invalid name, but got role: %s", role.Name())

	role, ok = RoleByName("READER")
	assert.Truef(t, ok && role == READER, "string READER should return role READER")

	role, ok = RoleByName("WRITER")
	assert.Truef(t, ok && role == WRITER, "string WRITER should return role WRITER")

	role, ok = RoleByName("ADMIN")
	assert.Truef(t, ok && role == ADMIN, "string ADMIN should return role ADMIN")
}
