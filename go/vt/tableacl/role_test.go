/*
Copyright 2017 Google Inc.

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

import "testing"

func TestRoleName(t *testing.T) {
	if READER.Name() != roleNames[READER] {
		t.Fatalf("role READER does not return expected name, expected: %s, actual: %s", roleNames[READER], READER.Name())
	}

	if WRITER.Name() != roleNames[WRITER] {
		t.Fatalf("role WRITER does not return expected name, expected: %s, actual: %s", roleNames[WRITER], WRITER.Name())
	}

	if ADMIN.Name() != roleNames[ADMIN] {
		t.Fatalf("role ADMIN does not return expected name, expected: %s, actual: %s", roleNames[ADMIN], ADMIN.Name())
	}

	unknownRole := Role(-1)
	if unknownRole.Name() != "" {
		t.Fatalf("role is not defined, expected to get an empty string but got: %s", unknownRole.Name())
	}
}

func TestRoleByName(t *testing.T) {
	if role, ok := RoleByName("unknown"); ok {
		t.Fatalf("should not find a valid role for invalid name, but got role: %s", role.Name())
	}

	role, ok := RoleByName("READER")
	if !ok || role != READER {
		t.Fatalf("string READER should return role READER")
	}

	role, ok = RoleByName("WRITER")
	if !ok || role != WRITER {
		t.Fatalf("string WRITER should return role WRITER")
	}

	role, ok = RoleByName("ADMIN")
	if !ok || role != ADMIN {
		t.Fatalf("string ADMIN should return role ADMIN")
	}
}
