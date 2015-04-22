// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
