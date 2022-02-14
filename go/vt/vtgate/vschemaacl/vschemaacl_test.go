package vschemaacl

import (
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestVschemaAcl(t *testing.T) {
	redUser := querypb.VTGateCallerID{Username: "redUser"}
	yellowUser := querypb.VTGateCallerID{Username: "yellowUser"}

	// By default no users are allowed in
	if Authorized(&redUser) {
		t.Errorf("user should not be authorized")
	}
	if Authorized(&yellowUser) {
		t.Errorf("user should not be authorized")
	}

	// Test wildcard
	*AuthorizedDDLUsers = "%"
	Init()

	if !Authorized(&redUser) {
		t.Errorf("user should be authorized")
	}
	if !Authorized(&yellowUser) {
		t.Errorf("user should be authorized")
	}

	// Test user list
	*AuthorizedDDLUsers = "oneUser, twoUser, redUser, blueUser"
	Init()

	if !Authorized(&redUser) {
		t.Errorf("user should be authorized")
	}
	if Authorized(&yellowUser) {
		t.Errorf("user should not be authorized")
	}

	// Revert to baseline state for other tests
	*AuthorizedDDLUsers = ""
	Init()

	// By default no users are allowed in
	if Authorized(&redUser) {
		t.Errorf("user should not be authorized")
	}
	if Authorized(&yellowUser) {
		t.Errorf("user should not be authorized")
	}
}
