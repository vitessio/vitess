package env

import (
	"os"
	"testing"
)

func TestVtDataRoot(t *testing.T) {
	envVar := "VTDATAROOT"
	oldEnvVar := os.Getenv(envVar)

	if oldEnvVar != "" {
		os.Setenv(envVar, "")
	}

	defer os.Setenv(envVar, oldEnvVar)

	root := VtDataRoot()
	if root != DefaultVtDataRoot {
		t.Errorf("When VTDATAROOT is not set, the default value should be %v, not %v.", DefaultVtDataRoot, root)
	}

	passed := "/tmp"
	os.Setenv(envVar, passed)
	root = VtDataRoot()
	if root != passed {
		t.Errorf("The value of VtDataRoot should be %v, not %v.", passed, root)
	}
}
