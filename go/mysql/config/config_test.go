// config_test.go

package config

import (
	"testing"
)

func TestDefaultValues(t *testing.T) {
	// Test DefaultSQLMode
	if DefaultSQLMode != "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION" {
		t.Errorf("DefaultSQLMode is incorrect, got: %s, want: %s", DefaultSQLMode, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")
	}

	// Test DefaultMySQLVersion
	if DefaultMySQLVersion != "8.0.30" {
		t.Errorf("DefaultMySQLVersion is incorrect, got: %s, want: %s", DefaultMySQLVersion, "8.0.30")
	}
}
