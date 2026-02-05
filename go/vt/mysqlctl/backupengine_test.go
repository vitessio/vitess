package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateMySQLVersionUpgradeCompatible(t *testing.T) {
	// Test that the MySQL version is compatible with the upgrade.
	testCases := []struct {
		name        string
		fromVersion string
		toVersion   string
		upgradeSafe bool
		error       string
	}{
		{
			name:        "upgrade from 5.7 to 8.0",
			fromVersion: "mysqld  Ver 5.7.35",
			toVersion:   "mysqld  Ver 8.0.23",
			upgradeSafe: true,
		},
		{
			name:        "downgrade from 8.0 to 5.7",
			fromVersion: "mysqld  Ver 8.0.23",
			toVersion:   "mysqld  Ver 5.7.35",
			upgradeSafe: true,
			error:       `running MySQL version "mysqld  Ver 5.7.35" is older than backup MySQL version "mysqld  Ver 8.0.23"`,
		},
		{
			name:        "upgrade from 5.7 to 8.0",
			fromVersion: "mysqld  Ver 5.7.35",
			toVersion:   "mysqld  Ver 8.0.23",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.0.23" is newer than backup MySQL version "mysqld  Ver 5.7.35" which is not safe to upgrade`,
		},
		{
			name:        "downgrade from 8.0 to 5.7",
			fromVersion: "mysqld  Ver 8.0.23",
			toVersion:   "mysqld  Ver 5.7.35",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 5.7.35" is older than backup MySQL version "mysqld  Ver 8.0.23"`,
		},
		{
			name:        "upgrade from 8.0.23 to 8.0.34",
			fromVersion: "mysqld  Ver 8.0.23",
			toVersion:   "mysqld  Ver 8.0.34",
			upgradeSafe: true,
		},
		{
			name:        "downgrade from 8.0.34 to 8.0.23",
			fromVersion: "mysqld  Ver 8.0.34",
			toVersion:   "mysqld  Ver 8.0.23",
			upgradeSafe: true,
			error:       `running MySQL version "mysqld  Ver 8.0.23" is older than backup MySQL version "mysqld  Ver 8.0.34"`,
		},
		{
			name:        "upgrade from 8.0.23 to 8.0.34",
			fromVersion: "mysqld  Ver 8.0.23",
			toVersion:   "mysqld  Ver 8.0.34",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.0.34" is newer than backup MySQL version "mysqld  Ver 8.0.23" which is not safe to upgrade`,
		},
		{
			name:        "downgrade from 8.0.34 to 8.0.23",
			fromVersion: "mysqld  Ver 8.0.34",
			toVersion:   "mysqld  Ver 8.0.23",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.0.23" is older than backup MySQL version "mysqld  Ver 8.0.34"`,
		},
		{
			name:        "upgrade from 8.0.32 to 8.0.36",
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.36",
			upgradeSafe: true,
		},
		{
			name:        "downgrade from 8.0.36 to 8.0.32",
			fromVersion: "mysqld  Ver 8.0.36",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: true,
			error:       `running MySQL version "mysqld  Ver 8.0.32" is older than backup MySQL version "mysqld  Ver 8.0.36"`,
		},
		{
			name:        "upgrade from 8.0.32 to 8.0.36",
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.36",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.0.36" is newer than backup MySQL version "mysqld  Ver 8.0.32" which is not safe to upgrade`,
		},
		{
			name:        "downgrade from 8.0.36 to 8.0.32",
			fromVersion: "mysqld  Ver 8.0.36",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.0.32" is older than backup MySQL version "mysqld  Ver 8.0.36"`,
		},
		{
			name:        "upgrade from 8.0.35 to 8.0.36",
			fromVersion: "mysqld  Ver 8.0.35",
			toVersion:   "mysqld  Ver 8.0.36",
			upgradeSafe: true,
		},
		{
			name:        "downgrade from 8.0.36 to 8.0.35",
			fromVersion: "mysqld  Ver 8.0.36",
			toVersion:   "mysqld  Ver 8.0.35",
			upgradeSafe: true,
		},
		{
			name:        "upgrade from 8.0.35 to 8.0.36",
			fromVersion: "mysqld  Ver 8.0.35",
			toVersion:   "mysqld  Ver 8.0.36",
			upgradeSafe: false,
		},
		{
			name:        "downgrade from 8.0.36 to 8.0.35",
			fromVersion: "mysqld  Ver 8.0.36",
			toVersion:   "mysqld  Ver 8.0.35",
			upgradeSafe: false,
		},
		{
			name:        "upgrade from 8.4.0 to 8.4.1",
			fromVersion: "mysqld  Ver 8.4.0",
			toVersion:   "mysqld  Ver 8.4.1",
			upgradeSafe: true,
		},
		{
			name:        "downgrade from 8.4.1 to 8.4.0",
			fromVersion: "mysqld  Ver 8.4.1",
			toVersion:   "mysqld  Ver 8.4.0",
			upgradeSafe: true,
		},
		{
			name:        "upgrade from 8.4.0 to 8.4.1",
			fromVersion: "mysqld  Ver 8.4.0",
			toVersion:   "mysqld  Ver 8.4.1",
			upgradeSafe: false,
		},
		{
			name:        "downgrade from 8.4.1 to 8.4.0",
			fromVersion: "mysqld  Ver 8.4.1",
			toVersion:   "mysqld  Ver 8.4.0",
			upgradeSafe: false,
		},
		{
			name:        "upgrade from 8.0.35 to 8.4.0",
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.4.0",
			upgradeSafe: true,
		},
		{
			name:        "downgrade from 8.4.0 to 8.0.32",
			fromVersion: "mysqld  Ver 8.4.0",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: true,
			error:       `running MySQL version "mysqld  Ver 8.0.32" is older than backup MySQL version "mysqld  Ver 8.4.0"`,
		},
		{
			name:        "upgrade from 8.0.32 to 8.4.0",
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.4.0",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.4.0" is newer than backup MySQL version "mysqld  Ver 8.0.32" which is not safe to upgrade`,
		},
		{
			name:        "downgrade from 8.4.0 to 8.0.32",
			fromVersion: "mysqld  Ver 8.4.0",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.0.32" is older than backup MySQL version "mysqld  Ver 8.4.0"`,
		},
		{
			name:        "upgrade from 5.7.35 to 8.4.0",
			fromVersion: "mysqld  Ver 5.7.32",
			toVersion:   "mysqld  Ver 8.4.0",
			upgradeSafe: true,
			error:       `running MySQL version "mysqld  Ver 8.4.0" is too new for backup MySQL version "mysqld  Ver 5.7.32"`,
		},
		{
			name:        "downgrade from 8.4.0 to 5.7.32",
			fromVersion: "mysqld  Ver 8.4.0",
			toVersion:   "mysqld  Ver 5.7.32",
			upgradeSafe: true,
			error:       `running MySQL version "mysqld  Ver 5.7.32" is older than backup MySQL version "mysqld  Ver 8.4.0"`,
		},
		{
			name:        "upgrade from 5.7.32 to 8.4.0",
			fromVersion: "mysqld  Ver 5.7.32",
			toVersion:   "mysqld  Ver 8.4.0",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 8.4.0" is newer than backup MySQL version "mysqld  Ver 5.7.32" which is not safe to upgrade`,
		},
		{
			name:        "downgrade from 8.4.0 to 5.7.32",
			fromVersion: "mysqld  Ver 8.4.0",
			toVersion:   "mysqld  Ver 5.7.32",
			upgradeSafe: false,
			error:       `running MySQL version "mysqld  Ver 5.7.32" is older than backup MySQL version "mysqld  Ver 8.4.0"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMySQLVersionUpgradeCompatible(tc.toVersion, tc.fromVersion, tc.upgradeSafe)
			if tc.error == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.error)
			}
		})
	}

}
