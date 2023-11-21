package cli

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestWaitForDBAGrants(t *testing.T) {
	tests := []struct {
		name      string
		waitTime  time.Duration
		errWanted string
		setupFunc func(t *testing.T) (*tabletenv.TabletConfig, func())
	}{
		{
			name:      "Success without any wait",
			waitTime:  1 * time.Second,
			errWanted: "",
			setupFunc: func(t *testing.T) (*tabletenv.TabletConfig, func()) {
				// Create new mysql instance and the dba user with required grants.
				// Since all the grants already exist, this should pass without any waiting to be needed.
				connParams, _, cleanup, mysqlDir, err := utils.NewMySQLWithMysqld(rand.Intn(10000), "localhost", "ks",
					"CREATE USER 'vt_dba'@'localhost';",
					"GRANT ALL ON *.* TO 'vt_dba'@'localhost';",
					"GRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';",
					"FLUSH PRIVILEGES;",
				)
				require.NoError(t, err)
				tc := &tabletenv.TabletConfig{
					DB: &dbconfigs.DBConfigs{},
				}
				connParams.Uname = "vt_dba"
				tc.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})
				return tc, func() {
					cleanup()
					os.RemoveAll(mysqlDir)
				}
			},
		},
		{
			name:      "Success with wait",
			waitTime:  1 * time.Second,
			errWanted: "",
			setupFunc: func(t *testing.T) (*tabletenv.TabletConfig, func()) {
				// Create a new mysql instance, but delay granting the privileges to the dba user.
				// This makes the waitForDBAGrants function retry the grant check.
				connParams, _, cleanup, mysqlDir, err := utils.NewMySQLWithMysqld(rand.Intn(10000), "localhost", "ks",
					"CREATE USER 'vt_dba'@'localhost';",
				)
				cpToUse := connParams
				go func() {
					conn, err := mysql.Connect(context.Background(), &cpToUse)
					require.NoError(t, err)
					time.Sleep(500 * time.Millisecond)
					_, err = conn.ExecuteFetch("GRANT ALL ON *.* TO 'vt_dba'@'localhost';", 1000, false)
					require.NoError(t, err)
					_, err = conn.ExecuteFetch("GRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';", 1000, false)
					require.NoError(t, err)
					_, err = conn.ExecuteFetch("FLUSH PRIVILEGES;", 1000, false)
					require.NoError(t, err)
				}()
				require.NoError(t, err)
				tc := &tabletenv.TabletConfig{
					DB: &dbconfigs.DBConfigs{},
				}
				connParams.Uname = "vt_dba"
				tc.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})
				return tc, func() {
					cleanup()
					os.RemoveAll(mysqlDir)
				}
			},
		}, {
			name:      "Failure due to timeout",
			waitTime:  300 * time.Millisecond,
			errWanted: "waited 300ms for dba user to have the required permissions",
			setupFunc: func(t *testing.T) (*tabletenv.TabletConfig, func()) {
				// Create a new mysql but don't give the grants to the vt_dba user at all.
				// This should cause a timeout after waiting, since the privileges are never granted.
				connParams, _, cleanup, mysqlDir, err := utils.NewMySQLWithMysqld(rand.Intn(10000), "localhost", "ks",
					"CREATE USER 'vt_dba'@'localhost';",
				)
				require.NoError(t, err)
				tc := &tabletenv.TabletConfig{
					DB: &dbconfigs.DBConfigs{},
				}
				connParams.Uname = "vt_dba"
				tc.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})
				return tc, func() {
					cleanup()
					os.RemoveAll(mysqlDir)
				}
			},
		}, {
			name:      "Empty timeout",
			waitTime:  0,
			errWanted: "",
			setupFunc: func(t *testing.T) (*tabletenv.TabletConfig, func()) {
				return nil, func() {}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := tt.setupFunc(t)
			defer cleanup()
			err := waitForDBAGrants(config, tt.waitTime)
			if tt.errWanted == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.errWanted)
			}
		})
	}
}
