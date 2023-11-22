package cli

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"
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
				// Create a new mysql instance, and the dba user with required grants.
				// Since all the grants already exist, this should pass without any waiting to be needed.
				testUser := "vt_test_dba"
				cluster, err := startMySQLAndCreateUser(t, testUser)
				require.NoError(t, err)
				grantAllPrivilegesToUser(t, cluster.MySQLConnParams(), testUser)
				tc := &tabletenv.TabletConfig{
					DB: &dbconfigs.DBConfigs{},
				}
				connParams := cluster.MySQLConnParams()
				connParams.Uname = testUser
				tc.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})
				return tc, func() {
					cluster.TearDown()
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
				testUser := "vt_test_dba"
				cluster, err := startMySQLAndCreateUser(t, testUser)
				require.NoError(t, err)

				go func() {
					time.Sleep(500 * time.Millisecond)
					grantAllPrivilegesToUser(t, cluster.MySQLConnParams(), testUser)
				}()

				tc := &tabletenv.TabletConfig{
					DB: &dbconfigs.DBConfigs{},
				}
				connParams := cluster.MySQLConnParams()
				connParams.Uname = testUser
				tc.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})
				return tc, func() {
					cluster.TearDown()
				}
			},
		}, {
			name:      "Failure due to timeout",
			waitTime:  300 * time.Millisecond,
			errWanted: "waited 300ms for dba user to have the required permissions",
			setupFunc: func(t *testing.T) (*tabletenv.TabletConfig, func()) {
				// Create a new mysql but don't give the grants to the vt_dba user at all.
				// This should cause a timeout after waiting, since the privileges are never granted.
				testUser := "vt_test_dba"
				cluster, err := startMySQLAndCreateUser(t, testUser)
				require.NoError(t, err)

				tc := &tabletenv.TabletConfig{
					DB: &dbconfigs.DBConfigs{},
				}
				connParams := cluster.MySQLConnParams()
				connParams.Uname = testUser
				tc.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})
				return tc, func() {
					cluster.TearDown()
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

// startMySQLAndCreateUser starts a MySQL instance and creates the given user
func startMySQLAndCreateUser(t *testing.T, testUser string) (vttest.LocalCluster, error) {
	// Launch MySQL.
	// We need a Keyspace in the topology, so the DbName is set.
	// We need a Shard too, so the database 'vttest' is created.
	cfg := vttest.Config{
		Topology: &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{
				{
					Name: "vttest",
					Shards: []*vttestpb.Shard{
						{
							Name:           "0",
							DbNameOverride: "vttest",
						},
					},
				},
			},
		},
		OnlyMySQL: true,
		Charset:   "utf8mb4",
	}
	cluster := vttest.LocalCluster{
		Config: cfg,
	}
	err := cluster.Setup()
	if err != nil {
		return cluster, nil
	}

	connParams := cluster.MySQLConnParams()
	conn, err := mysql.Connect(context.Background(), &connParams)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch(fmt.Sprintf(`CREATE USER '%v'@'localhost';`, testUser), 1000, false)
	conn.Close()

	return cluster, err
}

// grantAllPrivilegesToUser grants all the privileges to the user specified.
func grantAllPrivilegesToUser(t *testing.T, connParams mysql.ConnParams, testUser string) {
	conn, err := mysql.Connect(context.Background(), &connParams)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch(fmt.Sprintf(`GRANT ALL ON *.* TO '%v'@'localhost';`, testUser), 1000, false)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch(fmt.Sprintf(`GRANT GRANT OPTION ON *.* TO '%v'@'localhost';`, testUser), 1000, false)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch("FLUSH PRIVILEGES;", 1000, false)
	require.NoError(t, err)
	conn.Close()
}
