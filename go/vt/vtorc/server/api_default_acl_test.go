/*
Copyright 2026 The Vitess Authors.

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

package server

import (
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
	"vitess.io/vitess/go/vt/vtorc/process"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	aclLeakHostname = "vtorc-acl-leak-marker.example"
	aclLeakGTID     = "vtorc-acl-leak-marker-gtid:1-9"
	aclLeakBinlog   = "vt-acl-marker-bin.000009"
)

// TestDefaultSecurityPolicyDeniesVTOrcHTTPAPI is the spec for the default VTOrc
// HTTP policy. An unauthenticated client that can reach the listener started the
// way Run starts it (empty bind address, port 0) must be refused for every
// registered API, and the refusal must happen before any handler side effect:
// global recoveries stay as the test left them, node_health is not rewritten,
// and the orchestrator snapshot and process settings are not returned.
func TestDefaultSecurityPolicyDeniesVTOrcHTTPAPI(t *testing.T) {
	_, err := db.OpenVTOrc()
	require.NoError(t, err)
	db.ClearVTOrcDatabase()
	t.Cleanup(func() { db.ClearVTOrcDatabase() })

	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 999},
		Hostname:      aclLeakHostname,
		MysqlHostname: aclLeakHostname,
		MysqlPort:     3306,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
	}))
	_, err = db.ExecVTOrc(`INSERT INTO database_instance VALUES('zone1-0000000999','` + aclLeakHostname + `',3306,2,'zone1','2022-12-28 07:26:04','2022-12-28 07:26:04',213696377,'8.0.31','ROW',1,1,'` + aclLeakBinlog + `',4242,'localhost',6714,8,4.0,1,1,'vt-0000000101-bin.000001',15583,'vt-0000000101-bin.000001',15583,0,0,1,'','',1,'vt-0000000112-relay-bin.000002',15815,1,0,0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a5138-8680-11ed-9240-92a06c3be3c2','2022-12-28 07:26:04','',1,0,0,'Homebrew','8.0','FULL',10816929,0,0,'ON',1,'729a4cc4-8680-11ed-a104-47706090afbd','` + aclLeakGTID + `','729a4cc4-8680-11ed-a104-47706090afbd,729a5138-8680-11ed-9240-92a06c3be3c2',1,1,1000000000000000000,1,0,0,0,false,false)`)
	require.NoError(t, err)

	// Same address Run passes through when --bind-address and --port are left at their zero values.
	listener, err := servenv.Listen("tcp", net.JoinHostPort("", strconv.Itoa(0)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.True(t, tcpAddr.IP.IsUnspecified(), "default listener %s is not a wildcard address", tcpAddr)
	require.NotZero(t, tcpAddr.Port)

	registerVTOrcAPIOnce.Do(RegisterVTOrcAPIEndpoints)
	go func() { _ = servenv.HTTPServe(listener) }()

	client := &http.Client{Timeout: 30 * time.Second}
	baseURL := "http://" + listener.Addr().String()

	// ServeHTTP does not look at the method, so a refusal that only filters GET
	// would still let POST turn recoveries off or on.
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method+" "+disableGlobalRecoveriesAPI, func(t *testing.T) {
			require.NoError(t, logic.EnableRecovery())
			status, body := callVTOrcAPI(t, client, method, baseURL+disableGlobalRecoveriesAPI)
			assert.Equal(t, http.StatusForbidden, status, "body: %s", body)
			assert.NotContains(t, body, "Global recoveries disabled")
			disabled, err := logic.IsRecoveryDisabled()
			require.NoError(t, err)
			require.False(t, disabled, "disable-global-recoveries changed recovery state")
		})

		t.Run(method+" "+enableGlobalRecoveriesAPI, func(t *testing.T) {
			require.NoError(t, logic.DisableRecovery())
			status, body := callVTOrcAPI(t, client, method, baseURL+enableGlobalRecoveriesAPI)
			assert.Equal(t, http.StatusForbidden, status, "body: %s", body)
			assert.NotContains(t, body, "Global recoveries enabled")
			disabled, err := logic.IsRecoveryDisabled()
			require.NoError(t, err)
			require.True(t, disabled, "enable-global-recoveries changed recovery state")
		})
	}

	t.Run(healthAPI, func(t *testing.T) {
		process.FirstDiscoveryCycleComplete.Store(true)
		t.Cleanup(func() { process.FirstDiscoveryCycleComplete.Store(false) })
		before := countRows(t, "node_health")
		status, body := callVTOrcAPI(t, client, http.MethodGet, baseURL+healthAPI)
		assert.Equal(t, http.StatusForbidden, status, "body: %s", body)
		assert.Equal(t, before, countRows(t, "node_health"), "health API wrote node_health")
	})

	for _, path := range []string{problemsAPI, errantGTIDsAPI, detectionAnalysisAPI, databaseStateAPI, configAPI, shardQuorumAPI} {
		t.Run(path, func(t *testing.T) {
			status, body := callVTOrcAPI(t, client, http.MethodGet, baseURL+path)
			assert.Equal(t, http.StatusForbidden, status, "body: %s", body)
			assert.NotContains(t, body, aclLeakHostname)
			assert.NotContains(t, body, aclLeakGTID)
			assert.NotContains(t, body, aclLeakBinlog)
			if path == configAPI {
				assert.NotContains(t, body, "sqlite-data-file")
			}
		})
	}
}

var registerVTOrcAPIOnce sync.Once

func callVTOrcAPI(t *testing.T, client *http.Client, method, rawURL string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, rawURL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func countRows(t *testing.T, table string) int {
	t.Helper()
	var n int
	err := db.QueryVTOrc("SELECT COUNT(*) AS n FROM "+table, nil, func(row sqlutils.RowMap) error {
		n = row.GetInt("n")
		return nil
	})
	require.NoError(t, err)
	return n
}
