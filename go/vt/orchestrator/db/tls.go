/*
   Copyright 2014 Outbrain Inc.

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

package db

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/ssl"
)

const Error3159 = "Error 3159:"
const Error1045 = "Access denied for user"

// Track if a TLS has already been configured for topology
var topologyTLSConfigured bool = false

// Track if a TLS has already been configured for Orchestrator
var orchestratorTLSConfigured bool = false

var requireTLSCache *cache.Cache = cache.New(time.Duration(config.Config.TLSCacheTTLFactor*config.Config.InstancePollSeconds)*time.Second, time.Second)

var readInstanceTLSCounter = metrics.NewCounter()
var writeInstanceTLSCounter = metrics.NewCounter()
var readInstanceTLSCacheCounter = metrics.NewCounter()
var writeInstanceTLSCacheCounter = metrics.NewCounter()

func init() {
	metrics.Register("instance_tls.read", readInstanceTLSCounter)
	metrics.Register("instance_tls.write", writeInstanceTLSCounter)
	metrics.Register("instance_tls.read_cache", readInstanceTLSCacheCounter)
	metrics.Register("instance_tls.write_cache", writeInstanceTLSCacheCounter)
}

func requiresTLS(host string, port int, mysql_uri string) bool {
	cacheKey := fmt.Sprintf("%s:%d", host, port)

	if value, found := requireTLSCache.Get(cacheKey); found {
		readInstanceTLSCacheCounter.Inc(1)
		return value.(bool)
	}

	required := false
	db, _, _ := sqlutils.GetDB(mysql_uri)
	if err := db.Ping(); err != nil && (strings.Contains(err.Error(), Error3159) || strings.Contains(err.Error(), Error1045)) {
		required = true
	}

	query := `
			insert into
				database_instance_tls (
					hostname, port, required
				) values (
					?, ?, ?
				)
				on duplicate key update
					required=values(required)
				`
	if _, err := ExecOrchestrator(query, host, port, required); err != nil {
		log.Errore(err)
	}
	writeInstanceTLSCounter.Inc(1)

	requireTLSCache.Set(cacheKey, required, cache.DefaultExpiration)
	writeInstanceTLSCacheCounter.Inc(1)

	return required
}

// Create a TLS configuration from the config supplied CA, Certificate, and Private key.
// Register the TLS config with the mysql drivers as the "topology" config
// Modify the supplied URI to call the TLS config
func SetupMySQLTopologyTLS(uri string) (string, error) {
	if !topologyTLSConfigured {
		tlsConfig, err := ssl.NewTLSConfig(config.Config.MySQLTopologySSLCAFile, !config.Config.MySQLTopologySSLSkipVerify)
		// Drop to TLS 1.0 for talking to MySQL
		tlsConfig.MinVersion = tls.VersionTLS10
		if err != nil {
			return "", log.Errorf("Can't create TLS configuration for Topology connection %s: %s", uri, err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.MySQLTopologySSLSkipVerify

		if (config.Config.MySQLTopologyUseMutualTLS && !config.Config.MySQLTopologySSLSkipVerify) &&
			config.Config.MySQLTopologySSLCertFile != "" &&
			config.Config.MySQLTopologySSLPrivateKeyFile != "" {
			if err = ssl.AppendKeyPair(tlsConfig, config.Config.MySQLTopologySSLCertFile, config.Config.MySQLTopologySSLPrivateKeyFile); err != nil {
				return "", log.Errorf("Can't setup TLS key pairs for %s: %s", uri, err)
			}
		}
		if err = mysql.RegisterTLSConfig("topology", tlsConfig); err != nil {
			return "", log.Errorf("Can't register mysql TLS config for topology: %s", err)
		}
		topologyTLSConfigured = true
	}
	return fmt.Sprintf("%s&tls=topology", uri), nil
}

// Create a TLS configuration from the config supplied CA, Certificate, and Private key.
// Register the TLS config with the mysql drivers as the "orchestrator" config
// Modify the supplied URI to call the TLS config
func SetupMySQLOrchestratorTLS(uri string) (string, error) {
	if !orchestratorTLSConfigured {
		tlsConfig, err := ssl.NewTLSConfig(config.Config.MySQLOrchestratorSSLCAFile, !config.Config.MySQLOrchestratorSSLSkipVerify)
		// Drop to TLS 1.0 for talking to MySQL
		tlsConfig.MinVersion = tls.VersionTLS10
		if err != nil {
			return "", log.Fatalf("Can't create TLS configuration for Orchestrator connection %s: %s", uri, err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.MySQLOrchestratorSSLSkipVerify
		if (!config.Config.MySQLOrchestratorSSLSkipVerify) &&
			config.Config.MySQLOrchestratorSSLCertFile != "" &&
			config.Config.MySQLOrchestratorSSLPrivateKeyFile != "" {
			if err = ssl.AppendKeyPair(tlsConfig, config.Config.MySQLOrchestratorSSLCertFile, config.Config.MySQLOrchestratorSSLPrivateKeyFile); err != nil {
				return "", log.Fatalf("Can't setup TLS key pairs for %s: %s", uri, err)
			}
		}
		if err = mysql.RegisterTLSConfig("orchestrator", tlsConfig); err != nil {
			return "", log.Fatalf("Can't register mysql TLS config for orchestrator: %s", err)
		}
		orchestratorTLSConfigured = true
	}
	return fmt.Sprintf("%s&tls=orchestrator", uri), nil
}
