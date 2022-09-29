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

	"vitess.io/vitess/go/vt/log"

	"github.com/go-sql-driver/mysql"
	"github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/ssl"
)

const Error3159 = "Error 3159:"
const Error1045 = "Access denied for user"

// Track if a TLS has already been configured for VTOrc
var vtorcTLSConfigured = false

var readInstanceTLSCounter = metrics.NewCounter()
var writeInstanceTLSCounter = metrics.NewCounter()
var readInstanceTLSCacheCounter = metrics.NewCounter()
var writeInstanceTLSCacheCounter = metrics.NewCounter()

func init() {
	_ = metrics.Register("instance_tls.read", readInstanceTLSCounter)
	_ = metrics.Register("instance_tls.write", writeInstanceTLSCounter)
	_ = metrics.Register("instance_tls.read_cache", readInstanceTLSCacheCounter)
	_ = metrics.Register("instance_tls.write_cache", writeInstanceTLSCacheCounter)
}

// Create a TLS configuration from the config supplied CA, Certificate, and Private key.
// Register the TLS config with the mysql drivers as the "vtorc" config
// Modify the supplied URI to call the TLS config
func SetupMySQLVTOrcTLS(uri string) (string, error) {
	if !vtorcTLSConfigured {
		tlsConfig, err := ssl.NewTLSConfig(config.Config.MySQLVTOrcSSLCAFile, !config.Config.MySQLVTOrcSSLSkipVerify)
		// Drop to TLS 1.0 for talking to MySQL
		tlsConfig.MinVersion = tls.VersionTLS10
		if err != nil {
			log.Fatalf("Can't create TLS configuration for VTOrc connection %s: %s", uri, err)
			return "", err
		}
		tlsConfig.InsecureSkipVerify = config.Config.MySQLVTOrcSSLSkipVerify
		if (!config.Config.MySQLVTOrcSSLSkipVerify) &&
			config.Config.MySQLVTOrcSSLCertFile != "" &&
			config.Config.MySQLVTOrcSSLPrivateKeyFile != "" {
			if err = ssl.AppendKeyPair(tlsConfig, config.Config.MySQLVTOrcSSLCertFile, config.Config.MySQLVTOrcSSLPrivateKeyFile); err != nil {
				log.Fatalf("Can't setup TLS key pairs for %s: %s", uri, err)
				return "", err
			}
		}
		if err = mysql.RegisterTLSConfig("vtorc", tlsConfig); err != nil {
			log.Fatalf("Can't register mysql TLS config for vtorc: %s", err)
			return "", err
		}
		vtorcTLSConfigured = true
	}
	return fmt.Sprintf("%s&tls=vtorc", uri), nil
}
