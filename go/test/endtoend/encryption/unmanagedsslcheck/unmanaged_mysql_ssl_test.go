/*
Copyright 2025 The Vitess Authors.

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

package unmanagedsslcheck

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"
	"vitess.io/vitess/go/vt/vttls"
)

func TestConnectUnamanagedMySQLRequiringSSLSuccess(t *testing.T) {
	testUser := "vt_test_user"
	cluster, cleanup, err := startMySQLWithSSLRequired(t, testUser)
	require.NoError(t, err)
	defer cleanup()

	tc1 := tabletenv.NewDefaultConfig()
	tc1.Unmanaged = true
	tc1.DB = &dbconfigs.DBConfigs{}

	connParams := cluster.MySQLTCPConnParams()
	connParams.Uname = testUser
	connParams.Pass = "password"
	tc1.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})

	tc1.DB.Host = "127.0.0.1"
	tc1.DB.Port = connParams.Port
	tc1.DB.Socket = ""
	tc1.DB.DBName = connParams.DbName
	tc1.DB.App.User = testUser
	tc1.DB.App.Password = "password"
	tc1.DB.SslMode = vttls.Required
	tc1.DB.SslCa = cluster.MySQLTCPConnParams().SslCa
	tc1.DB.SslCert = cluster.MySQLTCPConnParams().SslCert
	tc1.DB.SslKey = cluster.MySQLTCPConnParams().SslKey

	err = tc1.Verify()
	require.NoError(t, err)
}

func startMySQLWithSSLRequired(t *testing.T, testUser string) (vttest.LocalCluster, func(), error) {
	certDir := t.TempDir()
	caFile, serverCertFile, serverKeyFile := createTestSSLCerts(t, certDir)

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
		ExtraMyCnf: []string{
			"[mysqld]",
			"require_secure_transport=ON",
			"ssl-ca=" + caFile,
			"ssl-cert=" + serverCertFile,
			"ssl-key=" + serverKeyFile,
		},
	}

	// Create a temporary .cnf file with the SSL configuration if provided
	var tempCnfFilePath string
	if len(cfg.ExtraMyCnf) > 0 {
		tempCnfFile, cnfErr := os.CreateTemp(t.TempDir(), "mysql_ssl_config-*.cnf")
		require.NoError(t, cnfErr)
		defer tempCnfFile.Close()

		for _, line := range cfg.ExtraMyCnf {
			_, cnfErr = tempCnfFile.WriteString(line + "\n")
			require.NoError(t, cnfErr)
		}
		require.NoError(t, tempCnfFile.Sync())

		tempCnfFilePath = tempCnfFile.Name()
		cfg.ExtraMyCnf = append(cfg.ExtraMyCnf, tempCnfFilePath)
	}

	cluster := vttest.LocalCluster{
		Config: cfg,
	}
	err := cluster.Setup()
	if err != nil {
		return cluster, nil, err
	}

	cleanup := func() {
		cluster.TearDown()
		if tempCnfFilePath != "" {
			_ = os.Remove(tempCnfFilePath)
		}
	}

	connParams := cluster.MySQLConnParams()
	conn, err := mysql.Connect(context.Background(), &connParams)
	if err != nil {
		cleanup()
		return cluster, cleanup, err
	}
	defer conn.Close()

	_, err = conn.ExecuteFetch(fmt.Sprintf(`CREATE USER '%v'@'%%' IDENTIFIED WITH mysql_native_password BY 'password'`, testUser), 1000, false)
	if err != nil {
		cleanup()
		return cluster, cleanup, err
	}

	_, err = conn.ExecuteFetch(fmt.Sprintf(`GRANT ALL ON *.* TO '%v'@'%%'`, testUser), 1000, false)
	if err != nil {
		cleanup()
		return cluster, cleanup, err
	}

	return cluster, cleanup, nil
}

func createTestSSLCerts(t *testing.T, certDir string) (caFile, certFile, keyFile string) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Write CA certificate
	caFile = filepath.Join(certDir, "ca.pem")
	caCertOut, err := os.Create(caFile)
	require.NoError(t, err)
	defer caCertOut.Close()

	err = pem.Encode(caCertOut, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	require.NoError(t, err)

	// Generate client certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Write client certificate
	certFile = filepath.Join(certDir, "client.pem")
	certOut, err := os.Create(certFile)
	require.NoError(t, err)
	defer certOut.Close()

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	require.NoError(t, err)

	// Write client key
	keyFile = filepath.Join(certDir, "client-key.pem")
	keyOut, err := os.Create(keyFile)
	require.NoError(t, err)
	defer keyOut.Close()

	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey)})
	require.NoError(t, err)

	return caFile, certFile, keyFile
}
