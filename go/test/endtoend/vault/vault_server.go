/*
Copyright 2020 The Vitess Authors.

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

package vault

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	vaultImage        = "vault:1.6.1"
	vaultNetworkAlias = "vault"
	vaultPort         = "8200/tcp"

	// Staged file paths inside the Vault container.
	vaultStageDir       = "/tmp"
	vaultConfigPath     = vaultStageDir + "/vault.hcl"
	vaultCACertPath     = vaultStageDir + "/ca.pem"
	vaultCertPath       = vaultStageDir + "/vault-cert.pem"
	vaultKeyPath        = vaultStageDir + "/vault-key.pem"
	vaultPolicyPath     = vaultStageDir + "/dbcreds_policy.hcl"
	vaultDBSecretPath   = vaultStageDir + "/dbcreds_secret.json"
	vaultGateSecretPath = vaultStageDir + "/vtgatecreds_secret.json"

	// File names tlstest gives the CA and the server certificate it mints for
	// the "vault" hostname.
	caCertFile     = "ca-cert.pem"
	serverCertFile = vaultNetworkAlias + "-cert.pem"
	serverKeyFile  = vaultNetworkAlias + "-key.pem"

	// vaultLocalAddr reaches the server over its loopback interface from
	// inside the container, where the server certificate is valid.
	vaultLocalAddr = "https://127.0.0.1:8200"

	vaultStartupTimeout = 60 * time.Second
)

// vaultConfig is the server configuration: a TLS listener on all interfaces
// backed by in-memory storage.
const vaultConfig = `{
    "ui": "true",
    "disable_mlock": "true",
    "listener": [
        {
            "tcp": {
                "address": "0.0.0.0:8200",
                "tls_disable": 0,
                "tls_cert_file": "` + vaultCertPath + `",
                "tls_key_file": "` + vaultKeyPath + `"
            }
        }
    ],
    "storage": [
        {
            "inmem": {}
        }
    ],
    "default_lease_ttl": "168h",
    "max_lease_ttl": "720h"
}`

// Server is a running Vault container and the AppRole credentials minted
// against it.
type Server struct {
	container testcontainers.Container
	roleID    string
	secretID  string
}

// startVaultServer runs a Vault container on the given network, initializes and
// unseals it, loads the DB and vtgate credential secrets, and mints an AppRole
// role_id and secret_id for Vitess to authenticate with. certDir holds the CA
// and the server certificate the container serves TLS with.
func startVaultServer(ctx context.Context, nw *testcontainers.DockerNetwork, certDir string) (*Server, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, vterrors.Wrapf(err, "resolving working directory")
	}

	files := []testcontainers.ContainerFile{
		{Reader: strings.NewReader(vaultConfig), ContainerFilePath: vaultConfigPath, FileMode: 0o644},
		{HostFilePath: filepath.Join(certDir, caCertFile), ContainerFilePath: vaultCACertPath, FileMode: 0o644},
		{HostFilePath: filepath.Join(certDir, serverCertFile), ContainerFilePath: vaultCertPath, FileMode: 0o644},
		{HostFilePath: filepath.Join(certDir, serverKeyFile), ContainerFilePath: vaultKeyPath, FileMode: 0o644},
		{HostFilePath: filepath.Join(wd, "dbcreds_policy.hcl"), ContainerFilePath: vaultPolicyPath, FileMode: 0o644},
		{HostFilePath: filepath.Join(wd, "dbcreds_secret.json"), ContainerFilePath: vaultDBSecretPath, FileMode: 0o644},
		{HostFilePath: filepath.Join(wd, "vtgatecreds_secret.json"), ContainerFilePath: vaultGateSecretPath, FileMode: 0o644},
	}

	ctr, err := testcontainers.Run(ctx, vaultImage,
		testcontainers.WithEntrypoint("vault", "server", "-config="+vaultConfigPath),
		testcontainers.WithExposedPorts(vaultPort),
		network.WithNetwork([]string{vaultNetworkAlias}, nw),
		testcontainers.WithFiles(files...),
		testcontainers.WithWaitStrategyAndDeadline(vaultStartupTimeout,
			wait.ForListeningPort(vaultPort).WithStartupTimeout(vaultStartupTimeout),
		),
	)
	if err != nil {
		return nil, vterrors.Wrapf(err, "starting vault container")
	}

	vs := &Server{container: ctr}
	if err := vs.setup(ctx); err != nil {
		if stopErr := vs.stop(ctx); stopErr != nil {
			return nil, vterrors.Wrapf(err, "setting up vault (cleanup also failed: %v)", stopErr)
		}
		return nil, err
	}
	return vs, nil
}

// setup initializes and unseals Vault, enables the secret and auth engines,
// loads the credential secrets, and reads back the AppRole role_id/secret_id.
func (vs *Server) setup(ctx context.Context) error {
	baseEnv := []string{"VAULT_ADDR=" + vaultLocalAddr, "VAULT_CACERT=" + vaultCACertPath}

	initOut, err := vs.exec(ctx, baseEnv, "operator", "init", "-key-shares=1", "-key-threshold=1")
	if err != nil {
		return err
	}
	unsealKey := colonValue(initOut, "Unseal Key 1:")
	rootToken := colonValue(initOut, "Initial Root Token:")
	if unsealKey == "" || rootToken == "" {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "could not parse vault init output: %s", initOut)
	}

	if _, err := vs.exec(ctx, baseEnv, "operator", "unseal", unsealKey); err != nil {
		return err
	}

	tokenEnv := []string{"VAULT_ADDR=" + vaultLocalAddr, "VAULT_CACERT=" + vaultCACertPath, "VAULT_TOKEN=" + rootToken}

	steps := [][]string{
		{"secrets", "enable", "-version=2", "kv"},
		{"auth", "enable", "approle"},
		{"policy", "write", "dbcreds", vaultPolicyPath},
		{"kv", "put", "kv/prod/dbcreds", "@" + vaultDBSecretPath},
		{"kv", "put", "kv/prod/vtgatecreds", "@" + vaultGateSecretPath},
		{
			"write", "auth/approle/role/vitess",
			"secret_id_ttl=10m", "token_num_uses=0", "token_ttl=30s",
			"token_max_ttl=0", "secret_id_num_uses=4", "policies=dbcreds",
		},
	}
	for _, args := range steps {
		if _, err := vs.exec(ctx, tokenEnv, args...); err != nil {
			return err
		}
	}

	roleOut, err := vs.exec(ctx, tokenEnv, "read", "auth/approle/role/vitess/role-id")
	if err != nil {
		return err
	}
	secretOut, err := vs.exec(ctx, tokenEnv, "write", "auth/approle/role/vitess/secret-id", "k=v")
	if err != nil {
		return err
	}

	vs.roleID = fieldValue(roleOut, "role_id")
	vs.secretID = fieldValue(secretOut, "secret_id")
	if vs.roleID == "" || vs.secretID == "" {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "could not parse role_id/secret_id from vault output: %s\n%s", roleOut, secretOut)
	}
	return nil
}

// exec runs the vault CLI inside the container with the given environment and
// returns its combined output.
func (vs *Server) exec(ctx context.Context, env []string, args ...string) (string, error) {
	cmd := append([]string{"vault"}, args...)
	exitCode, reader, err := vs.container.Exec(ctx, cmd, tcexec.Multiplexed(), tcexec.WithEnv(env))
	if err != nil {
		return "", vterrors.Wrapf(err, "exec vault %s", strings.Join(args, " "))
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		return "", vterrors.Wrapf(err, "reading output of vault %s", strings.Join(args, " "))
	}
	if exitCode != 0 {
		return string(output), vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vault %s failed with exit code %d: %s", strings.Join(args, " "), exitCode, output)
	}
	return string(output), nil
}

// stop terminates the Vault container.
func (vs *Server) stop(ctx context.Context) error {
	if vs.container == nil {
		return nil
	}
	return testcontainers.TerminateContainer(vs.container, testcontainers.StopContext(ctx), testcontainers.StopTimeout(10*time.Second))
}

// colonValue returns the last whitespace-separated field of the first line
// containing prefix, e.g. the key in "Unseal Key 1: <value>".
func colonValue(output, prefix string) string {
	for line := range strings.SplitSeq(output, "\n") {
		if strings.Contains(line, prefix) {
			fields := strings.Fields(line)
			if len(fields) > 0 {
				return fields[len(fields)-1]
			}
		}
	}
	return ""
}

// fieldValue returns the last field of the first line whose first field equals
// key, matching how the Vault CLI table output pairs a key with its value.
func fieldValue(output, key string) string {
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == key {
			return fields[len(fields)-1]
		}
	}
	return ""
}
