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

package cli

// This plugin imports InitAuthServerVault to register the HashiCorp Vault implementation of AuthServer.

import (
	"time"

	"vitess.io/vitess/go/mysql/vault"
	"vitess.io/vitess/go/vt/vtgate"
)

var (
	vaultAddr             string
	vaultTimeout          time.Duration
	vaultCACert           string
	vaultPath             string
	vaultCacheTTL         time.Duration
	vaultTokenFile        string
	vaultRoleID           string
	vaultRoleSecretIDFile string
	vaultRoleMountPoint   string
)

func init() {
	Main.Flags().StringVar(&vaultAddr, "mysql_auth_vault_addr", "", "URL to Vault server")
	Main.Flags().DurationVar(&vaultTimeout, "mysql_auth_vault_timeout", 10*time.Second, "Timeout for vault API operations")
	Main.Flags().StringVar(&vaultCACert, "mysql_auth_vault_tls_ca", "", "Path to CA PEM for validating Vault server certificate")
	Main.Flags().StringVar(&vaultPath, "mysql_auth_vault_path", "", "Vault path to vtgate credentials JSON blob, e.g.: secret/data/prod/vtgatecreds")
	Main.Flags().DurationVar(&vaultCacheTTL, "mysql_auth_vault_ttl", 30*time.Minute, "How long to cache vtgate credentials from the Vault server")
	Main.Flags().StringVar(&vaultTokenFile, "mysql_auth_vault_tokenfile", "", "Path to file containing Vault auth token; token can also be passed using VAULT_TOKEN environment variable")
	Main.Flags().StringVar(&vaultRoleID, "mysql_auth_vault_roleid", "", "Vault AppRole id; can also be passed using VAULT_ROLEID environment variable")
	Main.Flags().StringVar(&vaultRoleSecretIDFile, "mysql_auth_vault_role_secretidfile", "", "Path to file containing Vault AppRole secret_id; can also be passed using VAULT_SECRETID environment variable")
	Main.Flags().StringVar(&vaultRoleMountPoint, "mysql_auth_vault_role_mountpoint", "approle", "Vault AppRole mountpoint; can also be passed using VAULT_MOUNTPOINT environment variable")

	vtgate.RegisterPluginInitializer(func() {
		vault.InitAuthServerVault(vaultAddr, vaultTimeout, vaultCACert, vaultPath, vaultCacheTTL, vaultTokenFile, vaultRoleID, vaultRoleSecretIDFile, vaultRoleMountPoint)
	})
}
