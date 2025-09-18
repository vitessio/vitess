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
	"vitess.io/vitess/go/vt/utils"
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
	utils.SetFlagStringVar(Main.Flags(), &vaultAddr, "mysql-auth-vault-addr", "", "URL to Vault server")
	utils.SetFlagDurationVar(Main.Flags(), &vaultTimeout, "mysql-auth-vault-timeout", 10*time.Second, "Timeout for vault API operations")
	utils.SetFlagStringVar(Main.Flags(), &vaultCACert, "mysql-auth-vault-tls-ca", "", "Path to CA PEM for validating Vault server certificate")
	utils.SetFlagStringVar(Main.Flags(), &vaultPath, "mysql-auth-vault-path", "", "Vault path to vtgate credentials JSON blob, e.g.: secret/data/prod/vtgatecreds")
	utils.SetFlagDurationVar(Main.Flags(), &vaultCacheTTL, "mysql-auth-vault-ttl", 30*time.Minute, "How long to cache vtgate credentials from the Vault server")
	utils.SetFlagStringVar(Main.Flags(), &vaultTokenFile, "mysql-auth-vault-tokenfile", "", "Path to file containing Vault auth token; token can also be passed using VAULT_TOKEN environment variable")
	utils.SetFlagStringVar(Main.Flags(), &vaultRoleID, "mysql-auth-vault-roleid", "", "Vault AppRole id; can also be passed using VAULT_ROLEID environment variable")
	utils.SetFlagStringVar(Main.Flags(), &vaultRoleSecretIDFile, "mysql-auth-vault-role-secretidfile", "", "Path to file containing Vault AppRole secret_id; can also be passed using VAULT_SECRETID environment variable")
	utils.SetFlagStringVar(Main.Flags(), &vaultRoleMountPoint, "mysql-auth-vault-role-mountpoint", "approle", "Vault AppRole mountpoint; can also be passed using VAULT_MOUNTPOINT environment variable")

	vtgate.RegisterPluginInitializer(func() {
		vault.InitAuthServerVault(vaultAddr, vaultTimeout, vaultCACert, vaultPath, vaultCacheTTL, vaultTokenFile, vaultRoleID, vaultRoleSecretIDFile, vaultRoleMountPoint)
	})
}
