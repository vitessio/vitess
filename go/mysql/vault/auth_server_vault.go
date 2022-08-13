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
	"crypto/subtle"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	vaultapi "github.com/aquarapid/vaultlib"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
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
	servenv.OnParseFor("vtgate", func(fs *pflag.FlagSet) {
		fs.StringVar(&vaultAddr, "mysql_auth_vault_addr", "", "URL to Vault server")
		fs.DurationVar(&vaultTimeout, "mysql_auth_vault_timeout", 10*time.Second, "Timeout for vault API operations")
		fs.StringVar(&vaultCACert, "mysql_auth_vault_tls_ca", "", "Path to CA PEM for validating Vault server certificate")
		fs.StringVar(&vaultPath, "mysql_auth_vault_path", "", "Vault path to vtgate credentials JSON blob, e.g.: secret/data/prod/vtgatecreds")
		fs.DurationVar(&vaultCacheTTL, "mysql_auth_vault_ttl", 30*time.Minute, "How long to cache vtgate credentials from the Vault server")
		fs.StringVar(&vaultTokenFile, "mysql_auth_vault_tokenfile", "", "Path to file containing Vault auth token; token can also be passed using VAULT_TOKEN environment variable")
		fs.StringVar(&vaultRoleID, "mysql_auth_vault_roleid", "", "Vault AppRole id; can also be passed using VAULT_ROLEID environment variable")
		fs.StringVar(&vaultRoleSecretIDFile, "mysql_auth_vault_role_secretidfile", "", "Path to file containing Vault AppRole secret_id; can also be passed using VAULT_SECRETID environment variable")
		fs.StringVar(&vaultRoleMountPoint, "mysql_auth_vault_role_mountpoint", "approle", "Vault AppRole mountpoint; can also be passed using VAULT_MOUNTPOINT environment variable")
	})
}

// AuthServerVault implements AuthServer with a config loaded from Vault.
type AuthServerVault struct {
	methods []mysql.AuthMethod
	mu      sync.Mutex
	// users, passwords and user data
	// We use the same JSON format as for --mysql_auth_server_static
	// Acts as a cache for the in-Vault data
	entries                map[string][]*mysql.AuthServerStaticEntry
	vaultCacheExpireTicker *time.Ticker
	vaultClient            *vaultapi.Client
	vaultPath              string
	vaultTTL               time.Duration

	sigChan chan os.Signal
}

// InitAuthServerVault - entrypoint for initialization of Vault AuthServer implementation
func InitAuthServerVault() {
	// Check critical parameters.
	if vaultAddr == "" {
		log.Infof("Not configuring AuthServerVault, as --mysql_auth_vault_addr is empty.")
		return
	}
	if vaultPath == "" {
		log.Exitf("If using Vault auth server, --mysql_auth_vault_path is required.")
	}

	registerAuthServerVault(vaultAddr, vaultTimeout, vaultCACert, vaultPath, vaultCacheTTL, vaultTokenFile, vaultRoleID, vaultRoleSecretIDFile, vaultRoleMountPoint)
}

func registerAuthServerVault(addr string, timeout time.Duration, caCertPath string, path string, ttl time.Duration, tokenFilePath string, roleID string, secretIDPath string, roleMountPoint string) {
	authServerVault, err := newAuthServerVault(addr, timeout, caCertPath, path, ttl, tokenFilePath, roleID, secretIDPath, roleMountPoint)
	if err != nil {
		log.Exitf("%s", err)
	}
	mysql.RegisterAuthServer("vault", authServerVault)
}

func newAuthServerVault(addr string, timeout time.Duration, caCertPath string, path string, ttl time.Duration, tokenFilePath string, roleID string, secretIDPath string, roleMountPoint string) (*AuthServerVault, error) {
	// Validate more parameters
	token, err := readFromFile(tokenFilePath)
	if err != nil {
		return nil, fmt.Errorf("No Vault token in provided filename for --mysql_auth_vault_tokenfile")
	}
	secretID, err := readFromFile(secretIDPath)
	if err != nil {
		return nil, fmt.Errorf("No Vault secret_id in provided filename for --mysql_auth_vault_role_secretidfile")
	}

	config := vaultapi.NewConfig()

	// All these can be overriden by environment
	//   so we need to check if they have been set by NewConfig
	if config.Address == "" {
		config.Address = addr
	}
	if config.Timeout == (0 * time.Second) {
		config.Timeout = timeout
	}
	if config.CACert == "" {
		config.CACert = caCertPath
	}
	if config.Token == "" {
		config.Token = token
	}
	if config.AppRoleCredentials.RoleID == "" {
		config.AppRoleCredentials.RoleID = roleID
	}
	if config.AppRoleCredentials.SecretID == "" {
		config.AppRoleCredentials.SecretID = secretID
	}
	if config.AppRoleCredentials.MountPoint == "" {
		config.AppRoleCredentials.MountPoint = roleMountPoint
	}

	if config.CACert != "" {
		// If we provide a CA, ensure we actually use it
		config.InsecureSSL = false
	}

	client, err := vaultapi.NewClient(config)
	if err != nil || client == nil {
		log.Errorf("Error in vault client initialization, will retry: %v", err)
	}

	a := &AuthServerVault{
		vaultClient: client,
		vaultPath:   path,
		vaultTTL:    ttl,
		entries:     make(map[string][]*mysql.AuthServerStaticEntry),
	}

	authMethodNative := mysql.NewMysqlNativeAuthMethod(a, a)
	a.methods = []mysql.AuthMethod{authMethodNative}

	a.reloadVault()
	a.installSignalHandlers()
	return a, nil
}

// AuthMethods returns the list of registered auth methods
// implemented by this auth server.
func (a *AuthServerVault) AuthMethods() []mysql.AuthMethod {
	return a.methods
}

// DefaultAuthMethodDescription returns MysqlNativePassword as the default
// authentication method for the auth server implementation.
func (a *AuthServerVault) DefaultAuthMethodDescription() mysql.AuthMethodDescription {
	return mysql.MysqlNativePassword
}

// HandleUser is part of the Validator interface. We
// handle any user here since we don't check up front.
func (a *AuthServerVault) HandleUser(user string) bool {
	return true
}

// UserEntryWithHash is called when mysql_native_password is used.
func (a *AuthServerVault) UserEntryWithHash(conn *mysql.Conn, salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (mysql.Getter, error) {
	a.mu.Lock()
	userEntries, ok := a.entries[user]
	a.mu.Unlock()

	if !ok {
		return &mysql.StaticUserData{}, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	for _, entry := range userEntries {
		if entry.MysqlNativePassword != "" {
			hash, err := mysql.DecodeMysqlNativePasswordHex(entry.MysqlNativePassword)
			if err != nil {
				return &mysql.StaticUserData{Username: entry.UserData, Groups: entry.Groups}, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", user)
			}
			isPass := mysql.VerifyHashedMysqlNativePassword(authResponse, salt, hash)
			if mysql.MatchSourceHost(remoteAddr, entry.SourceHost) && isPass {
				return &mysql.StaticUserData{Username: entry.UserData, Groups: entry.Groups}, nil
			}
		} else {
			computedAuthResponse := mysql.ScrambleMysqlNativePassword(salt, []byte(entry.Password))
			// Validate the password.
			if mysql.MatchSourceHost(remoteAddr, entry.SourceHost) && subtle.ConstantTimeCompare(authResponse, computedAuthResponse) == 1 {
				return &mysql.StaticUserData{Username: entry.UserData, Groups: entry.Groups}, nil
			}
		}
	}
	return &mysql.StaticUserData{}, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", user)
}

func (a *AuthServerVault) setTTLTicker(ttl time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.vaultCacheExpireTicker == nil {
		a.vaultCacheExpireTicker = time.NewTicker(ttl)
		go func() {
			for range a.vaultCacheExpireTicker.C {
				a.sigChan <- syscall.SIGHUP
			}
		}()
	} else {
		a.vaultCacheExpireTicker.Reset(ttl)
	}
}

// Reload JSON auth key from Vault. Return true if successful, false if not
func (a *AuthServerVault) reloadVault() error {
	a.mu.Lock()
	secret, err := a.vaultClient.GetSecret(a.vaultPath)
	a.mu.Unlock()
	a.setTTLTicker(10 * time.Second) // Reload frequently on error

	if err != nil {
		return fmt.Errorf("Error in vtgate Vault auth server params: %v", err)
	}

	if secret.JSONSecret == nil {
		return fmt.Errorf("Empty vtgate credentials retrieved from Vault server")
	}

	entries := make(map[string][]*mysql.AuthServerStaticEntry)
	if err := mysql.ParseConfig(secret.JSONSecret, &entries); err != nil {
		return fmt.Errorf("Error parsing vtgate Vault auth server config: %v", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("vtgate credentials from Vault empty! Not updating previously cached values")
	}

	log.Infof("reloadVault(): success. Client status: %s", a.vaultClient.GetStatus())
	a.mu.Lock()
	a.entries = entries
	a.mu.Unlock()
	a.setTTLTicker(a.vaultTTL)
	return nil
}

func (a *AuthServerVault) installSignalHandlers() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.sigChan = make(chan os.Signal, 1)
	signal.Notify(a.sigChan, syscall.SIGHUP)
	go func() {
		for range a.sigChan {
			err := a.reloadVault()
			if err != nil {
				log.Errorf("%s", err)
			}

		}
	}()
}

func (a *AuthServerVault) close() {
	log.Warningf("Closing AuthServerVault instance.")
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.vaultCacheExpireTicker != nil {
		a.vaultCacheExpireTicker.Stop()
	}
	if a.sigChan != nil {
		signal.Stop(a.sigChan)
	}
}

// We ignore most errors here, to allow us to retry cleanly
//
//	or ignore the cases where the input is not passed by file, but via env
func readFromFile(filePath string) (string, error) {
	if filePath == "" {
		return "", nil
	}
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Errorf("Could not read file: %s", filePath)
		return "", err
	}
	return strings.TrimSpace(string(fileBytes)), nil
}
