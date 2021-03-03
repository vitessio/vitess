/*
Copyright 2019 The Vitess Authors.

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

package dbconfigs

// This file contains logic for a pluggable credentials system.
// The default implementation is file based.
// The flags are global, but only programs that need to access the database
// link with this library, so we should be safe.

import (
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	vaultapi "github.com/aquarapid/vaultlib"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

var (
	// generic flags
	dbCredentialsServer = flag.String("db-credentials-server", "file", "db credentials server type ('file' - file implementation; 'vault' - HashiCorp Vault implementation)")

	// 'file' implementation flags
	dbCredentialsFile = flag.String("db-credentials-file", "", "db credentials file; send SIGHUP to reload this file")

	// 'vault' implementation flags
	vaultAddr             = flag.String("db-credentials-vault-addr", "", "URL to Vault server")
	vaultTimeout          = flag.Duration("db-credentials-vault-timeout", 10*time.Second, "Timeout for vault API operations")
	vaultCACert           = flag.String("db-credentials-vault-tls-ca", "", "Path to CA PEM for validating Vault server certificate")
	vaultPath             = flag.String("db-credentials-vault-path", "", "Vault path to credentials JSON blob, e.g.: secret/data/prod/dbcreds")
	vaultCacheTTL         = flag.Duration("db-credentials-vault-ttl", 30*time.Minute, "How long to cache DB credentials from the Vault server")
	vaultTokenFile        = flag.String("db-credentials-vault-tokenfile", "", "Path to file containing Vault auth token; token can also be passed using VAULT_TOKEN environment variable")
	vaultRoleID           = flag.String("db-credentials-vault-roleid", "", "Vault AppRole id; can also be passed using VAULT_ROLEID environment variable")
	vaultRoleSecretIDFile = flag.String("db-credentials-vault-role-secretidfile", "", "Path to file containing Vault AppRole secret_id; can also be passed using VAULT_SECRETID environment variable")
	vaultRoleMountPoint   = flag.String("db-credentials-vault-role-mountpoint", "approle", "Vault AppRole mountpoint; can also be passed using VAULT_MOUNTPOINT environment variable")

	// ErrUnknownUser is returned by credential server when the
	// user doesn't exist
	ErrUnknownUser = errors.New("unknown user")
)

// CredentialsServer is the interface for a credential server
type CredentialsServer interface {
	// GetUserAndPassword returns the user / password to use for a given
	// user. May return ErrUnknownUser. The user might be altered
	// to support versioned users.
	// Note this call needs to be thread safe, as we may call this from
	// multiple go routines.
	GetUserAndPassword(user string) (string, string, error)
}

// AllCredentialsServers contains all the known CredentialsServer
// implementations.  Note we will only access this after flags have
// been parsed.
var AllCredentialsServers = make(map[string]CredentialsServer)

// GetCredentialsServer returns the current CredentialsServer. Only valid
// after flag.Init was called.
func GetCredentialsServer() CredentialsServer {
	cs, ok := AllCredentialsServers[*dbCredentialsServer]
	if !ok {
		log.Exitf("Invalid credential server: %v", *dbCredentialsServer)
	}
	return cs
}

// FileCredentialsServer is a simple implementation of CredentialsServer using
// a json file. Protected by mu.
type FileCredentialsServer struct {
	mu            sync.Mutex
	dbCredentials map[string][]string
}

// VaultCredentialsServer implements CredentialsServer using
// a Vault backend from HashiCorp.
type VaultCredentialsServer struct {
	mu                     sync.Mutex
	dbCredsCache           map[string][]string
	vaultCacheExpireTicker *time.Ticker
	vaultClient            *vaultapi.Client
	// We use a separate valid flag to allow invalidating the cache
	// without destroying it, in case Vault is temp down.
	cacheValid bool
}

// GetUserAndPassword is part of the CredentialsServer interface
func (fcs *FileCredentialsServer) GetUserAndPassword(user string) (string, string, error) {
	fcs.mu.Lock()
	defer fcs.mu.Unlock()

	if *dbCredentialsFile == "" {
		return "", "", ErrUnknownUser
	}

	// read the json file only once
	if fcs.dbCredentials == nil {
		fcs.dbCredentials = make(map[string][]string)

		data, err := ioutil.ReadFile(*dbCredentialsFile)
		if err != nil {
			log.Warningf("Failed to read dbCredentials file: %v", *dbCredentialsFile)
			return "", "", err
		}

		if err = json.Unmarshal(data, &fcs.dbCredentials); err != nil {
			log.Warningf("Failed to parse dbCredentials file: %v", *dbCredentialsFile)
			return "", "", err
		}
	}

	passwd, ok := fcs.dbCredentials[user]
	if !ok {
		return "", "", ErrUnknownUser
	}
	return user, passwd[0], nil
}

// GetUserAndPassword for Vault implementation
func (vcs *VaultCredentialsServer) GetUserAndPassword(user string) (string, string, error) {
	vcs.mu.Lock()
	defer vcs.mu.Unlock()

	if vcs.vaultCacheExpireTicker == nil {
		vcs.vaultCacheExpireTicker = time.NewTicker(*vaultCacheTTL)
		go func() {
			for range vcs.vaultCacheExpireTicker.C {
				if vcs, ok := AllCredentialsServers["vault"].(*VaultCredentialsServer); ok {
					vcs.cacheValid = false
				}
			}
		}()
	}

	if vcs.cacheValid && vcs.dbCredsCache != nil {
		if vcs.dbCredsCache[user] == nil {
			log.Errorf("Vault cache is valid, but user %s unknown in cache, will retry", user)
			return "", "", ErrUnknownUser
		}
		return user, vcs.dbCredsCache[user][0], nil
	}

	if *vaultAddr == "" {
		return "", "", errors.New("No Vault server specified")
	}

	token, err := readFromFile(*vaultTokenFile)
	if err != nil {
		return "", "", errors.New("No Vault token in provided filename")
	}
	secretID, err := readFromFile(*vaultRoleSecretIDFile)
	if err != nil {
		return "", "", errors.New("No Vault secret_id in provided filename")
	}

	// From here on, errors might be transient, so we use ErrUnknownUser
	// for everything, so we get retries
	if vcs.vaultClient == nil {
		config := vaultapi.NewConfig()

		// All these can be overriden by environment
		//   so we need to check if they have been set by NewConfig
		if config.Address == "" {
			config.Address = *vaultAddr
		}
		if config.Timeout == (0 * time.Second) {
			config.Timeout = *vaultTimeout
		}
		if config.CACert == "" {
			config.CACert = *vaultCACert
		}
		if config.Token == "" {
			config.Token = token
		}
		if config.AppRoleCredentials.RoleID == "" {
			config.AppRoleCredentials.RoleID = *vaultRoleID
		}
		if config.AppRoleCredentials.SecretID == "" {
			config.AppRoleCredentials.SecretID = secretID
		}
		if config.AppRoleCredentials.MountPoint == "" {
			config.AppRoleCredentials.MountPoint = *vaultRoleMountPoint
		}

		if config.CACert != "" {
			// If we provide a CA, ensure we actually use it
			config.InsecureSSL = false
		}

		var err error
		vcs.vaultClient, err = vaultapi.NewClient(config)
		if err != nil || vcs.vaultClient == nil {
			log.Errorf("Error in vault client initialization, will retry: %v", err)
			vcs.vaultClient = nil
			return "", "", ErrUnknownUser
		}
	}

	secret, err := vcs.vaultClient.GetSecret(*vaultPath)
	if err != nil {
		log.Errorf("Error in Vault server params: %v", err)
		return "", "", ErrUnknownUser
	}

	if secret.JSONSecret == nil {
		log.Errorf("Empty DB credentials retrieved from Vault server")
		return "", "", ErrUnknownUser
	}

	dbCreds := make(map[string][]string)
	if err = json.Unmarshal(secret.JSONSecret, &dbCreds); err != nil {
		log.Errorf("Error unmarshaling DB credentials from Vault server")
		return "", "", ErrUnknownUser
	}
	if dbCreds[user] == nil {
		log.Warningf("Vault lookup for user not found: %v\n", user)
		return "", "", ErrUnknownUser
	}
	log.Infof("Vault client status: %s", vcs.vaultClient.GetStatus())

	vcs.dbCredsCache = dbCreds
	vcs.cacheValid = true
	return user, dbCreds[user][0], nil
}

func readFromFile(filePath string) (string, error) {
	if filePath == "" {
		return "", nil
	}
	fileBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(fileBytes)), nil
}

// WithCredentials returns a copy of the provided ConnParams that we can use
// to connect, after going through the CredentialsServer.
func withCredentials(cp *mysql.ConnParams) (*mysql.ConnParams, error) {
	result := *cp
	user, passwd, err := GetCredentialsServer().GetUserAndPassword(cp.Uname)
	switch err {
	case nil:
		result.Uname = user
		result.Pass = passwd
	case ErrUnknownUser:
		// we just use what we have, and will fail later anyway
		// except if the actual password is empty, in which case
		// things will just "work"
		err = nil
	}
	return &result, err
}

func init() {
	AllCredentialsServers["file"] = &FileCredentialsServer{}
	AllCredentialsServers["vault"] = &VaultCredentialsServer{}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for range sigChan {
			if fcs, ok := AllCredentialsServers["file"].(*FileCredentialsServer); ok {
				fcs.mu.Lock()
				fcs.dbCredentials = nil
				fcs.mu.Unlock()
			}
			if vcs, ok := AllCredentialsServers["vault"].(*VaultCredentialsServer); ok {
				vcs.mu.Lock()
				vcs.dbCredsCache = nil
				vcs.mu.Unlock()
			}
		}
	}()
}
