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
	"sync"
	"syscall"
	"time"

	vaultapi "github.com/mch1307/vaultlib"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

var (
	// generic flags
	dbCredentialsServer = flag.String("db-credentials-server", "file", "db credentials server type ('file' - file implementation; 'vault' - HashiCorp Vault implementation)")

	// 'file' implementation flags
	dbCredentialsFile = flag.String("db-credentials-file", "", "db credentials file; send SIGHUP to reload this file")

	// 'vault' implementation flags
	dbCredentialsVaultAddr      = flag.String("db-credentials-vault-addr", "", "URL to Vault server")
	dbCredentialsVaultTimeout   = flag.Duration("db-credentials-vault-timeout", 10*time.Second, "Timeout for vault API operations")
	dbCredentialsVaultPath      = flag.String("db-credentials-vault-path", "", "Vault path to credentials JSON blob, e.g.: secret/data/prod/dbcreds")
	dbCredentialsVaultTokenFile = flag.String("db-credentials-vault-tokenfile", "", "Path to file containing Vault auth token; token can also be passed using VAULT_TOKEN environment variable")
	dbCredentialsVaultCacheTTL  = flag.Duration("db-credentials-vault-ttl", 30*time.Minute, "How long to cache DB credentials from the Vault server")

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

func (vcs *VaultCredentialsServer) GetUserAndPassword(user string) (string, string, error) {
	vcs.mu.Lock()
	defer vcs.mu.Unlock()

	if vcs.vaultCacheExpireTicker == nil {
		vcs.vaultCacheExpireTicker = time.NewTicker(*dbCredentialsVaultCacheTTL)
		go func() {
			for range vcs.vaultCacheExpireTicker.C {
				if vcs, ok := AllCredentialsServers["vault"].(*VaultCredentialsServer); ok {
					vcs.dbCredsCache = nil
				}
			}
		}()
	}

	if vcs.dbCredsCache != nil {
		return user, vcs.dbCredsCache[user][0], nil
	}

	if *dbCredentialsVaultAddr == "" {
		return "", "", errors.New("No Vault server specified")
	}

	token := os.Getenv("VAULT_TOKEN")
	if *dbCredentialsVaultTokenFile != "" {
		tokenBytes, err := ioutil.ReadFile(*dbCredentialsVaultTokenFile)
		if err != nil {
			return "", "", errors.New("No Vault token in provided filename")
		}
		token = string(tokenBytes)
	}

	// TODO: Add:  Vault CA cert;  Vault client cert/key
	// SNI Name;  disable retries or just single retry?  retry interval?
	// https://www.vaultproject.io/docs/commands#environment-variables
	// Basically guaranteed to work;  won't instantiate until we call out
	config := vaultapi.NewConfig()
	config.Address = *dbCredentialsVaultAddr
	config.Timeout = *dbCredentialsVaultTimeout
	config.Token = token
	client, _ := vaultapi.NewClient(config)

	// From here on, errors might be transient, so we use ErrUnknownUser
	// for everything
	secret, err := client.GetSecret(*dbCredentialsVaultPath)
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
	vcs.dbCredsCache = dbCreds

	return user, dbCreds[user][0], nil
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
		// TODO:  except if the actual password is empty, in which case
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
