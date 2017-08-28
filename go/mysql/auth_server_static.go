/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"

	log "github.com/golang/glog"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var (
	mysqlAuthServerStaticFile   = flag.String("mysql_auth_server_static_file", "", "JSON File to read the users/passwords from.")
	mysqlAuthServerStaticString = flag.String("mysql_auth_server_static_string", "", "JSON representation of the users/passwords config.")
)

const (
	localhostName = "localhost"
)

// AuthServerStatic implements AuthServer using a static configuration.
type AuthServerStatic struct {
	// Method can be set to:
	// - MysqlNativePassword
	// - MysqlClearPassword
	// - MysqlDialog
	// It defaults to MysqlNativePassword.
	Method string

	// Entries contains the users, passwords and user data.
	Entries map[string][]*AuthServerStaticEntry
}

// AuthServerStaticEntry stores the values for a given user.
type AuthServerStaticEntry struct {
	Password   string
	UserData   string
	SourceHost string
}

// InitAuthServerStatic Handles initializing the AuthServerStatic if necessary.
func InitAuthServerStatic() {
	// Check parameters.
	if *mysqlAuthServerStaticFile == "" && *mysqlAuthServerStaticString == "" {
		// Not configured, nothing to do.
		log.Infof("Not configuring AuthServerStatic, as mysql_auth_server_static_file and mysql_auth_server_static_string are empty")
		return
	}
	if *mysqlAuthServerStaticFile != "" && *mysqlAuthServerStaticString != "" {
		// Both parameters specified, can only use one.
		log.Fatalf("Both mysql_auth_server_static_file and mysql_auth_server_static_string specified, can only use one.")
	}

	// Create and register auth server.
	RegisterAuthServerStaticFromParams(*mysqlAuthServerStaticFile, *mysqlAuthServerStaticString)
}

// NewAuthServerStatic returns a new empty AuthServerStatic.
func NewAuthServerStatic() *AuthServerStatic {
	return &AuthServerStatic{
		Method:  MysqlNativePassword,
		Entries: make(map[string][]*AuthServerStaticEntry),
	}
}

// RegisterAuthServerStaticFromParams creates and registers a new
// AuthServerStatic, loaded for a JSON file or string. If file is set,
// it uses file. Otherwise, load the string. It log.Fatals out in case
// of error.
func RegisterAuthServerStaticFromParams(file, str string) {
	authServerStatic := NewAuthServerStatic()
	jsonConfig := []byte(str)
	if file != "" {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalf("Failed to read mysql_auth_server_static_file file: %v", err)
		}
		jsonConfig = data
	}

	// Parse JSON config.
	if err := parseConfig(jsonConfig, &authServerStatic.Entries); err != nil {
		log.Fatalf("Error parsing auth server config: %v", err)
	}

	// And register the server.
	RegisterAuthServerImpl("static", authServerStatic)
}

func parseConfig(jsonConfig []byte, config *map[string][]*AuthServerStaticEntry) error {
	if err := json.Unmarshal(jsonConfig, config); err == nil {
		if err := validateConfig(*config); err != nil {
			return err
		}
		return nil
	}
	// Couldn't parse, will try to parse with legacy config
	return parseLegacyConfig(jsonConfig, config)
}

func parseLegacyConfig(jsonConfig []byte, config *map[string][]*AuthServerStaticEntry) error {
	// legacy config doesn't have an array
	legacyConfig := make(map[string]*AuthServerStaticEntry)
	err := json.Unmarshal(jsonConfig, &legacyConfig)
	if err == nil {
		log.Warningf("Config parsed using legacy configuration. Please update to the latest format: {\"user\":[{\"Password\": \"xxx\"}, ...]}")
		for key, value := range legacyConfig {
			(*config)[key] = append((*config)[key], value)
		}
		return nil
	}
	return err
}

func validateConfig(config map[string][]*AuthServerStaticEntry) error {
	for _, entries := range config {
		for _, entry := range entries {
			if entry.SourceHost != "" && entry.SourceHost != localhostName {
				return fmt.Errorf("Invalid SourceHost found (only localhost is supported): %v", entry.SourceHost)
			}
		}
	}
	return nil
}

// AuthMethod is part of the AuthServer interface.
func (a *AuthServerStatic) AuthMethod(user string) (string, error) {
	return a.Method, nil
}

// Salt is part of the AuthServer interface.
func (a *AuthServerStatic) Salt() ([]byte, error) {
	return NewSalt()
}

// ValidateHash is part of the AuthServer interface.
func (a *AuthServerStatic) ValidateHash(salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error) {
	// Find the entry.
	entries, ok := a.Entries[user]
	if !ok {
		return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	for _, entry := range entries {
		computedAuthResponse := scramblePassword(salt, []byte(entry.Password))
		// Validate the password.
		if matchSourceHost(remoteAddr, entry.SourceHost) && bytes.Compare(authResponse, computedAuthResponse) == 0 {
			return &StaticUserData{entry.UserData}, nil
		}
	}
	return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
}

// Negotiate is part of the AuthServer interface.
// It will be called if Method is anything else than MysqlNativePassword.
// We only recognize MysqlClearPassword and MysqlDialog here.
func (a *AuthServerStatic) Negotiate(c *Conn, user string, remoteAddr net.Addr) (Getter, error) {
	// Finish the negotiation.
	password, err := AuthServerNegotiateClearOrDialog(c, a.Method)
	if err != nil {
		return nil, err
	}

	// Find the entry.
	entries, ok := a.Entries[user]
	if !ok {
		return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	for _, entry := range entries {
		// Validate the password.
		if matchSourceHost(remoteAddr, entry.SourceHost) && entry.Password == password {
			return &StaticUserData{entry.UserData}, nil
		}
	}
	return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
}

func matchSourceHost(remoteAddr net.Addr, targetSourceHost string) bool {
	// Legacy support, there was not matcher defined default to true
	if targetSourceHost == "" {
		return true
	}
	switch remoteAddr.(type) {
	case *net.UnixAddr:
		if targetSourceHost == localhostName {
			return true
		}
	}
	return false
}

// StaticUserData holds the username
type StaticUserData struct {
	value string
}

// Get returns the wrapped username
func (sud *StaticUserData) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: sud.value}
}
