package mysqlconn

import (
	"bytes"
	"encoding/json"
	"io/ioutil"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqldb"
)

// AuthServerConfig implements AuthServer using a static configuration.
type AuthServerConfig struct {
	// ClearText can be set to force the use of ClearText auth.
	ClearText bool

	// Entries contains the users, passwords and user data.
	Entries map[string]*AuthServerConfigEntry
}

// AuthServerConfigEntry stores the values for a given user.
type AuthServerConfigEntry struct {
	Password string
	UserData string
}

// NewAuthServerConfig returns a new empty AuthServerConfig.
func NewAuthServerConfig() *AuthServerConfig {
	return &AuthServerConfig{
		ClearText: false,
		Entries:   make(map[string]*AuthServerConfigEntry),
	}
}

// RegisterAuthServerConfigFromParams creates and registers a new
// AuthServerConfig, loaded for a JSON file or string. If file is set,
// it uses file. Otherwise, load the string. It log.Fatals out in case
// of error.
func RegisterAuthServerConfigFromParams(file, str string) {
	authServerConfig := NewAuthServerConfig()
	jsonConfig := []byte(str)
	if file != "" {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalf("Failed to read mysql_auth_server_config_file file: %v", err)
		}
		jsonConfig = data
	}

	// Parse JSON config.
	if err := json.Unmarshal(jsonConfig, &authServerConfig.Entries); err != nil {
		log.Fatalf("Error parsing auth server config: %v", err)
	}

	// And register the server.
	RegisterAuthServerImpl("config", authServerConfig)
}

// UseClearText is part of the AuthServer interface.
func (a *AuthServerConfig) UseClearText() bool {
	return a.ClearText
}

// Salt is part of the AuthServer interface.
func (a *AuthServerConfig) Salt() ([]byte, error) {
	return newSalt()
}

// ValidateHash is part of the AuthServer interface.
func (a *AuthServerConfig) ValidateHash(salt []byte, user string, authResponse []byte) (string, error) {
	// Find the entry.
	entry, ok := a.Entries[user]
	if !ok {
		return "", sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	// Validate the password.
	computedAuthResponse := scramblePassword(salt, []byte(entry.Password))
	if bytes.Compare(authResponse, computedAuthResponse) != 0 {
		return "", sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	return entry.UserData, nil
}

// ValidateClearText is part of the AuthServer interface.
func (a *AuthServerConfig) ValidateClearText(user, password string) (string, error) {
	// Find the entry.
	entry, ok := a.Entries[user]
	if !ok {
		return "", sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	// Validate the password.
	if entry.Password != password {
		return "", sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	return entry.UserData, nil
}
