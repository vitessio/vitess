package mysqlconn

import (
	"bytes"
	"encoding/json"
	"io/ioutil"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqldb"
)

// AuthServerStatic implements AuthServer using a static configuration.
type AuthServerStatic struct {
	// ClearText can be set to force the use of ClearText auth.
	ClearText bool

	// Entries contains the users, passwords and user data.
	Entries map[string]*AuthServerStaticEntry
}

// AuthServerStaticEntry stores the values for a given user.
type AuthServerStaticEntry struct {
	Password string
	UserData string
}

// NewAuthServerStatic returns a new empty AuthServerStatic.
func NewAuthServerStatic() *AuthServerStatic {
	return &AuthServerStatic{
		ClearText: false,
		Entries:   make(map[string]*AuthServerStaticEntry),
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
	if err := json.Unmarshal(jsonConfig, &authServerStatic.Entries); err != nil {
		log.Fatalf("Error parsing auth server config: %v", err)
	}

	// And register the server.
	RegisterAuthServerImpl("static", authServerStatic)
}

// UseClearText is part of the AuthServer interface.
func (a *AuthServerStatic) UseClearText() bool {
	return a.ClearText
}

// Salt is part of the AuthServer interface.
func (a *AuthServerStatic) Salt() ([]byte, error) {
	return newSalt()
}

// ValidateHash is part of the AuthServer interface.
func (a *AuthServerStatic) ValidateHash(salt []byte, user string, authResponse []byte) (string, error) {
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
func (a *AuthServerStatic) ValidateClearText(user, password string) (string, error) {
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
