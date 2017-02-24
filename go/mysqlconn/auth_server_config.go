package mysqlconn

import (
	"bytes"

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
