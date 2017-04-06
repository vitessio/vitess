package mysqlconn

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqldb"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var (
	mysqlAuthServerStaticFile   = flag.String("mysql_auth_server_static_file", "", "JSON File to read the users/passwords from.")
	mysqlAuthServerStaticString = flag.String("mysql_auth_server_static_string", "", "JSON representation of the users/passwords config.")
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
	Entries map[string]*AuthServerStaticEntry
}

// AuthServerStaticEntry stores the values for a given user.
type AuthServerStaticEntry struct {
	Password string
	UserData string
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
		Entries: make(map[string]*AuthServerStaticEntry),
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

// AuthMethod is part of the AuthServer interface.
func (a *AuthServerStatic) AuthMethod(user string) (string, error) {
	return a.Method, nil
}

// Salt is part of the AuthServer interface.
func (a *AuthServerStatic) Salt() ([]byte, error) {
	return NewSalt()
}

// ValidateHash is part of the AuthServer interface.
func (a *AuthServerStatic) ValidateHash(salt []byte, user string, authResponse []byte) (Getter, error) {
	// Find the entry.
	entry, ok := a.Entries[user]
	if !ok {
		return &StaticUserData{""}, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	// Validate the password.
	computedAuthResponse := scramblePassword(salt, []byte(entry.Password))
	if bytes.Compare(authResponse, computedAuthResponse) != 0 {
		return &StaticUserData{""}, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	return &StaticUserData{entry.UserData}, nil
}

// Negotiate is part of the AuthServer interface.
// It will be called if Method is anything else than MysqlNativePassword.
// We only recognize MysqlClearPassword and MysqlDialog here.
func (a *AuthServerStatic) Negotiate(c *Conn, user string) (Getter, error) {
	// Finish the negotiation.
	password, err := AuthServerNegotiateClearOrDialog(c, a.Method)
	if err != nil {
		return nil, err
	}

	// Find the entry.
	entry, ok := a.Entries[user]
	if !ok {
		return &StaticUserData{""}, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	// Validate the password.
	if entry.Password != password {
		return &StaticUserData{""}, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	return &StaticUserData{entry.UserData}, nil
}

// StaticUserData holds the username
type StaticUserData struct {
	value string
}

// Get returns the wrapped username
func (sud *StaticUserData) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: sud.value}
}
