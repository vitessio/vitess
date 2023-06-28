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

package ldapauthserver

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/spf13/pflag"
	ldap "gopkg.in/ldap.v2"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttls"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	configPrefix       = viperutil.KeyPrefixFunc("mysql.auth_server.ldap")
	ldapAuthConfigFile = viperutil.Configure(configPrefix("config_file"), viperutil.Options[string]{
		FlagName: "mysql_ldap_auth_config_file",
	})
	ldapAuthConfigString = viperutil.Configure(configPrefix("config_string"), viperutil.Options[string]{
		FlagName: "mysql_ldap_auth_config_string",
	})
	ldapAuthMethod = viperutil.Configure(configPrefix("auth_method"), viperutil.Options[string]{ // TODO: make a strongly-typed mysql.AuthMethodDescription flag and viper decoder.
		FlagName: "mysql_ldap_auth_method",
		Default:  string(mysql.MysqlClearPassword),
	})
)

func init() {
	servenv.OnParseFor("vtgate", func(fs *pflag.FlagSet) {
		fs.String("mysql_ldap_auth_config_file", ldapAuthConfigFile.Default(), "JSON File from which to read LDAP server config.")
		fs.String("mysql_ldap_auth_config_string", ldapAuthConfigString.Default(), "JSON representation of LDAP server config.")
		fs.String("mysql_ldap_auth_method", ldapAuthMethod.Default(), "client-side authentication method to use. Supported values: mysql_clear_password, dialog.")

		viperutil.BindFlags(fs,
			ldapAuthConfigFile,
			ldapAuthConfigString,
			ldapAuthMethod,
		)
	})
}

// AuthServerLdap implements AuthServer with an LDAP backend
type AuthServerLdap struct {
	Client
	ServerConfig
	User           string
	Password       string
	GroupQuery     string
	UserDnPattern  string
	RefreshSeconds int64
	methods        []mysql.AuthMethod
}

// Init is public so it can be called from plugin_auth_ldap.go (go/cmd/vtgate)
func Init() {
	if ldapAuthConfigFile.Get() == "" && ldapAuthConfigString.Get() == "" {
		log.Infof("Not configuring AuthServerLdap because mysql_ldap_auth_config_file and mysql_ldap_auth_config_string are empty")
		return
	}
	if ldapAuthConfigFile.Get() != "" && ldapAuthConfigString.Get() != "" {
		log.Infof("Both mysql_ldap_auth_config_file and mysql_ldap_auth_config_string are non-empty, can only use one.")
		return
	}

	if ldapAuthMethod.Get() != string(mysql.MysqlClearPassword) && ldapAuthMethod.Get() != string(mysql.MysqlDialog) {
		log.Exitf("Invalid mysql_ldap_auth_method value: only support mysql_clear_password or dialog")
	}
	ldapAuthServer := &AuthServerLdap{
		Client:       &ClientImpl{},
		ServerConfig: ServerConfig{},
	}

	data := []byte(ldapAuthConfigString.Get())
	if ldapAuthConfigFile.Get() != "" {
		var err error
		data, err = os.ReadFile(ldapAuthConfigFile.Get())
		if err != nil {
			log.Exitf("Failed to read mysql_ldap_auth_config_file: %v", err)
		}
	}
	if err := json.Unmarshal(data, ldapAuthServer); err != nil {
		log.Exitf("Error parsing AuthServerLdap config: %v", err)
	}

	var authMethod mysql.AuthMethod
	switch mysql.AuthMethodDescription(ldapAuthMethod.Get()) {
	case mysql.MysqlClearPassword:
		authMethod = mysql.NewMysqlClearAuthMethod(ldapAuthServer, ldapAuthServer)
	case mysql.MysqlDialog:
		authMethod = mysql.NewMysqlDialogAuthMethod(ldapAuthServer, ldapAuthServer, "")
	default:
		log.Exitf("Invalid mysql_ldap_auth_method value: only support mysql_clear_password or dialog")
	}

	ldapAuthServer.methods = []mysql.AuthMethod{authMethod}
	mysql.RegisterAuthServer("ldap", ldapAuthServer)
}

// AuthMethods returns the list of registered auth methods
// implemented by this auth server.
func (asl *AuthServerLdap) AuthMethods() []mysql.AuthMethod {
	return asl.methods
}

// DefaultAuthMethodDescription returns MysqlNativePassword as the default
// authentication method for the auth server implementation.
func (asl *AuthServerLdap) DefaultAuthMethodDescription() mysql.AuthMethodDescription {
	return mysql.MysqlNativePassword
}

// HandleUser is part of the Validator interface. We
// handle any user here since we don't check up front.
func (asl *AuthServerLdap) HandleUser(user string) bool {
	return true
}

// UserEntryWithPassword is part of the PlaintextStorage interface
// and called after the password is sent by the client.
func (asl *AuthServerLdap) UserEntryWithPassword(conn *mysql.Conn, user string, password string, remoteAddr net.Addr) (mysql.Getter, error) {
	return asl.validate(user, password)
}

func (asl *AuthServerLdap) validate(username, password string) (mysql.Getter, error) {
	if err := asl.Client.Connect("tcp", &asl.ServerConfig); err != nil {
		return nil, err
	}
	defer asl.Client.Close()
	if err := asl.Client.Bind(fmt.Sprintf(asl.UserDnPattern, username), password); err != nil {
		return nil, err
	}
	groups, err := asl.getGroups(username)
	if err != nil {
		return nil, err
	}
	return &LdapUserData{asl: asl, groups: groups, username: username, lastUpdated: time.Now(), updating: false}, nil
}

// this needs to be passed an already connected client...should check for this
func (asl *AuthServerLdap) getGroups(username string) ([]string, error) {
	err := asl.Client.Bind(asl.User, asl.Password)
	if err != nil {
		return nil, err
	}
	req := ldap.NewSearchRequest(
		asl.GroupQuery,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(memberUid=%s)", username),
		[]string{"cn"},
		nil,
	)
	res, err := asl.Client.Search(req)
	if err != nil {
		return nil, err
	}
	var groups []string
	for _, entry := range res.Entries {
		for _, attr := range entry.Attributes {
			groups = append(groups, attr.Values[0])
		}
	}
	return groups, nil
}

// LdapUserData holds username and LDAP groups as well as enough data to
// intelligently update itself.
type LdapUserData struct {
	asl         *AuthServerLdap
	groups      []string
	username    string
	lastUpdated time.Time
	updating    bool
	sync.Mutex
}

func (lud *LdapUserData) update() {
	lud.Lock()
	if lud.updating {
		lud.Unlock()
		return
	}
	lud.updating = true
	lud.Unlock()
	err := lud.asl.Client.Connect("tcp", &lud.asl.ServerConfig)
	if err != nil {
		log.Errorf("Error updating LDAP user data: %v", err)
		return
	}
	defer lud.asl.Client.Close() //after the error check
	groups, err := lud.asl.getGroups(lud.username)
	if err != nil {
		log.Errorf("Error updating LDAP user data: %v", err)
		return
	}
	lud.Lock()
	lud.groups = groups
	lud.lastUpdated = time.Now()
	lud.updating = false
	lud.Unlock()
}

// Get returns wrapped username and LDAP groups and possibly updates the cache
func (lud *LdapUserData) Get() *querypb.VTGateCallerID {
	if int64(time.Since(lud.lastUpdated).Seconds()) > lud.asl.RefreshSeconds {
		go lud.update()
	}
	return &querypb.VTGateCallerID{Username: lud.username, Groups: lud.groups}
}

// ServerConfig holds the config for and LDAP server
// * include port in ldapServer, "ldap.example.com:386"
type ServerConfig struct {
	LdapServer        string
	LdapCert          string
	LdapKey           string
	LdapCA            string
	LdapCRL           string
	LdapTLSMinVersion string
}

// Client provides an interface we can mock
type Client interface {
	Connect(network string, config *ServerConfig) error
	Close()
	Bind(string, string) error
	Search(*ldap.SearchRequest) (*ldap.SearchResult, error)
}

// ClientImpl is the real implementation of LdapClient
type ClientImpl struct {
	*ldap.Conn
}

// Connect calls ldap.Dial and then upgrades the connection to TLS
// This must be called before any other methods
func (lci *ClientImpl) Connect(network string, config *ServerConfig) error {
	conn, err := ldap.Dial(network, config.LdapServer)
	if err != nil {
		return err
	}
	lci.Conn = conn
	// Reconnect with TLS ... why don't we simply DialTLS directly?
	serverName, _, err := netutil.SplitHostPort(config.LdapServer)
	if err != nil {
		return err
	}

	tlsVersion, err := vttls.TLSVersionToNumber(config.LdapTLSMinVersion)
	if err != nil {
		return err
	}

	tlsConfig, err := vttls.ClientConfig(vttls.VerifyIdentity, config.LdapCert, config.LdapKey, config.LdapCA, config.LdapCRL, serverName, tlsVersion)
	if err != nil {
		return err
	}
	err = conn.StartTLS(tlsConfig)
	if err != nil {
		return err
	}
	return nil
}
