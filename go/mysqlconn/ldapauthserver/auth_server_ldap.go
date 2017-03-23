package ldapauthserver

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/ldap"
	"github.com/youtube/vitess/go/mysqlconn"
)

var (
	ldapAuthConfigFile   = flag.String("mysql_ldap_auth_config_file", "", "JSON File from which to read LDAP server config.")
	ldapAuthConfigString = flag.String("mysql_ldap_auth_config_string", "", "JSON representation of LDAP server config.")
)

// AuthServerLdap implements AuthServer with an LDAP backend
type AuthServerLdap struct {
	ldap.Client
	ldap.ServerConfig
	UserDnPattern string
}

func init() {
	if *ldapAuthConfigFile == "" && *ldapAuthConfigString == "" {
		log.Infof("Not configuring AuthServerLdap because mysql_ldap_auth_config_file and mysql_ldap_auth_config_string are empty")
		return
	}
	if *ldapAuthConfigFile != "" && *ldapAuthConfigString != "" {
		log.Infof("Both mysql_ldap_auth_config_file and mysql_ldap_auth_config_string are non-empty, can only use one.")
		return
	}
	ldapAuthServer := &AuthServerLdap{&ldap.ClientImpl{}, ldap.ServerConfig{}, ""}

	data := []byte(*ldapAuthConfigString)
	if *ldapAuthConfigFile != "" {
		var err error
		data, err = ioutil.ReadFile(*ldapAuthConfigFile)
		if err != nil {
			log.Fatalf("Failed to read mysql_ldap_auth_config_file: %v", err)
		}
	}
	if err := json.Unmarshal(data, ldapAuthServer); err != nil {
		log.Fatalf("Error parsing AuthServerLdap config: %v", err)
	}
	mysqlconn.RegisterAuthServerImpl("ldap", ldapAuthServer)
}

// UseClearText is always true for AuthServerLdap
func (asl *AuthServerLdap) UseClearText() bool {
	return true
}

// Salt is unimplemented for AuthServerLdap
func (asl *AuthServerLdap) Salt() ([]byte, error) {
	panic("unimplemented")
}

// ValidateHash is unimplemented for AuthServerLdap
func (asl *AuthServerLdap) ValidateHash(salt []byte, user string, authResponse []byte) (string, error) {
	panic("unimplemented")
}

// ValidateClearText connects to the LDAP server over TLS
// and attempts to bind as that user with the supplied password.
// It returns the supplied username.
func (asl *AuthServerLdap) ValidateClearText(username, password string) (string, error) {
	err := asl.Client.Connect("tcp", &asl.ServerConfig)
	defer asl.Client.Close()
	if err != nil {
		return "", err
	}
	err = asl.Client.Bind(fmt.Sprintf(asl.UserDnPattern, username), password)
	if err != nil {
		return "", err
	}
	return username, nil
}
