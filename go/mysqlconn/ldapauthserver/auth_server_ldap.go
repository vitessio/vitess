package ldapauthserver

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"gopkg.in/ldap.v2"
)

var (
	ldapAuthConfigFile   = flag.String("mysql_ldap_auth_config_file", "", "JSON File from which to read LDAP server config.")
	ldapAuthConfigString = flag.String("mysql_ldap_auth_config_string", "", "JSON representation of LDAP server config.")
)

// AuthServerLdap implements AuthServer with an LDAP backend
type AuthServerLdap struct {
	Config *AuthServerLdapConfig
	Client LdapClient
}

// AuthServerLdapConfig holds the config for AuthServerLdap
// * include port in ldapServer, "ldap.example.com:386"
type AuthServerLdapConfig struct {
	ldapServer    string
	ldapCert      string
	ldapKey       string
	ldapCA        string
	userDnPattern string
}

// LdapClient abstracts the call to Dial so we can mock it
type LdapClient interface {
	Dial(network, server string) (ldap.Client, error)
}

// LdapClientImpl is the real implementation of LdapClient
type LdapClientImpl struct{}

// Dial calls the ldap.v2 library's Dial
func (lci *LdapClientImpl) Dial(network, server string) (ldap.Client, error) {
	return ldap.Dial(network, server)
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
	ldapAuthServer := &AuthServerLdap{Config: &AuthServerLdapConfig{}, Client: &LdapClientImpl{}}

	data := []byte(*ldapAuthConfigString)
	if *ldapAuthConfigFile != "" {
		var err error
		data, err = ioutil.ReadFile(*ldapAuthConfigFile)
		if err != nil {
			log.Fatalf("Failed to read mysql_ldap_auth_config_file: %v", err)
		}
	}
	if err := json.Unmarshal(data, ldapAuthServer.Config); err != nil {
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
	conn, err := asl.Client.Dial("tcp", asl.Config.ldapServer)
	defer conn.Close()
	if err != nil {
		return "", err
	}

	// Reconnect with TLS ... why don't we simply DialTLS directly?
	serverName, _, err := netutil.SplitHostPort(asl.Config.ldapServer)
	if err != nil {
		return "", err
	}
	tlsConfig, err := grpcutils.TLSClientConfig(asl.Config.ldapCert, asl.Config.ldapKey, asl.Config.ldapCA, serverName)
	if err != nil {
		return "", err
	}
	err = conn.StartTLS(tlsConfig)
	if err != nil {
		return "", err
	}

	// queryUser can be read-only
	err = conn.Bind(fmt.Sprintf(asl.Config.userDnPattern, username), password)
	if err != nil {
		return "", err
	}
	return username, nil
}
