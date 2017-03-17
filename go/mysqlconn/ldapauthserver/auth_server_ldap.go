package ldapauthserver

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysqlconn"
	"gopkg.in/ldap.v2"
)

var (
	ldapAuthConfigFile = flag.String("mysql_ldap_auth_config_file", "", "JSON File to read LDAP server config from.")
)

// AuthServerLdap implements AuthServer with an LDAP backend
type AuthServerLdap struct {
	queryUser     string
	queryPassword string
	ldapServer    string
	ldapPort      int
	queryStr      string
	getGroups     bool
	groupQueryStr string
}

func init() {
	if *ldapAuthConfigFile == "" {
		log.Infof("Not configuring AuthServerLdap because mysql_ldap_auth_config_file is empty")
		return
	}
	ldapAuthServer := newAuthServerLdap()

	data, err := ioutil.ReadFile(*ldapAuthConfigFile)
	if err != nil {
		log.Fatalf("Failed to read mysql_ldap_auth_config_file: %v", err)
	}
	if err := json.Unmarshal(data, &ldapAuthServer); err != nil {
		log.Fatalf("Error parsing AuthServerLdap config: %v", err)
	}
	mysqlconn.RegisterAuthServerImpl("ldap", ldapAuthServer)
}

func newAuthServerLdap() *AuthServerLdap {
	return &AuthServerLdap{}
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

// ValidateClearText connects to the LDAP server over TLS,
// searches for the user, attempts to bind as that user with the supplied password,
// and, if so configured, queries for the user's groups, returning them in a
// comma-separated string.
// In reality, it runs whatever queries are supplied. See TODO(acharis) for an example.
// It is recommended that queryUser have read-only privileges on ldapServer
func (asl *AuthServerLdap) ValidateClearText(username, password string) (string, error) {
	conn, err := ldap.Dial("tcp", fmt.Sprintf("%s:%d", asl.ldapServer, asl.ldapPort))
	defer conn.Close()
	if err != nil {
		return "", err
	}
	// Reconnect with TLS
	err = conn.StartTLS(&tls.Config{})
	if err != nil {
		return "", err
	}

	// queryUser can be read-only
	err = conn.Bind(asl.queryUser, asl.queryPassword)
	if err != nil {
		return "", err
	}

	// Search for the given username
	req := ldap.NewSearchRequest(
		fmt.Sprintf(asl.queryStr, username),
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		"(objectClass=organizationalPerson)",
		[]string{"dn"},
		nil,
	)

	res, err := conn.Search(req)
	if err != nil {
		return "", err
	}

	if len(res.Entries) != 1 {
		return "", errors.New("User does not exist or too many entries returned")
	}

	userdn := res.Entries[0].DN

	// Bind as the user to verify their password
	err = conn.Bind(userdn, password)
	if err != nil {
		return "", err
	}
	if !asl.getGroups {
		return "", nil
	}

	// Rebind as the query user for group query
	err = conn.Bind(asl.queryUser, asl.queryPassword)
	if err != nil {
		return "", err
	}

	req = ldap.NewSearchRequest(
		asl.groupQueryStr,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(memberUid=%s)", username),
		[]string{"cn"},
		nil,
	)
	res, err = conn.Search(req)
	if err != nil {
		return "", err
	}
	var buffer bytes.Buffer
	sep := ""
	for _, entry := range res.Entries {
		for _, attr := range entry.Attributes {
			buffer.WriteString(sep)
			buffer.WriteString(attr.Values[0])
			sep = ","
		}
	}
	return buffer.String(), nil
}
