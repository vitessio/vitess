package ldap

import (
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"gopkg.in/ldap.v2"
)

// ServerConfig holds the config for and LDAP server
// * include port in ldapServer, "ldap.example.com:386"
type ServerConfig struct {
	LdapServer string
	LdapCert   string
	LdapKey    string
	LdapCA     string
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
	lci.Conn = conn
	// Reconnect with TLS ... why don't we simply DialTLS directly?
	serverName, _, err := netutil.SplitHostPort(config.LdapServer)
	if err != nil {
		return err
	}
	tlsConfig, err := grpcutils.TLSClientConfig(config.LdapCert, config.LdapKey, config.LdapCA, serverName)
	if err != nil {
		return err
	}
	err = conn.StartTLS(tlsConfig)
	if err != nil {
		return err
	}
	return nil
}
