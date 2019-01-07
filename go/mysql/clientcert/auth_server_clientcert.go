package clientcert

import (
	"flag"
	"fmt"
	"net"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var clientcertAuthMethod = flag.String("mysql_clientcert_auth_method", mysql.MysqlClearPassword, "client-side authentication method to use. Supported values: mysql_clear_password, dialog.")

type AuthServerClientCert struct {
	Method string
}

type UserData struct {
	querypb.VTGateCallerID
}

// Init is public so it can be called from plugin_auth_ldap.go (go/cmd/vtgate)
func Init() {
	if *clientcertAuthMethod != mysql.MysqlClearPassword && *clientcertAuthMethod != mysql.MysqlDialog {
		log.Exitf("Invalid mysql_clientcert_auth_method value: only support mysql_clear_password or dialog")
	}
	ascc := &AuthServerClientCert{
		Method: *clientcertAuthMethod,
	}
	mysql.RegisterAuthServerImpl("clientcert", ascc)
}

// AuthMethod is part of the AuthServer interface.
func (ascc *AuthServerClientCert) AuthMethod(user string) (string, error) {
	return ascc.Method, nil
}

// Salt is not used for this plugin.
func (ascc *AuthServerClientCert) Salt() ([]byte, error) {
	return mysql.NewSalt()
}

// ValidateHash is unimplemented.
func (ascc *AuthServerClientCert) ValidateHash(salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (mysql.Getter, error) {
	panic("unimplemented")
}

// Negotiate is part of the AuthServer interface.
func (ascc *AuthServerClientCert) Negotiate(c *mysql.Conn, user string, remoteAddr net.Addr) (mysql.Getter, error) {
	// This code depends on the fact that golang's tls server enforces client cert verification.
	// Note that the -mysql_server_ssl_ca flag must be set in order for the vtgate to accept client certs.
	// If not set, the vtgate will effectively deny all incoming mysql connections, since they will all lack certificates.
	// For more info, check out go/vt/vtttls/vttls.go
	certs := c.GetTLSClientCerts()
	if certs == nil || len(certs) == 0 {
		return nil, fmt.Errorf("no client certs for connection ID %v", c.ConnectionID)
	}

	if _, err := mysql.AuthServerReadPacketString(c); err != nil {
		return nil, err
	}

	return &UserData{
		VTGateCallerID: querypb.VTGateCallerID{
			Username: certs[0].Subject.CommonName,
			Groups:   certs[0].DNSNames,
		},
	}, nil
}

func (ud *UserData) Get() *querypb.VTGateCallerID {
	return &ud.VTGateCallerID
}
