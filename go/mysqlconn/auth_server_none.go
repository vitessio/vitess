package mysqlconn

import (
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// AuthServerNone takes all comers.
// It's meant to be used for testing and prototyping.
// With this config, you can connect to a local vtgate using
// the following command line: 'mysql -P port -h ::'.
// It only uses MysqlNativePassword method.
type AuthServerNone struct{}

// AuthMethod is part of the AuthServer interface.
// We always return MysqlNativePassword.
func (a *AuthServerNone) AuthMethod(user string) (string, error) {
	return MysqlNativePassword, nil
}

// Salt makes salt
func (a *AuthServerNone) Salt() ([]byte, error) {
	return NewSalt()
}

// ValidateHash validates hash
func (a *AuthServerNone) ValidateHash(salt []byte, user string, authResponse []byte) (Getter, error) {
	return &NoneGetter{}, nil
}

// Negotiate is part of the AuthServer interface.
// It will never be called.
func (a *AuthServerNone) Negotiate(c *Conn, user string) (Getter, error) {
	panic("Negotiate should not be called as AuthMethod returned mysql_native_password")
}

func init() {
	RegisterAuthServerImpl("none", &AuthServerNone{})
}

// NoneGetter holds the empty string
type NoneGetter struct{}

// Get returns the empty string
func (ng *NoneGetter) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: "userData1"}
}
