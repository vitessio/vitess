package mysqlconn

import (
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// authServerNone accepts any username/password as valid.
// It's meant to be used for testing and prototyping.
// With this config, you can connect to a local vtgate using
// the following command line: 'mysql -P port -h ::'.
type authServerNone struct {
}

func (a *authServerNone) UseClearText() bool {
	return false
}

func (a *authServerNone) Salt() ([]byte, error) {
	return make([]byte, 20), nil
}

func (a *authServerNone) ValidateHash(salt []byte, user string, authResponse []byte) (Getter, error) {
	return &NoneGetter{}, nil
}

func (a *authServerNone) ValidateClearText(user, password string) (Getter, error) {
	panic("unimplemented")
}

func init() {
	RegisterAuthServerImpl("none", &authServerNone{})
}

// NoneGetter holds the empty string
type NoneGetter struct{}

// Get returns the empty string
func (ng *NoneGetter) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: ""}
}
