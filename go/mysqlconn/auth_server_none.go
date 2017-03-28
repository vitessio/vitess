package mysqlconn

import (
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// AuthServerNone takes all comers.
// It's meant to be used for testing and prototyping.
// With this config, you can connect to a local vtgate using
// the following command line: 'mysql -P port -h ::'.
type AuthServerNone struct {
	ClearText bool
}

// UseClearText reports clear text status
func (a *AuthServerNone) UseClearText() bool {
	return a.ClearText
}

// Salt makes salt
func (a *AuthServerNone) Salt() ([]byte, error) {
	return make([]byte, 20), nil
}

// ValidateHash validates hash
func (a *AuthServerNone) ValidateHash(salt []byte, user string, authResponse []byte) (Getter, error) {
	return &NoneGetter{}, nil
}

// ValidateClearText validates clear text
func (a *AuthServerNone) ValidateClearText(user, password string) (Getter, error) {
	return &NoneGetter{}, nil
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
