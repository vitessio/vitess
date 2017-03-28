package mysqlconn

import (
	"bytes"

	"github.com/youtube/vitess/go/sqldb"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// AuthServerNone accepts only "user1", and rejects only "bad"
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
	return NewSalt()
}

// ValidateHash validates hash
func (a *AuthServerNone) ValidateHash(salt []byte, user string, authResponse []byte) (Getter, error) {
	if user != "user1" {
		return nil, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	computed := ScramblePassword(salt, []byte("bad"))
	if bytes.Compare(authResponse, computed) == 0 {
		return nil, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	return &NoneGetter{}, nil
}

// ValidateClearText validates clear text
func (a *AuthServerNone) ValidateClearText(user, password string) (Getter, error) {
	if user != "user1" {
		return nil, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	if password == "bad" {
		return nil, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
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
