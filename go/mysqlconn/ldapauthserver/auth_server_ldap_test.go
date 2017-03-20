package ldapauthserver

import (
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"gopkg.in/ldap.v2"
)

type MockLdapClient struct{}

func (mlc *MockLdapClient) Dial(network, server string) (ldap.Client, error) {
	return &MockLdapConn{}, nil
}

type MockLdapConn struct{}

func (mlc *MockLdapConn) StartTLS(config *tls.Config) error { return nil }
func (mlc *MockLdapConn) Bind(username, password string) error {
	if username != "testuser" || password != "testpass" {
		return fmt.Errorf("invalid credentials: %s, %s", username, password)
	}
	return nil
}
func (mlc *MockLdapConn) Close() {}

func (mlc *MockLdapConn) Start()                   { panic("unimpl") }
func (mlc *MockLdapConn) SetTimeout(time.Duration) { panic("unimpl") }
func (mlc *MockLdapConn) SimpleBind(simpleBindRequest *ldap.SimpleBindRequest) (*ldap.SimpleBindResult, error) {
	panic("unimpl")
}
func (mlc *MockLdapConn) Add(addRequest *ldap.AddRequest) error             { panic("unimpl") }
func (mlc *MockLdapConn) Del(delRequest *ldap.DelRequest) error             { panic("unimpl") }
func (mlc *MockLdapConn) Modify(modifyRequest *ldap.ModifyRequest) error    { panic("unimpl") }
func (mlc *MockLdapConn) Compare(dn, attribute, value string) (bool, error) { panic("unimpl") }
func (mlc *MockLdapConn) PasswordModify(passwordModifyRequest *ldap.PasswordModifyRequest) (*ldap.PasswordModifyResult, error) {
	panic("unimpl")
}
func (mlc *MockLdapConn) Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	panic("unimpl")
}
func (mlc *MockLdapConn) SearchWithPaging(searchRequest *ldap.SearchRequest, pagingSize uint32) (*ldap.SearchResult, error) {
	panic("unimpl")
}

func TestValidateClearText(t *testing.T) {
	mockLdapConfig := &AuthServerLdapConfig{ldapServer: "ldap.test.com:386", userDnPattern: "%s"}
	asl := &AuthServerLdap{Config: mockLdapConfig, Client: &MockLdapClient{}}
	_, err := asl.ValidateClearText("testuser", "testpass")
	if err != nil {
		t.Fatalf("AuthServerLdap failed to validate valid credentials. Got: %v", err)
	}

	_, err = asl.ValidateClearText("invaliduser", "invalidpass")
	if err == nil {
		t.Fatalf("AuthServerLdap validated invalid credentials.")
	}
}
