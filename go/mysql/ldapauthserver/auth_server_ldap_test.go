/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ldapauthserver

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	ldap "gopkg.in/ldap.v2"
)

type MockLdapClient struct{}

func (mlc *MockLdapClient) Connect(network string, config *ServerConfig) error { return nil }
func (mlc *MockLdapClient) Close()                                             {}
func (mlc *MockLdapClient) Bind(username, password string) error {
	if username != "testuser" || password != "testpass" {
		return fmt.Errorf("invalid credentials: %s, %s", username, password)
	}
	return nil
}
func (mlc *MockLdapClient) Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	return &ldap.SearchResult{}, nil
}

func TestValidateClearText(t *testing.T) {
	asl := &AuthServerLdap{
		Client:         &MockLdapClient{},
		User:           "testuser",
		Password:       "testpass",
		UserDnPattern:  "%s",
		RefreshSeconds: 1,
	}
	_, err := asl.validate("testuser", "testpass")
	require.NoError(t, err, "AuthServerLdap failed to validate valid credentials. Got: %v", err)

	_, err = asl.validate("invaliduser", "invalidpass")
	require.Error(t, err, "AuthServerLdap validated invalid credentials.")

}

// flakyLdapClient fails its first Connect and then behaves normally, so a test can drive
// update() through a failed refresh followed by a successful one.
type flakyLdapClient struct {
	connectAttempts int
}

func (c *flakyLdapClient) Connect(network string, config *ServerConfig) error {
	c.connectAttempts++
	if c.connectAttempts == 1 {
		return errors.New("simulated LDAP connect failure")
	}
	return nil
}

func (c *flakyLdapClient) Close() {}

func (c *flakyLdapClient) Bind(username, password string) error { return nil }

func (c *flakyLdapClient) Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	return &ldap.SearchResult{
		Entries: []*ldap.Entry{
			{Attributes: []*ldap.EntryAttribute{{Name: "cn", Values: []string{"refreshedgroup"}}}},
		},
	}, nil
}

// TestFailedRefreshDoesNotFreezeFutureUpdates checks that a refresh which fails on an LDAP
// error still clears the updating latch. Without that, every later update() short-circuits on
// the updating check and the user's cached groups stay frozen until the process restarts.
func TestFailedRefreshDoesNotFreezeFutureUpdates(t *testing.T) {
	client := &flakyLdapClient{}
	asl := &AuthServerLdap{
		Client:         client,
		User:           "testuser",
		Password:       "testpass",
		UserDnPattern:  "%s",
		RefreshSeconds: 1,
	}
	lud := &LdapUserData{asl: asl, groups: []string{"stalegroup"}, username: "testuser"}

	// The first refresh fails at Connect, so the cached groups are left untouched.
	lud.update()
	require.Equal(t, []string{"stalegroup"}, lud.groups, "a failed refresh should not change cached groups")

	// The failed refresh must not leave updating latched: the next refresh has to run and succeed.
	lud.update()
	require.Equal(t, []string{"refreshedgroup"}, lud.groups, "a refresh after an earlier failure must succeed; the updating latch was left set")
}
