/*
Copyright 2025 The Vitess Authors.

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

package binlogacl

import (
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/servenv"
)

type authorizedBinlogUsers struct {
	allowAll bool
	acl      map[string]struct{}
	source   string
}

// NewAuthorizedBinlogUsers creates a new authorizedBinlogUsers from a config string.
// The string can be:
//   - "" (empty): no users are authorized
//   - "%": all users are authorized
//   - "user1,user2,...": comma-separated list of authorized usernames
func NewAuthorizedBinlogUsers(users string) *authorizedBinlogUsers {
	acl := make(map[string]struct{})
	allowAll := false

	switch users {
	case "":
		// no users authorized
	case "%":
		allowAll = true
	default:
		for user := range strings.SplitSeq(users, ",") {
			user = strings.TrimSpace(user)
			acl[user] = struct{}{}
		}
	}

	return &authorizedBinlogUsers{
		allowAll: allowAll,
		acl:      acl,
		source:   users,
	}
}

func (a *authorizedBinlogUsers) String() string {
	return a.source
}

// AuthorizedBinlogUsers specifies the users that can perform binlog dump operations
var AuthorizedBinlogUsers = viperutil.Configure(
	"binlog_dump_authorized_users",
	viperutil.Options[*authorizedBinlogUsers]{
		FlagName: "binlog-dump-authorized-users",
		Default:  &authorizedBinlogUsers{},
		Dynamic:  true,
		GetFunc: func(v *viper.Viper) func(key string) *authorizedBinlogUsers {
			return func(key string) *authorizedBinlogUsers {
				newVal := v.GetString(key)
				curVal, ok := v.Get(key).(*authorizedBinlogUsers)
				if ok && newVal == curVal.source {
					return curVal
				}
				return NewAuthorizedBinlogUsers(newVal)
			}
		},
	},
)

// RegisterBinlogACLFlags registers the binlog ACL flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which
// calls this function, or call this function directly before parsing
// command-line arguments.
func RegisterBinlogACLFlags(fs *pflag.FlagSet) {
	fs.String("binlog-dump-authorized-users", "", "List of users authorized to execute binlog dump operations, or '%' to allow all users.")
	viperutil.BindFlags(fs, AuthorizedBinlogUsers)
}

func init() {
	for _, cmd := range []string{"vtcombo", "vtgate", "vtgateclienttest"} {
		servenv.OnParseFor(cmd, RegisterBinlogACLFlags)
	}
}

// Authorized returns true if the given caller is allowed to perform binlog dump operations
func Authorized(caller *querypb.VTGateCallerID) bool {
	users := AuthorizedBinlogUsers.Get()
	if users.allowAll {
		return true
	}

	user := caller.GetUsername()
	_, ok := users.acl[user]
	return ok
}
