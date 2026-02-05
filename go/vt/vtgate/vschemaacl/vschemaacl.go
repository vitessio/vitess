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

package vschemaacl

import (
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/servenv"
)

type authorizedDDLUsers struct {
	allowAll bool
	acl      map[string]struct{}
	source   string
}

func NewAuthorizedDDLUsers(users string) *authorizedDDLUsers {
	acl := make(map[string]struct{})
	allowAll := false

	switch users {
	case "":
	case "%":
		allowAll = true
	default:
		for _, user := range strings.Split(users, ",") {
			user = strings.TrimSpace(user)
			acl[user] = struct{}{}
		}
	}

	return &authorizedDDLUsers{
		allowAll: allowAll,
		acl:      acl,
		source:   users,
	}
}

func (a *authorizedDDLUsers) String() string {
	return a.source
}

var (
	// AuthorizedDDLUsers specifies the users that can perform ddl operations
	AuthorizedDDLUsers = viperutil.Configure(
		"vschema_ddl_authorized_users",
		viperutil.Options[*authorizedDDLUsers]{
			FlagName: "vschema-ddl-authorized-users",
			Default:  &authorizedDDLUsers{},
			Dynamic:  true,
			GetFunc: func(v *viper.Viper) func(key string) *authorizedDDLUsers {
				return func(key string) *authorizedDDLUsers {
					newVal := v.GetString(key)
					curVal, ok := v.Get(key).(*authorizedDDLUsers)
					if ok && newVal == curVal.source {
						return curVal
					}
					return NewAuthorizedDDLUsers(newVal)
				}
			},
		},
	)
)

// RegisterSchemaACLFlags installs log flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which
// calls this function, or call this function directly before parsing
// command-line arguments.
func RegisterSchemaACLFlags(fs *pflag.FlagSet) {
	fs.String("vschema-ddl-authorized-users", "", "List of users authorized to execute vschema ddl operations, or '%' to allow all users.")
	viperutil.BindFlags(fs, AuthorizedDDLUsers)
}

func init() {
	for _, cmd := range []string{"vtcombo", "vtgate", "vtgateclienttest"} {
		servenv.OnParseFor(cmd, RegisterSchemaACLFlags)
	}
}

// Authorized returns true if the given caller is allowed to execute vschema operations
func Authorized(caller *querypb.VTGateCallerID) bool {
	users := AuthorizedDDLUsers.Get()
	if users.allowAll {
		return true
	}

	user := caller.GetUsername()
	_, ok := users.acl[user]
	return ok
}
