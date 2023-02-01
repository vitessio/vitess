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
	"sync"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	// AuthorizedDDLUsers specifies the users that can perform ddl operations
	AuthorizedDDLUsers string

	// ddlAllowAll is true if the special value of "*" was specified
	allowAll bool

	// ddlACL contains a set of allowed usernames
	acl map[string]struct{}

	initMu sync.Mutex
)

// RegisterSchemaACLFlags installs log flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which
// calls this function, or call this function directly before parsing
// command-line arguments.
func RegisterSchemaACLFlags(fs *pflag.FlagSet) {
	fs.StringVar(&AuthorizedDDLUsers, "vschema_ddl_authorized_users", AuthorizedDDLUsers, "List of users authorized to execute vschema ddl operations, or '%' to allow all users.")
}

func init() {
	for _, cmd := range []string{"vtcombo", "vtgate", "vtgateclienttest"} {
		servenv.OnParseFor(cmd, RegisterSchemaACLFlags)
	}
}

// Init parses the users option and sets allowAll / acl accordingly
func Init() {
	initMu.Lock()
	defer initMu.Unlock()
	acl = make(map[string]struct{})
	allowAll = false

	if AuthorizedDDLUsers == "%" {
		allowAll = true
		return
	} else if AuthorizedDDLUsers == "" {
		return
	}

	for _, user := range strings.Split(AuthorizedDDLUsers, ",") {
		user = strings.TrimSpace(user)
		acl[user] = struct{}{}
	}
}

// Authorized returns true if the given caller is allowed to execute vschema operations
func Authorized(caller *querypb.VTGateCallerID) bool {
	if allowAll {
		return true
	}

	user := caller.GetUsername()
	_, ok := acl[user]
	return ok
}
