package vschemaacl

import (
	"flag"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	// AuthorizedDDLUsers specifies the users that can perform ddl operations
	AuthorizedDDLUsers = flag.String("vschema_ddl_authorized_users", "", "List of users authorized to execute vschema ddl operations, or '%' to allow all users.")

	// ddlAllowAll is true if the special value of "*" was specified
	allowAll bool

	// ddlACL contains a set of allowed usernames
	acl map[string]struct{}
)

// Init parses the users option and sets allowAll / acl accordingly
func Init() {
	acl = make(map[string]struct{})
	allowAll = false

	if *AuthorizedDDLUsers == "%" {
		allowAll = true
		return
	} else if *AuthorizedDDLUsers == "" {
		return
	}

	for _, user := range strings.Split(*AuthorizedDDLUsers, ",") {
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
