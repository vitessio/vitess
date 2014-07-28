package tableacl

import "strings"

// Role defines the level of access on a table
type Role int

const (
	// READER can run SELECT statements
	READER Role = iota
	// WRITER can run SELECT, INSERT & UPDATE statements
	WRITER
	// ADMIN can run any statements including DDLs
	ADMIN
	// NumRoles is number of Roles defined
	NumRoles
)

var roleNames = []string{
	"READER",
	"WRITER",
	"ADMIN",
}

// Name returns the name of a role
func (r Role) Name() string {
	if r < READER || r > ADMIN {
		return ""
	}
	return roleNames[r]
}

// RoleByName returns the Role corresponding to a name
func RoleByName(s string) (Role, bool) {
	for i, v := range roleNames {
		if v == strings.ToUpper(s) {
			return Role(i), true
		}
	}
	return NumRoles, false
}
