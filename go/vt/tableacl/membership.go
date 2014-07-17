package tableacl

// Check checks the membership of user in a list
func Check(user string, acl map[string]bool) bool {
	return acl[user] || acl[ALL]
}
