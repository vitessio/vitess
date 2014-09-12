package tableacl

var allAcl simpleACL

const (
	ALL = "*"
)

// NewACL returns an ACL with the specified entries
func NewACL(entries []string) (ACL, error) {
	a := simpleACL(map[string]bool{})
	for _, e := range entries {
		a[e] = true
	}
	return a, nil
}

// simpleACL keeps all entries in a unique in-memory list
type simpleACL map[string]bool

// IsMember checks the membership of a principal in this ACL
func (a simpleACL) IsMember(principal string) bool {
	return a[principal] || a[ALL]
}

func all() ACL {
	if allAcl == nil {
		allAcl = simpleACL(map[string]bool{ALL: true})
	}
	return allAcl
}
