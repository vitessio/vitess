package tableacl

var allAcl SimpleACL

const (
	ALL = "*"
)

// NewACL returns an ACL with the specified entries
func NewACL(entries []string) (ACL, error) {
	a := SimpleACL(map[string]bool{})
	for _, e := range entries {
		a[e] = true
	}
	return a, nil
}

// SimpleACL keeps all entries in a unique in-memory list
type SimpleACL map[string]bool

// Check checks the membership of a principal in this ACL
func (a SimpleACL) IsMember(principal string) bool {
	return a[principal] || a[ALL]
}

func all() ACL {
	if allAcl == nil {
		allAcl = SimpleACL(map[string]bool{ALL: true})
	}
	return allAcl
}
