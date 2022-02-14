/*
 Detect server flavors and capabilities
*/

package mysqlctl

type serverVersion struct {
	Major, Minor, Patch int
}

func (v *serverVersion) atLeast(compare serverVersion) bool {
	if v.Major > compare.Major {
		return true
	}
	if v.Major == compare.Major && v.Minor > compare.Minor {
		return true
	}
	if v.Major == compare.Major && v.Minor == compare.Minor && v.Patch >= compare.Patch {
		return true
	}
	return false
}
