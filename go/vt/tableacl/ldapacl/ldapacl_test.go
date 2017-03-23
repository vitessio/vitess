package ldapacl

import (
	"testing"

	"github.com/youtube/vitess/go/vt/tableacl/testlib"
)

func TestLdapAcl(t *testing.T) {
	cache["DummyUser"] = []string{"ldap-grp1", "ldap-grp2"}
	testlib.TestSuite(t, &Factory{})
}
