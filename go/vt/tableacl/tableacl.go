package tableacl

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
)

// ACL is an interface for Access Control List
type ACL interface {
	// IsMember checks the membership of a principal in this ACL
	IsMember(principal string) bool
}

var tableAcl map[*regexp.Regexp]map[Role]ACL

// Init initiates table ACLs
func Init(configFile string) {
	config, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("unable to read tableACL config file: %v", err)
	}
	tableAcl, err = load(config)
	if err != nil {
		log.Fatalf("tableACL initialization error: %v", err)
	}
}

// load loads configurations from a JSON byte array
//
// Sample configuration
// []byte (`{
//	<tableRegexPattern1>: {"READER": "*", "WRITER": "<u2>,<u4>...","ADMIN": "<u5>"},
//	<tableRegexPattern2>: {"ADMIN": "<u5>"}
//}`)
func load(config []byte) (map[*regexp.Regexp]map[Role]ACL, error) {
	var contents map[string]map[string]string
	err := json.Unmarshal(config, &contents)
	if err != nil {
		return nil, err
	}
	tableAcl := make(map[*regexp.Regexp]map[Role]ACL)
	for tblPattern, accessMap := range contents {
		re, err := regexp.Compile(tblPattern)
		if err != nil {
			return nil, fmt.Errorf("regexp compile error %v: %v", tblPattern, err)
		}
		tableAcl[re] = make(map[Role]ACL)

		entriesByRole := make(map[Role][]string)
		for i := READER; i < NumRoles; i++ {
			entriesByRole[i] = []string{}
		}
		for role, entries := range accessMap {
			r, ok := RoleByName(role)
			if !ok {
				return nil, fmt.Errorf("parse error, invalid role %v", role)
			}
			// Entries must be assigned to all roles up to r
			for i := READER; i <= r; i++ {
				entriesByRole[i] = append(entriesByRole[i], strings.Split(entries, ",")...)
			}
		}
		for r, entries := range entriesByRole {
			a, err := NewACL(entries)
			if err != nil {
				return nil, err
			}
			tableAcl[re][r] = a
		}

	}
	return tableAcl, nil
}

// Authorized returns the list of entities who have at least the
// minimum specified Role on a table
func Authorized(table string, minRole Role) ACL {
	// If table ACL is disabled, return nil
	if tableAcl == nil {
		return nil
	}
	for re, accessMap := range tableAcl {
		if !re.MatchString(table) {
			continue
		}
		return accessMap[minRole]
	}
	// No matching patterns for table, allow all access
	return all()
}
