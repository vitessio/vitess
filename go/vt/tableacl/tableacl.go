package tableacl

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
)

const (
	// ALL represents all users
	ALL = "*"
)

var acl map[*regexp.Regexp]map[Role][]string

// Init initiates table ACLs
func Init(configFile string) {
	config, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("unable to read tableACL config file: %v", err)
	}
	acl, err = load(config)
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
func load(config []byte) (map[*regexp.Regexp]map[Role][]string, error) {
	var contents map[string]map[string]string
	err := json.Unmarshal(config, &contents)
	if err != nil {
		return nil, err
	}
	acl := make(map[*regexp.Regexp]map[Role][]string)
	for tblPattern, accessMap := range contents {
		re, err := regexp.Compile(tblPattern)
		if err != nil {
			return nil, fmt.Errorf("regexp compile error %v: %v", tblPattern, err)
		}
		if _, ok := acl[re]; !ok {
			acl[re] = make(map[Role][]string)
		}
		for role, listStr := range accessMap {
			r, ok := RoleByName(role)
			if !ok {
				return nil, fmt.Errorf("parse error, invalid role %v", role)
			}
			acl[re][r] = strings.Split(listStr, ",")
		}
	}
	return acl, nil
}

// Authorized returns the list of entities who have at least the
// minimum specified Role on a table
func Authorized(table string, minRole Role) map[string]bool {
	all := map[string]bool{ALL: true}
	if acl == nil {
		// No ACLs, allow all
		return all
	}
	authorized := map[string]bool{}
	for re, accessMap := range acl {
		if !re.MatchString(table) {
			continue
		}
		for r := minRole; r < NumRoles; r++ {
			entries, ok := accessMap[r]
			if !ok {
				continue
			}
			for _, e := range entries {
				if e == ALL {
					return all
				}
				authorized[e] = true
			}
		}
		return authorized
	}
	// No matching patterns for table, allow all
	return all
}
