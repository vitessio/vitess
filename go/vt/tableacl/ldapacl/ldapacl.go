package ldapacl

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/ldap"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
	ldapv2 "gopkg.in/ldap.v2"
)

var (
	ldapACLConfigFile   = flag.String("mysql_ldap_acl_config_file", "", "JSON File from which to read LDAP server config.")
	ldapACLConfigString = flag.String("mysql_ldap_acl_config_string", "", "JSON representation of LDAP server config.")
)

var queryLdap func(string) ([]string, error)

// ACLLdapServerConfig holds the ACL-specific configuration for an LDAP connection
// It is recommended that User be read-only
type ACLLdapServerConfig struct {
	ldap.ServerConfig
	User     string
	Password string
	Query    string
}

func init() {
	if *ldapACLConfigFile == "" && *ldapACLConfigString == "" {
		log.Infof("Not configuring Ldap ACLs because mysql_ldap_acl_config_file and mysql_ldap_acl_config_string are empty")
		return
	}
	if *ldapACLConfigFile != "" && *ldapACLConfigString != "" {
		log.Infof("Both mysql_ldap_acl_config_file and mysql_ldap_acl_config_string are non-empty, can only use one.")
		return
	}
	config := &ACLLdapServerConfig{ldap.ServerConfig{}, "", "", ""}

	data := []byte(*ldapACLConfigString)
	if *ldapACLConfigFile != "" {
		var err error
		data, err = ioutil.ReadFile(*ldapACLConfigFile)
		if err != nil {
			log.Fatalf("Failed to read mysql_ldap_acl_config_file: %v", err)
		}
	}
	if err := json.Unmarshal(data, config); err != nil {
		log.Fatalf("Error parsing AuthServerLdap config: %v", err)
	}
	queryLdap = makeQueryClosure(config)
	tableacl.Register("ldapacl", &Factory{})
	tableacl.SetDefaultACL("ldapacl")
	go refreshCachedUsers()
}

// Factory creates new ACL instances
type Factory struct{}

// New creates a new ACL instance.
// accessGroups will be a list of ldap groups
// needs to return an object which has an IsMember(string) bool
func (factory *Factory) New(accessGroups []string) (acl.ACL, error) {
	lacl := LdapACL(map[string]bool{})
	for _, entry := range accessGroups {
		lacl[entry] = true
	}
	return lacl, nil
}

// LdapACL is essentially a list of ldap groups, stored as a map for read efficiency
type LdapACL map[string]bool

// LDAP cache for the whole tablet, rather than per role
//it would be better if this were in vtgate and the tablets didn't talk to ldap at all
// {username:[ldap-grp1, ldap-grp2], ...}
var cache = make(map[string][]string)

// IsMember checks if principal or any of principal's LDAP groups is in lacl
func (lacl LdapACL) IsMember(principal string) bool {
	if lacl[principal] {
		return true
	}
	var groups []string
	var ok bool
	if groups, ok = cache[principal]; !ok {
		var err error
		cache[principal], err = queryLdap(principal) //assignment is atomic...r-right?
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
	}
	for _, grp := range groups {
		if lacl[grp] {
			return true
		}
	}
	return false
}

func refreshCachedUsers() {
	for {
		for principal := range cache {
			var err error
			cache[principal], err = queryLdap(principal) //assignment is atomic...r-right?
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
		}
		time.Sleep(5000000000) //TODO(acharis): convert this to the vitess standard background thread machinery
	}
}

func makeQueryClosure(config *ACLLdapServerConfig) func(string) ([]string, error) {
	return func(principal string) ([]string, error) {
		conn := &ldap.ClientImpl{}
		err := conn.Connect("tcp", &config.ServerConfig)
		if err != nil {
			return nil, err
		}
		defer conn.Close() //keep it after the error check

		err = conn.Bind(config.User, config.Password)
		if err != nil {
			return nil, err
		}
		req := ldapv2.NewSearchRequest(
			config.Query,
			ldapv2.ScopeWholeSubtree, ldapv2.NeverDerefAliases, 0, 0, false,
			fmt.Sprintf("(memberUid=%s)", principal),
			[]string{"cn"},
			nil,
		)
		res, err := conn.Search(req)
		if err != nil {
			return nil, err
		}
		var groups []string
		for _, entry := range res.Entries {
			for _, attr := range entry.Attributes {
				groups = append(groups, attr.Values[0])
			}
		}
		return groups, nil
	}
}
