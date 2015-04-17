// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tableacl

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
)

var mu sync.Mutex
var tableAcl map[*regexp.Regexp]map[Role]acl.ACL
var acls = make(map[string]acl.Factory)

// defaultACL tells the default ACL implementation to use.
var defaultACL string

// Init initiates table ACLs.
func Init(configFile string) {
	config, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Errorf("unable to read tableACL config file: %v", err)
		panic(fmt.Errorf("unable to read tableACL config file: %v", err))
	}
	tableAcl, err = load(config)
	if err != nil {
		log.Errorf("tableACL initialization error: %v", err)
		panic(fmt.Errorf("tableACL initialization error: %v", err))
	}
}

// InitFromBytes inits table ACLs from a byte array.
func InitFromBytes(config []byte) (err error) {
	tableAcl, err = load(config)
	return
}

// load loads configurations from a JSON byte array
//
// Sample configuration
// []byte (`{
//	<tableRegexPattern1>: {"READER": "*", "WRITER": "<u2>,<u4>...","ADMIN": "<u5>"},
//	<tableRegexPattern2>: {"ADMIN": "<u5>"}
//}`)
func load(config []byte) (map[*regexp.Regexp]map[Role]acl.ACL, error) {
	var contents map[string]map[string]string
	err := json.Unmarshal(config, &contents)
	if err != nil {
		return nil, err
	}
	tableAcl := make(map[*regexp.Regexp]map[Role]acl.ACL)
	for tblPattern, accessMap := range contents {
		re, err := regexp.Compile(tblPattern)
		if err != nil {
			return nil, fmt.Errorf("regexp compile error %v: %v", tblPattern, err)
		}
		tableAcl[re] = make(map[Role]acl.ACL)

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
			a, err := newACL(entries)
			if err != nil {
				return nil, err
			}
			tableAcl[re][r] = a
		}

	}
	return tableAcl, nil
}

// Authorized returns the list of entities who have at least the
// minimum specified Role on a tablel.
func Authorized(table string, minRole Role) acl.ACL {
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

// Register registers a AclFactory.
func Register(name string, factory acl.Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := acls[name]; ok {
		panic(fmt.Sprintf("register a registered key: %s", name))
	}
	acls[name] = factory
}

// SetDefaultACL sets the default ACL implementation.
func SetDefaultACL(name string) {
	mu.Lock()
	defer mu.Unlock()
	defaultACL = name
}

// GetCurrentAclFactory returns current table acl implementation.
func GetCurrentAclFactory() acl.Factory {
	mu.Lock()
	defer mu.Unlock()
	if defaultACL == "" {
		if len(acls) == 1 {
			for _, aclFactory := range acls {
				return aclFactory
			}
		}
		panic("there are more than one AclFactory " +
			"registered but no default has been given.")
	}
	aclFactory, ok := acls[defaultACL]
	if !ok {
		panic(fmt.Sprintf("aclFactory for given default: %s is not found.", defaultACL))
	}
	return aclFactory
}

func newACL(entries []string) (acl.ACL, error) {
	return GetCurrentAclFactory().New(entries)
}

func all() acl.ACL {
	return GetCurrentAclFactory().All()
}
