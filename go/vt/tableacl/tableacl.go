// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tableacl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/tableacl/acl"

	tableaclpb "github.com/youtube/vitess/go/vt/proto/tableacl"
)

// ACLResult embeds an acl.ACL and also tell which table group it belongs to.
type ACLResult struct {
	acl.ACL
	GroupName string
}

type aclEntry struct {
	tableNameOrPrefix string
	groupName         string
	acl               map[Role]acl.ACL
}

type aclEntries []aclEntry

func (aes aclEntries) Len() int {
	return len(aes)
}

func (aes aclEntries) Less(i, j int) bool {
	return aes[i].tableNameOrPrefix < aes[j].tableNameOrPrefix
}

func (aes aclEntries) Swap(i, j int) {
	aes[i], aes[j] = aes[j], aes[i]
}

// mu protects acls and defaultACL.
var mu sync.Mutex

var acls = make(map[string]acl.Factory)

// defaultACL tells the default ACL implementation to use.
var defaultACL string

type tableACL struct {
	sync.RWMutex
	entries aclEntries
	config  tableaclpb.Config
}

// currentACL stores current effective ACL information.
var currentACL tableACL

// aclCallback is the tablet callback executed on table acl reload.
var aclCallback func()

// Init initiates table ACLs.
//
// The config file can be binary-proto-encoded, or json-encoded.
// In the json case, it looks like this:
//
// {
//   "table_groups": [
//     {
//       "table_names_or_prefixes": ["name1"],
//       "readers": ["client1"],
//       "writers": ["client1"],
//       "admins": ["client1"]
//     }
//   ]
// }
func Init(configFile string, aclCB func()) error {
	aclCallback = aclCB
	if configFile != "" {
		log.Infof("Loading Table ACL from local file: %v", configFile)
		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			log.Infof("unable to read tableACL config file: %v", err)
			return err
		}
		config := &tableaclpb.Config{}
		if err := proto.Unmarshal(data, config); err != nil {
			log.Infof("unable to parse tableACL config file as a protobuf file: %v", err)
			// try to parse tableacl as json file
			if jsonErr := json.Unmarshal(data, config); jsonErr != nil {
				log.Infof("unable to parse tableACL config file as a json file: %v", jsonErr)
				return fmt.Errorf("Unable to unmarshal Table ACL data: %v", data)
			}
		}
		if err = load(config); err != nil {
			log.Infof("tableACL initialization error: %v", err)
			return err
		}
	}
	return nil
}

// InitFromProto inits table ACLs from a proto.
func InitFromProto(config *tableaclpb.Config) (err error) {
	return load(config)
}

// load loads configurations from a proto-defined Config
func load(config *tableaclpb.Config) error {
	var entries aclEntries
	for _, group := range config.TableGroups {
		readers, err := newACL(group.Readers)
		if err != nil {
			return err
		}
		writers, err := newACL(group.Writers)
		if err != nil {
			return err
		}
		admins, err := newACL(group.Admins)
		if err != nil {
			return err
		}
		for _, tableNameOrPrefix := range group.TableNamesOrPrefixes {
			entries = append(entries, aclEntry{
				tableNameOrPrefix: tableNameOrPrefix,
				groupName:         group.Name,
				acl: map[Role]acl.ACL{
					READER: readers,
					WRITER: writers,
					ADMIN:  admins,
				},
			})
		}
	}
	sort.Sort(entries)
	if err := validate(entries); err != nil {
		return err
	}
	currentACL.Lock()
	currentACL.entries = entries
	currentACL.config = *config
	defer func() {
		currentACL.Unlock()
		if aclCallback != nil {
			aclCallback()
		}
	}()

	return nil
}

func validate(entries aclEntries) error {
	if len(entries) == 0 {
		return nil
	}
	if !sort.IsSorted(entries) {
		return errors.New("acl entries are not sorted by table name or prefix")
	}
	if err := validateNameOrPrefix(entries[0].tableNameOrPrefix); err != nil {
		return err
	}
	for i := 1; i < len(entries); i++ {
		prev := entries[i-1].tableNameOrPrefix
		cur := entries[i].tableNameOrPrefix
		if err := validateNameOrPrefix(cur); err != nil {
			return err
		}
		if prev == cur {
			return fmt.Errorf("conflict entries, name: %s overlaps with name: %s", cur, prev)
		} else if isPrefix(prev) && isPrefix(cur) {
			if strings.HasPrefix(cur[:len(cur)-1], prev[:len(prev)-1]) {
				return fmt.Errorf("conflict entries, prefix: %s overlaps with prefix: %s", cur, prev)
			}
		} else if isPrefix(prev) {
			if strings.HasPrefix(cur, prev[:len(prev)-1]) {
				return fmt.Errorf("conflict entries, name: %s overlaps with prefix: %s", cur, prev)
			}
		} else if isPrefix(cur) {
			if strings.HasPrefix(prev, cur[:len(cur)-1]) {
				return fmt.Errorf("conflict entries, prefix: %s overlaps with name: %s", cur, prev)
			}
		}
	}
	return nil
}

func isPrefix(nameOrPrefix string) bool {
	length := len(nameOrPrefix)
	return length > 0 && nameOrPrefix[length-1] == '%'
}

func validateNameOrPrefix(nameOrPrefix string) error {
	for i := 0; i < len(nameOrPrefix)-1; i++ {
		if nameOrPrefix[i] == '%' {
			return fmt.Errorf("got: %s, '%%' means this entry is a prefix and should not appear in the middle of name or prefix", nameOrPrefix)
		}
	}
	return nil
}

// Authorized returns the list of entities who have the specified role on a tablel.
func Authorized(table string, role Role) *ACLResult {
	currentACL.RLock()
	defer currentACL.RUnlock()
	start := 0
	end := len(currentACL.entries)
	for start < end {
		mid := start + (end-start)/2
		val := currentACL.entries[mid].tableNameOrPrefix
		if table == val || (strings.HasSuffix(val, "%") && strings.HasPrefix(table, val[:len(val)-1])) {
			acl, ok := currentACL.entries[mid].acl[role]
			if ok {
				return &ACLResult{
					ACL:       acl,
					GroupName: currentACL.entries[mid].groupName,
				}
			}
			break
		} else if table < val {
			end = mid
		} else {
			start = mid + 1
		}
	}
	return &ACLResult{
		ACL:       acl.DenyAllACL{},
		GroupName: "",
	}
}

// GetCurrentConfig returns a copy of current tableacl configuration.
func GetCurrentConfig() *tableaclpb.Config {
	config := &tableaclpb.Config{}
	currentACL.RLock()
	defer currentACL.RUnlock()
	*config = currentACL.config
	return config
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
func GetCurrentAclFactory() (acl.Factory, error) {
	mu.Lock()
	defer mu.Unlock()
	if defaultACL == "" {
		if len(acls) == 1 {
			for _, aclFactory := range acls {
				return aclFactory, nil
			}
		}
		return nil, errors.New("there are more than one AclFactory registered but no default has been given")
	}
	if aclFactory, ok := acls[defaultACL]; ok {
		return aclFactory, nil
	}
	return nil, fmt.Errorf("aclFactory for given default: %s is not found", defaultACL)
}

func newACL(entries []string) (_ acl.ACL, err error) {
	if f, err := GetCurrentAclFactory(); err == nil {
		return f.New(entries)
	}
	return nil, err
}
