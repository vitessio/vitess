/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tableacl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/tchap/go-patricia/patricia"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

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
	// mutex protects entries, config, and callback
	sync.RWMutex
	entries aclEntries
	config  tableaclpb.Config
	// callback is executed on successful reload.
	callback func()
	// ACL Factory override for testing
	factory acl.Factory
}

// currentTableACL stores current effective ACL information.
var currentTableACL tableACL

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
	return currentTableACL.init(configFile, aclCB)
}

func (tacl *tableACL) init(configFile string, aclCB func()) error {
	tacl.SetCallback(aclCB)
	if configFile == "" {
		return nil
	}
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
	return tacl.Set(config)
}

func (tacl *tableACL) SetCallback(callback func()) {
	tacl.Lock()
	defer tacl.Unlock()
	tacl.callback = callback
}

// InitFromProto inits table ACLs from a proto.
func InitFromProto(config *tableaclpb.Config) error {
	return currentTableACL.Set(config)
}

// load loads configurations from a proto-defined Config
// If err is nil, then entries is guaranteed to be non-nil (though possibly empty).
func load(config *tableaclpb.Config, newACL func([]string) (acl.ACL, error)) (entries aclEntries, err error) {
	if err := ValidateProto(config); err != nil {
		return nil, err
	}
	entries = aclEntries{}
	for _, group := range config.TableGroups {
		readers, err := newACL(group.Readers)
		if err != nil {
			return nil, err
		}
		writers, err := newACL(group.Writers)
		if err != nil {
			return nil, err
		}
		admins, err := newACL(group.Admins)
		if err != nil {
			return nil, err
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
	return entries, nil
}

func (tacl *tableACL) aclFactory() (acl.Factory, error) {
	if tacl.factory == nil {
		return GetCurrentAclFactory()
	}
	return tacl.factory, nil
}

func (tacl *tableACL) Set(config *tableaclpb.Config) error {
	factory, err := tacl.aclFactory()
	if err != nil {
		return err
	}
	entries, err := load(config, factory.New)
	if err != nil {
		return err
	}
	tacl.Lock()
	tacl.entries = entries
	tacl.config = *config
	callback := tacl.callback
	tacl.Unlock()
	if callback != nil {
		callback()
	}
	return nil
}

// Valid returns whether the tableACL is valid.
// Currently it only checks that it has been initialized.
func (tacl *tableACL) Valid() bool {
	tacl.RLock()
	defer tacl.RUnlock()
	return tacl.entries != nil
}

// ValidateProto returns an error if the given proto has problems
// that would cause InitFromProto to fail.
func ValidateProto(config *tableaclpb.Config) (err error) {
	t := patricia.NewTrie()
	for _, group := range config.TableGroups {
		for _, name := range group.TableNamesOrPrefixes {
			var prefix patricia.Prefix
			if strings.HasSuffix(name, "%") {
				prefix = []byte(strings.TrimSuffix(name, "%"))
			} else {
				prefix = []byte(name + "\000")
			}
			if bytes.Contains(prefix, []byte("%")) {
				return fmt.Errorf("got: %s, '%%' means this entry is a prefix and should not appear in the middle of name or prefix", name)
			}
			overlapVisitor := func(_ patricia.Prefix, item patricia.Item) error {
				return fmt.Errorf("conflicting entries: %q overlaps with %q", name, item)
			}
			if err := t.VisitSubtree(prefix, overlapVisitor); err != nil {
				return err
			}
			if err := t.VisitPrefixes(prefix, overlapVisitor); err != nil {
				return err
			}
			t.Insert(prefix, name)
		}
	}
	return nil
}

// Authorized returns the list of entities who have the specified role on a tablel.
func Authorized(table string, role Role) *ACLResult {
	return currentTableACL.Authorized(table, role)
}

func (tacl *tableACL) Authorized(table string, role Role) *ACLResult {
	tacl.RLock()
	defer tacl.RUnlock()
	start := 0
	end := len(tacl.entries)
	for start < end {
		mid := start + (end-start)/2
		val := tacl.entries[mid].tableNameOrPrefix
		if table == val || (strings.HasSuffix(val, "%") && strings.HasPrefix(table, val[:len(val)-1])) {
			acl, ok := tacl.entries[mid].acl[role]
			if ok {
				return &ACLResult{
					ACL:       acl,
					GroupName: tacl.entries[mid].groupName,
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
	return currentTableACL.Config()
}

func (tacl *tableACL) Config() *tableaclpb.Config {
	tacl.RLock()
	defer tacl.RUnlock()
	return proto.Clone(&tacl.config).(*tableaclpb.Config)
}

// Register registers an AclFactory.
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
	if len(acls) == 0 {
		return nil, fmt.Errorf("no AclFactories registered")
	}
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

func checkHealth(acl *tableACL) error {
	if !acl.Valid() {
		return errors.New("the tableacl is not valid")
	}
	return nil
}

func init() {
	servenv.OnRun(func() {
		if !tabletenv.Config.StrictTableACL {
			return
		}
		if tabletenv.Config.EnableTableACLDryRun {
			return
		}
		health.DefaultAggregator.RegisterSimpleCheck("tableacl", func() error { return checkHealth(&currentTableACL) })
	})
}
