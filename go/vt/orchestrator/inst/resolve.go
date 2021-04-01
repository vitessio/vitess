/*
   Copyright 2014 Outbrain Inc.

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

package inst

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

type HostnameResolve struct {
	hostname         string
	resolvedHostname string
}

func (this HostnameResolve) String() string {
	return fmt.Sprintf("%s %s", this.hostname, this.resolvedHostname)
}

type HostnameUnresolve struct {
	hostname           string
	unresolvedHostname string
}

func (this HostnameUnresolve) String() string {
	return fmt.Sprintf("%s %s", this.hostname, this.unresolvedHostname)
}

type HostnameRegistration struct {
	CreatedAt time.Time
	Key       InstanceKey
	Hostname  string
}

func NewHostnameRegistration(instanceKey *InstanceKey, hostname string) *HostnameRegistration {
	return &HostnameRegistration{
		CreatedAt: time.Now(),
		Key:       *instanceKey,
		Hostname:  hostname,
	}
}

func NewHostnameDeregistration(instanceKey *InstanceKey) *HostnameRegistration {
	return &HostnameRegistration{
		CreatedAt: time.Now(),
		Key:       *instanceKey,
		Hostname:  "",
	}
}

var hostnameResolvesLightweightCache *cache.Cache
var hostnameResolvesLightweightCacheInit = &sync.Mutex{}
var hostnameResolvesLightweightCacheLoadedOnceFromDB bool = false
var hostnameIPsCache = cache.New(10*time.Minute, time.Minute)

func init() {
	if config.Config.ExpiryHostnameResolvesMinutes < 1 {
		config.Config.ExpiryHostnameResolvesMinutes = 1
	}
}

func getHostnameResolvesLightweightCache() *cache.Cache {
	hostnameResolvesLightweightCacheInit.Lock()
	defer hostnameResolvesLightweightCacheInit.Unlock()
	if hostnameResolvesLightweightCache == nil {
		hostnameResolvesLightweightCache = cache.New(time.Duration(config.Config.ExpiryHostnameResolvesMinutes)*time.Minute, time.Minute)
	}
	return hostnameResolvesLightweightCache
}

func HostnameResolveMethodIsNone() bool {
	return strings.ToLower(config.Config.HostnameResolveMethod) == "none"
}

// GetCNAME resolves an IP or hostname into a normalized valid CNAME
func GetCNAME(hostname string) (string, error) {
	res, err := net.LookupCNAME(hostname)
	if err != nil {
		return hostname, err
	}
	res = strings.TrimRight(res, ".")
	return res, nil
}

func resolveHostname(hostname string) (string, error) {
	switch strings.ToLower(config.Config.HostnameResolveMethod) {
	case "none":
		return hostname, nil
	case "default":
		return hostname, nil
	case "cname":
		return GetCNAME(hostname)
	case "ip":
		return getHostnameIP(hostname)
	}
	return hostname, nil
}

// Attempt to resolve a hostname. This may return a database cached hostname or otherwise
// it may resolve the hostname via CNAME
func ResolveHostname(hostname string) (string, error) {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return hostname, errors.New("Will not resolve empty hostname")
	}
	if strings.Contains(hostname, ",") {
		return hostname, fmt.Errorf("Will not resolve multi-hostname: %+v", hostname)
	}
	if (&InstanceKey{Hostname: hostname}).IsDetached() {
		// quietly abort. Nothing to do. The hostname is detached for a reason: it
		// will not be resolved, for sure.
		return hostname, nil
	}

	// First go to lightweight cache
	if resolvedHostname, found := getHostnameResolvesLightweightCache().Get(hostname); found {
		return resolvedHostname.(string), nil
	}

	if !hostnameResolvesLightweightCacheLoadedOnceFromDB {
		// A continuous-discovery will first make sure to load all resolves from DB.
		// However cli does not do so.
		// Anyway, it seems like the cache was not loaded from DB. Before doing real resolves,
		// let's try and get the resolved hostname from database.
		if !HostnameResolveMethodIsNone() {
			go func() {
				if resolvedHostname, err := ReadResolvedHostname(hostname); err == nil && resolvedHostname != "" {
					getHostnameResolvesLightweightCache().Set(hostname, resolvedHostname, 0)
				}
			}()
		}
	}

	// Unfound: resolve!
	log.Debugf("Hostname unresolved yet: %s", hostname)
	resolvedHostname, err := resolveHostname(hostname)
	if config.Config.RejectHostnameResolvePattern != "" {
		// Reject, don't even cache
		if matched, _ := regexp.MatchString(config.Config.RejectHostnameResolvePattern, resolvedHostname); matched {
			log.Warningf("ResolveHostname: %+v resolved to %+v but rejected due to RejectHostnameResolvePattern '%+v'", hostname, resolvedHostname, config.Config.RejectHostnameResolvePattern)
			return hostname, nil
		}
	}

	if err != nil {
		// Problem. What we'll do is cache the hostname for just one minute, so as to avoid flooding requests
		// on one hand, yet make it refresh shortly on the other hand. Anyway do not write to database.
		getHostnameResolvesLightweightCache().Set(hostname, resolvedHostname, time.Minute)
		return hostname, err
	}
	// Good result! Cache it, also to DB
	log.Debugf("Cache hostname resolve %s as %s", hostname, resolvedHostname)
	go UpdateResolvedHostname(hostname, resolvedHostname)
	return resolvedHostname, nil
}

// UpdateResolvedHostname will store the given resolved hostname in cache
// Returns false when the key already existed with same resolved value (similar
// to AFFECTED_ROWS() in mysql)
func UpdateResolvedHostname(hostname string, resolvedHostname string) bool {
	if resolvedHostname == "" {
		return false
	}
	if existingResolvedHostname, found := getHostnameResolvesLightweightCache().Get(hostname); found && (existingResolvedHostname == resolvedHostname) {
		return false
	}
	getHostnameResolvesLightweightCache().Set(hostname, resolvedHostname, 0)
	if !HostnameResolveMethodIsNone() {
		WriteResolvedHostname(hostname, resolvedHostname)
	}
	return true
}

func LoadHostnameResolveCache() error {
	if !HostnameResolveMethodIsNone() {
		return loadHostnameResolveCacheFromDatabase()
	}
	return nil
}

func loadHostnameResolveCacheFromDatabase() error {
	allHostnamesResolves, err := ReadAllHostnameResolves()
	if err != nil {
		return err
	}
	for _, hostnameResolve := range allHostnamesResolves {
		getHostnameResolvesLightweightCache().Set(hostnameResolve.hostname, hostnameResolve.resolvedHostname, 0)
	}
	hostnameResolvesLightweightCacheLoadedOnceFromDB = true
	return nil
}

func FlushNontrivialResolveCacheToDatabase() error {
	if HostnameResolveMethodIsNone() {
		return nil
	}
	items, _ := HostnameResolveCache()
	for hostname := range items {
		resolvedHostname, found := getHostnameResolvesLightweightCache().Get(hostname)
		if found && (resolvedHostname.(string) != hostname) {
			WriteResolvedHostname(hostname, resolvedHostname.(string))
		}
	}
	return nil
}

func ResetHostnameResolveCache() error {
	err := deleteHostnameResolves()
	getHostnameResolvesLightweightCache().Flush()
	hostnameResolvesLightweightCacheLoadedOnceFromDB = false
	return err
}

func HostnameResolveCache() (map[string]cache.Item, error) {
	return getHostnameResolvesLightweightCache().Items(), nil
}

func UnresolveHostname(instanceKey *InstanceKey) (InstanceKey, bool, error) {
	if *config.RuntimeCLIFlags.SkipUnresolve {
		return *instanceKey, false, nil
	}
	unresolvedHostname, err := readUnresolvedHostname(instanceKey.Hostname)
	if err != nil {
		return *instanceKey, false, log.Errore(err)
	}
	if unresolvedHostname == instanceKey.Hostname {
		// unchanged. Nothing to do
		return *instanceKey, false, nil
	}
	// We unresovled to a different hostname. We will now re-resolve to double-check!
	unresolvedKey := &InstanceKey{Hostname: unresolvedHostname, Port: instanceKey.Port}

	instance, err := ReadTopologyInstance(unresolvedKey)
	if err != nil {
		return *instanceKey, false, log.Errore(err)
	}
	if instance.IsBinlogServer() && config.Config.SkipBinlogServerUnresolveCheck {
		// Do nothing. Everything is assumed to be fine.
	} else if instance.Key.Hostname != instanceKey.Hostname {
		// Resolve(Unresolve(hostname)) != hostname ==> Bad; reject
		if *config.RuntimeCLIFlags.SkipUnresolveCheck {
			return *instanceKey, false, nil
		}
		return *instanceKey, false, log.Errorf("Error unresolving; hostname=%s, unresolved=%s, re-resolved=%s; mismatch. Skip/ignore with --skip-unresolve-check", instanceKey.Hostname, unresolvedKey.Hostname, instance.Key.Hostname)
	}
	return *unresolvedKey, true, nil
}

func RegisterHostnameUnresolve(registration *HostnameRegistration) (err error) {
	if registration.Hostname == "" {
		return DeleteHostnameUnresolve(&registration.Key)
	}
	if registration.CreatedAt.Add(time.Duration(config.Config.ExpiryHostnameResolvesMinutes) * time.Minute).Before(time.Now()) {
		// already expired.
		return nil
	}
	return WriteHostnameUnresolve(&registration.Key, registration.Hostname)
}

func extractIPs(ips []net.IP) (ipv4String string, ipv6String string) {
	for _, ip := range ips {
		if ip4 := ip.To4(); ip4 != nil {
			ipv4String = ip.String()
		} else {
			ipv6String = ip.String()
		}
	}
	return ipv4String, ipv6String
}

func getHostnameIPs(hostname string) (ips []net.IP, fromCache bool, err error) {
	if ips, found := hostnameIPsCache.Get(hostname); found {
		return ips.([]net.IP), true, nil
	}
	ips, err = net.LookupIP(hostname)
	if err != nil {
		return ips, false, log.Errore(err)
	}
	hostnameIPsCache.Set(hostname, ips, cache.DefaultExpiration)
	return ips, false, nil
}

func getHostnameIP(hostname string) (ipString string, err error) {
	ips, _, err := getHostnameIPs(hostname)
	if err != nil {
		return ipString, err
	}
	ipv4String, ipv6String := extractIPs(ips)
	if ipv4String != "" {
		return ipv4String, nil
	}
	return ipv6String, nil
}

func ResolveHostnameIPs(hostname string) error {
	ips, fromCache, err := getHostnameIPs(hostname)
	if err != nil {
		return err
	}
	if fromCache {
		return nil
	}
	ipv4String, ipv6String := extractIPs(ips)
	return writeHostnameIPs(hostname, ipv4String, ipv6String)
}
