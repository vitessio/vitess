/*
Copyright 2019 The Vitess Authors.

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

// Package netutil contains network-related utility functions.
package netutil

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

// SplitHostPort is an alternative to net.SplitHostPort that also parses the
// integer port. In addition, it is more tolerant of improperly escaped IPv6
// addresses, such as "::1:456", which should actually be "[::1]:456".
func SplitHostPort(addr string) (string, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// If the above proper parsing fails, fall back on a naive split.
		i := strings.LastIndex(addr, ":")
		if i < 0 {
			return "", 0, fmt.Errorf("SplitHostPort: missing port in %q", addr)
		}
		host = addr[:i]
		port = addr[i+1:]
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("SplitHostPort: can't parse port %q: %v", port, err)
	}
	return host, int(p), nil
}

// JoinHostPort is an extension to net.JoinHostPort that also formats the
// integer port.
func JoinHostPort(host string, port int32) string {
	return net.JoinHostPort(host, strconv.FormatInt(int64(port), 10))
}

// FullyQualifiedHostname returns the FQDN of the machine.
func FullyQualifiedHostname() (string, error) {
	// The machine hostname (which is also returned by os.Hostname()) may not be
	// set to the FQDN, but only the first part of it e.g. "localhost" instead of
	// "localhost.localdomain".
	// To get the full FQDN, we do the following:

	// 1. Get the machine hostname. Example: localhost
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("FullyQualifiedHostname: failed to retrieve the hostname of this machine: %v", err)
	}

	// 2. Look up the IP address for that hostname. Example: 127.0.0.1
	ips, err := net.LookupHost(hostname)
	if err != nil {
		return "", fmt.Errorf("FullyQualifiedHostname: failed to lookup the IP of this machine's hostname (%v): %v", hostname, err)
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("FullyQualifiedHostname: lookup of the IP of this machine's hostname (%v) did not return any IP address", hostname)
	}
	// If multiple IPs are returned, we only look at the first one.
	localIP := ips[0]

	// 3. Reverse lookup the IP. Example: localhost.localdomain
	resolvedHostnames, err := net.LookupAddr(localIP)
	if err != nil {
		return "", fmt.Errorf("FullyQualifiedHostname: failed to reverse lookup this machine's local IP (%v): %v", localIP, err)
	}
	if len(resolvedHostnames) == 0 {
		return "", fmt.Errorf("FullyQualifiedHostname: reverse lookup of this machine's local IP (%v) did not return any hostnames", localIP)
	}
	// If multiple hostnames are found, we return only the first one.
	// If multiple hostnames are listed e.g. in an entry in the /etc/hosts file,
	// the current Go implementation returns them in that order.
	// Example for an /etc/hosts entry:
	//   127.0.0.1	localhost.localdomain localhost
	// If the FQDN isn't returned by this function, check the order in the entry
	// in your /etc/hosts file.
	return strings.TrimSuffix(resolvedHostnames[0], "."), nil
}

// FullyQualifiedHostnameOrPanic is the same as FullyQualifiedHostname
// but panics in case of an error.
func FullyQualifiedHostnameOrPanic() string {
	hostname, err := FullyQualifiedHostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

func dnsLookup(host string) ([]net.IP, error) {
	addrs, err := net.LookupHost(host)
	if err != nil {
		return nil, fmt.Errorf("Error looking up dns name [%v]: (%v)", host, err)
	}
	naddr := make([]net.IP, len(addrs))
	for i, a := range addrs {
		naddr[i] = net.ParseIP(a)
	}
	sort.Slice(naddr, func(i, j int) bool {
		return bytes.Compare(naddr[i], naddr[j]) < 0
	})
	return naddr, nil
}

// DNSTracker is a closure that persists state for
//
//	tracking changes in the DNS resolution of a target dns name
//	returns true if the DNS name resolution has changed
//	If there is a lookup problem, we pretend nothing has changed
func DNSTracker(host string) func() (bool, error) {
	dnsName := host
	var addrs []net.IP
	if dnsName != "" {
		addrs, _ = dnsLookup(dnsName)
	}

	return func() (bool, error) {
		if dnsName == "" {
			return false, nil
		}
		newaddrs, err := dnsLookup(dnsName)
		if err != nil {
			return false, err
		}
		if len(newaddrs) == 0 { // Should not happen, but just in case
			return false, fmt.Errorf("Connection DNS for %s reporting as empty, ignoring", dnsName)
		}
		if !addrEqual(addrs, newaddrs) {
			oldaddr := addrs
			addrs = newaddrs // Update the closure variable
			return true, fmt.Errorf("Connection DNS for %s has changed; old: %v  new: %v", dnsName, oldaddr, newaddrs)
		}
		return false, nil
	}
}

func addrEqual(a, b []net.IP) bool {
	if len(a) != len(b) {
		return false
	}
	for idx, v := range a {
		if !net.IP.Equal(v, b[idx]) {
			return false
		}
	}
	return true
}

// NormalizeIP normalizes loopback addresses to avoid spurious errors when
// communicating to different representations of the loopback.
//
// Note: this also maps IPv6 localhost to IPv4 localhost, as
// TabletManagerClient.GetReplicas() (the only place this function is used on)
// will return only IPv4 addresses.
func NormalizeIP(s string) string {
	if ip := net.ParseIP(s); ip != nil && ip.IsLoopback() {
		return "127.0.0.1"
	}

	return s
}
