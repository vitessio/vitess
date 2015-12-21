// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package netutil contains network-related utility functions.
package netutil

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// byPriorityWeight sorts records by ascending priority and weight.
type byPriorityWeight []*net.SRV

func (addrs byPriorityWeight) Len() int { return len(addrs) }

func (addrs byPriorityWeight) Swap(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] }

func (addrs byPriorityWeight) Less(i, j int) bool {
	return addrs[i].Priority < addrs[j].Priority ||
		(addrs[i].Priority == addrs[j].Priority && addrs[i].Weight < addrs[j].Weight)
}

// shuffleByWeight shuffles SRV records by weight using the algorithm
// described in RFC 2782.
// NOTE(msolo) This is disabled when the weights are zero.
func (addrs byPriorityWeight) shuffleByWeight() {
	sum := 0
	for _, addr := range addrs {
		sum += int(addr.Weight)
	}
	for sum > 0 && len(addrs) > 1 {
		s := 0
		n := rand.Intn(sum)
		for i := range addrs {
			s += int(addrs[i].Weight)
			if s > n {
				if i > 0 {
					t := addrs[i]
					copy(addrs[1:i+1], addrs[0:i])
					addrs[0] = t
				}
				break
			}
		}
		sum -= int(addrs[0].Weight)
		addrs = addrs[1:]
	}
}

func (addrs byPriorityWeight) sortRfc2782() {
	sort.Sort(addrs)
	i := 0
	for j := 1; j < len(addrs); j++ {
		if addrs[i].Priority != addrs[j].Priority {
			addrs[i:j].shuffleByWeight()
			i = j
		}
	}
	addrs[i:].shuffleByWeight()
}

// SortRfc2782 reorders SRV records as specified in RFC 2782.
func SortRfc2782(srvs []*net.SRV) {
	byPriorityWeight(srvs).sortRfc2782()
}

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
	return strings.TrimRight(resolvedHostnames[0], "."), nil
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

// ResolveIPv4Addr resolves the address:port part into an IP address:port pair
func ResolveIPv4Addr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	ipAddrs, err := net.LookupIP(host)
	for _, ipAddr := range ipAddrs {
		ipv4 := ipAddr.To4()
		if ipv4 != nil {
			return net.JoinHostPort(ipv4.String(), port), nil
		}
	}
	return "", fmt.Errorf("no IPv4addr for name %v", host)
}
