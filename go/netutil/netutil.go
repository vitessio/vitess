// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
This packages contains a few utility functions for network related functions.
*/
package netutil

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

// byPriorityWeight sorts records by ascending priority and weight.
type byPriorityWeight []*net.SRV

func (s byPriorityWeight) Len() int { return len(s) }

func (s byPriorityWeight) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s byPriorityWeight) Less(i, j int) bool {
	return s[i].Priority < s[j].Priority ||
		(s[i].Priority == s[j].Priority && s[i].Weight < s[j].Weight)
}

// shuffleByWeight shuffles SRV records by weight using the algorithm
// described in RFC 2782.
func (addrs byPriorityWeight) shuffleByWeight() {
	sum := 0
	for _, addr := range addrs {
		sum += int(addr.Weight)
	}
	for sum > 0 && len(addrs) > 1 {
		s := 0
		n := rand.Intn(sum + 1)
		for i := range addrs {
			s += int(addrs[i].Weight)
			if s >= n {
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

// SplitHostPort is an extension to net.SplitHostPort that also parses the
// integer port
func SplitHostPort(addr string) (string, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	p, err := strconv.ParseInt(port, 10, 16)
	if err != nil {
		return "", 0, err
	}
	return host, int(p), nil
}

// FullyQualifiedHostname returns the full hostname with domain
func FullyQualifiedHostname() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	cname, err := net.LookupCNAME(hostname)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(cname, "."), nil
}

// FullyQualifiedHostnameOrPanic is the same as FullyQualifiedHostname
// but panics in case of error
func FullyQualifiedHostnameOrPanic() string {
	hostname, err := FullyQualifiedHostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

// ResolveAddr can resolve an address where the host has been left
// blank, like ":3306"
func ResolveAddr(addr string) (string, error) {
	host, port, err := SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host, err = FullyQualifiedHostname()
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%v:%v", host, port), nil
}

// ResolveIpAddr resolves the address:port part into an IP address:port pair
func ResolveIpAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	ipAddrs, err := net.LookupHost(host)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(ipAddrs[0], port), nil
}
