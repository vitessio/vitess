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

// zk provides with higher level commands over the lower level zookeeper connector
package zk

import (
	"testing"

	"github.com/samuel/go-zookeeper/zk"
)

func TestParseACLString(t *testing.T) {
	cases := []struct {
		aclstr string
		want   []zk.ACL
	}{
		{"world:anyone:cdrwa", []zk.ACL{{Scheme: "world", ID: "anyone", Perms: 31}}},
		{"world:anyone:rw", []zk.ACL{{Scheme: "world", ID: "anyone", Perms: 3}}},
		{"world:anyone:3", []zk.ACL{{Scheme: "world", ID: "anyone", Perms: 3}}},
		{"host:example.com:cdrw", []zk.ACL{{Scheme: "host", ID: "example.com", Perms: 15}}},
		{"ip:10.2.1.15/32:cdrwa", []zk.ACL{{Scheme: "ip", ID: "10.2.1.15/32", Perms: 31}}},
		{"digest:username:pwhash:cd", []zk.ACL{{Scheme: "digest", ID: "username:pwhash", Perms: 12}}},
		{"auth::cdrwa", []zk.ACL{{Scheme: "auth", ID: "", Perms: 31}}},
	}

	for _, c := range cases {
		zook := NewZooKeeper()
		got, _ := zook.parseACLString(c.aclstr)
		if !aclsEqual(got, c.want) {
			t.Errorf("parseACLString(%q) == %q, want %q", c.aclstr, got, c.want)
		}
	}
}

func TestParseInvalidACLString(t *testing.T) {
	aclstr := "world:anyone:rwb"
	want := "invalid ACL string specified"

	zook := NewZooKeeper()
	_, err := zook.parseACLString(aclstr)

	if err == nil {
		t.Error("No error returned")
	} else {
		if err.Error() != want {
			t.Errorf("parseACLString(%q) error %q, want %q", aclstr, err.Error(), want)
		}
	}
}

func aclsEqual(a, b []zk.ACL) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
