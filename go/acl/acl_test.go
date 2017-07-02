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

package acl

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

type TestHandler struct {
}

func (th *TestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	role := GetUserName(r)
	if err := CheckAccessHTTP(r, role); err != nil {
		SendError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Access OK.\n")
}

func testRequest(t *testing.T, th *TestHandler, role string, path string, method string, code int) {
	r, _ := http.NewRequest(method, path, nil)
	r.SetBasicAuth(role, "123")
	w := httptest.NewRecorder()
	th.ServeHTTP(w, r)

	if w.Code != code {
		t.Errorf("%s, %s, %s: %d, supposed to be %d", role, path, method, w.Code, code)
	}
}

func initExamplePolicy() {
	enforcer.EnableLog(true)

	enforcer.AddPermissionForUser("alice", "/dataset1/*", "GET")
	enforcer.AddPermissionForUser("alice", "/dataset1/resource1", "POST")

	enforcer.AddPermissionForUser("bob", "/dataset2/resource1", "*")
	enforcer.AddPermissionForUser("bob", "/dataset2/resource2", "GET")
	enforcer.AddPermissionForUser("bob", "/dataset2/folder1/*", "POST")

	enforcer.AddPermissionForUser("dataset1_admin", "/dataset1/*", "*")

	enforcer.AddRoleForUser("cathy", "dataset1_admin")
}

func TestBasic(t *testing.T) {
	th := &TestHandler{}

	// Here we use HTTP basic authentication as the way to get the logged-in user name
	// For simplicity, the credential is not verified, you should implement and use your own
	// authentication before the authorization.
	// In this example, we assume "alice:123" is a legal user.
	initEnforcer()
	initExamplePolicy()

	testRequest(t, th, "alice", "/dataset1/resource1", "GET", 200)
	testRequest(t, th, "alice", "/dataset1/resource1", "POST", 200)
	testRequest(t, th, "alice", "/dataset1/resource2", "GET", 200)
	testRequest(t, th, "alice", "/dataset1/resource2", "POST", 403)
}

func TestPathWildcard(t *testing.T) {
	th := &TestHandler{}

	// Here we use HTTP basic authentication as the way to get the logged-in user name
	// For simplicity, the credential is not verified, you should implement and use your own
	// authentication before the authorization.
	// In this example, we assume "bob:123" is a legal user.
	initEnforcer()
	initExamplePolicy()

	testRequest(t, th, "bob", "/dataset2/resource1", "GET", 200)
	testRequest(t, th, "bob", "/dataset2/resource1", "POST", 200)
	testRequest(t, th, "bob", "/dataset2/resource1", "DELETE", 200)
	testRequest(t, th, "bob", "/dataset2/resource2", "GET", 200)
	testRequest(t, th, "bob", "/dataset2/resource2", "POST", 403)
	testRequest(t, th, "bob", "/dataset2/resource2", "DELETE", 403)

	testRequest(t, th, "bob", "/dataset2/folder1/item1", "GET", 403)
	testRequest(t, th, "bob", "/dataset2/folder1/item1", "POST", 200)
	testRequest(t, th, "bob", "/dataset2/folder1/item1", "DELETE", 403)
	testRequest(t, th, "bob", "/dataset2/folder1/item2", "GET", 403)
	testRequest(t, th, "bob", "/dataset2/folder1/item2", "POST", 200)
	testRequest(t, th, "bob", "/dataset2/folder1/item2", "DELETE", 403)
}

func TestRBAC(t *testing.T) {
	th := &TestHandler{}

	// Here we use HTTP basic authentication as the way to get the logged-in user name
	// For simplicity, the credential is not verified, you should implement and use your own
	// authentication before the authorization.
	// In this example, we assume "cathy:123" is a legal user.
	initEnforcer()
	initExamplePolicy()

	// cathy can access all /dataset1/* resources via all methods because it has the dataset1_admin role.
	testRequest(t, th, "cathy", "/dataset1/item", "GET", 200)
	testRequest(t, th, "cathy", "/dataset1/item", "POST", 200)
	testRequest(t, th, "cathy", "/dataset1/item", "DELETE", 200)
	testRequest(t, th, "cathy", "/dataset2/item", "GET", 403)
	testRequest(t, th, "cathy", "/dataset2/item", "POST", 403)
	testRequest(t, th, "cathy", "/dataset2/item", "DELETE", 403)

	// delete all roles on user cathy, so cathy cannot access any resources now.
	enforcer.DeleteRolesForUser("cathy")

	testRequest(t, th, "cathy", "/dataset1/item", "GET", 403)
	testRequest(t, th, "cathy", "/dataset1/item", "POST", 403)
	testRequest(t, th, "cathy", "/dataset1/item", "DELETE", 403)
	testRequest(t, th, "cathy", "/dataset2/item", "GET", 403)
	testRequest(t, th, "cathy", "/dataset2/item", "POST", 403)
	testRequest(t, th, "cathy", "/dataset2/item", "DELETE", 403)
}
