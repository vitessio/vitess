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

// Package acl contains functions to enforce access control lists.
// It allows you to register multiple security policies for enforcing
// ACLs for users or HTTP requests. The specific policy to use must be
// specified from a command line argument and cannot be changed on-the-fly.
package acl

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/casbin/casbin"
	log "github.com/golang/glog"
)

// This is a list of predefined roles. Applications are free
// to invent more roles, as long as the acl policies they use can
// understand what they mean.
const (
	ADMIN      = "admin"
	DEBUGGING  = "debugging"
	MONITORING = "monitoring"
)

var (
	enforcer *casbin.Enforcer
)

// initEnforcer creates a Casbin enforcer.
func initEnforcer() {
	model := casbin.NewModel()
	model.AddDef("r", "r", "sub, obj, act")
	model.AddDef("p", "p", "sub, obj, act")
	model.AddDef("g", "g", "_, _")
	model.AddDef("e", "e", "some(where (p.eft == allow))")
	model.AddDef("m", "m", "g(r.sub, p.sub) && keyMatch(r.obj, p.obj) && (r.act == p.act || p.act == \"*\")")

	enforcer = casbin.NewEnforcer(model, false)
}

// initPolicy adds the default security policy.
func initPolicy() {
	enforcer.AddPermissionForUser("admin", "*", "*")
	enforcer.AddPermissionForUser("debugging", "*", "*")
	enforcer.AddPermissionForUser("monitoring", "*", "*")
}

// CheckAccessHTTP uses the security policy to
// verify if a http request has access to
// the resource.
func CheckAccessHTTP(req *http.Request, role string) error {
	if enforcer == nil {
		initEnforcer()
		initPolicy()
	}

	path := req.URL.Path
	method := req.Method

	res := enforcer.Enforce(role, path, method)
	log.Warningf("Request: %s, %s, %s ---> %t", role, path, method, res)
	if res {
		return nil
	}

	return errors.New("access denied")
}

// SendError is a convenience function that sends an ACL
// error as an HTTP response.
func SendError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	fmt.Fprintf(w, "Access denied: %v\n", err)
}

// GetUserName gets the user name from the request.
// Currently, only HTTP basic authentication is supported
func GetUserName(r *http.Request) string {
	username, _, _ := r.BasicAuth()
	return username
}
