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
	"flag"
	"fmt"
	"net/http"
	"sync"

	"vitess.io/vitess/go/vt/log"
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
	securityPolicy = flag.String("security_policy", "", "security policy to enforce for URLs")
	policies       = make(map[string]Policy)
	once           sync.Once
	currentPolicy  Policy
)

// Policy defines the interface that needs to be satisfied by
// ACL policy implementors.
type Policy interface {
	// CheckAccessActor can be called to verify if an actor
	// has access to the role.
	CheckAccessActor(actor, role string) error
	// CheckAccessHTTP can be called to verify if an actor in
	// the http request has access to the role.
	CheckAccessHTTP(req *http.Request, role string) error
}

// RegisterPolicy registers a security policy. This function must be called
// before the first call to CheckAccess happens, preferably through an init.
// This will ensure that the requested policy can be found by other acl
// functions when needed.
func RegisterPolicy(name string, policy Policy) {
	if _, ok := policies[name]; ok {
		log.Fatalf("policy %s is already registered", name)
	}
	policies[name] = policy
}

func savePolicy() {
	if *securityPolicy == "" {
		return
	}
	currentPolicy = policies[*securityPolicy]
	if currentPolicy == nil {
		log.Warningf("policy %s not found, using fallback policy", *securityPolicy)
		currentPolicy = FallbackPolicy{}
	}
}

// CheckAccessActor uses the current security policy to
// verify if an actor has access to the role.
func CheckAccessActor(actor, role string) error {
	once.Do(savePolicy)
	if currentPolicy != nil {
		return currentPolicy.CheckAccessActor(actor, role)
	}
	return nil
}

// CheckAccessHTTP uses the current security policy to
// verify if an actor in an http request has access to
// the role.
func CheckAccessHTTP(req *http.Request, role string) error {
	once.Do(savePolicy)
	if currentPolicy != nil {
		return currentPolicy.CheckAccessHTTP(req, role)
	}
	return nil
}

// SendError is a convenience function that sends an ACL
// error as an HTTP response.
func SendError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	_, _ = fmt.Fprintf(w, "Access denied: %v\n", err)
}
