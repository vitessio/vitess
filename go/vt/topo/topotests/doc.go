/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package topotests contains all the unit tests for the topo.Server code
// that is based on topo.Backend.
//
// These tests cannot be in topo.Server yet, as they depend on
// memorytopo, which depends on topo.Backend, which is inside
// topo.Server.
//
// Once the conversion to topo.Backend is complete, we will move topo.Backend
// into its own interface package, break all the conflicting dependencies,
// and move these unit tests back into go/vt/topo.
package topotests
