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
