// Package topotests contains all the unit tests for the topo.Server code
// that is based on topo.Conn.
//
// These tests cannot be in topo.Server, as they depend on
// memorytopo, which depends on topo.Conn/Factory, which are inside
// topo.Server.
package topotests
