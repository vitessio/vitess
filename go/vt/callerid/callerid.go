// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package callerid stores/retrives CallerIDs (immediate CallerID
// and effective CallerID) to/from the Context
package callerid

import (
	"golang.org/x/net/context"
)

// ImmediateCallerID is the username of immediate caller to VTGate
type ImmediateCallerID struct {
	username string
}

// GetUsername returns the immediate caller of VTGate
func (im *ImmediateCallerID) GetUsername() string {
	if im == nil {
		return ""
	}
	return im.username
}

// NewImmediateCallerID creates a ImmediateCallerID initialized with username
func NewImmediateCallerID(username string) *ImmediateCallerID {
	return &ImmediateCallerID{username: username}
}

// EffectiveCallerID is the identity of the actual caller to Vitess, for example,
// if the actual caller sends requests to Vitess through a proxy, then the EffectiveCallerID
// will contain information about the actual caller while the ImmediatieCallerID has
// the username running the proxy
type EffectiveCallerID struct {
	principal    string
	component    string
	subComponent string
}

// GetPrincipal returns the effective user identifier, which is usually filled in
// with whoever made the request to the appserver, if the request
// came from an automated job or another system component.
// If the request comes directly from the Internet, or if the Vitess client
// takes action on its own accord, it is okay for this method to
// return empty string.
func (ef *EffectiveCallerID) GetPrincipal() string {
	if ef == nil {
		return ""
	}
	return ef.principal
}

// GetComponent returns the running process of the effective caller.
// It can for instance return hostname:port of the servlet initiating the
// database call, or the container engine ID used by the servlet.
func (ef *EffectiveCallerID) GetComponent() string {
	if ef == nil {
		return ""
	}
	return ef.component
}

// GetSubcomponent returns a component inisde the process of effective caller,
// which is responsible for generating this request. Suggested values are a
// servlet name or an API endpoint name.
func (ef *EffectiveCallerID) GetSubcomponent() string {
	if ef == nil {
		return ""
	}
	return ef.subComponent
}

// NewEffectiveCallerID creates a new effective CallerID with principal, component and
// subComponent
func NewEffectiveCallerID(principal string, component string, subComponent string) *EffectiveCallerID {
	return &EffectiveCallerID{principal: principal, component: component, subComponent: subComponent}
}

var (
	// internal Context key for immediate CallerID
	immediateCallerIDKey = "vtImmdiateCallerID"
	// internal Context key for effective CallerID
	effectiveCallerIDKey = "vtEffectiveCallerID"
)

// NewContext adds the provided EffectiveCallerID and ImmediateCallerID into the Context
func NewContext(ctx context.Context, ef *EffectiveCallerID, im *ImmediateCallerID) context.Context {
	ctx = context.WithValue(
		context.WithValue(ctx, effectiveCallerIDKey, ef),
		immediateCallerIDKey,
		im)
	return ctx
}

// EffectiveCallerIDFromContext returns the EffectiveCallerID stored in the Context, if any
func EffectiveCallerIDFromContext(ctx context.Context) (*EffectiveCallerID, bool) {
	ef, ok := ctx.Value(effectiveCallerIDKey).(*EffectiveCallerID)
	if ef != nil {
		return ef, ok
	}
	return nil, false
}

// ImmediateCallerIDFromContext returns the ImmediateCallerID stored in the Context, if any
func ImmediateCallerIDFromContext(ctx context.Context) (*ImmediateCallerID, bool) {
	im, ok := ctx.Value(immediateCallerIDKey).(*ImmediateCallerID)
	if im != nil {
		return im, ok
	}
	return nil, false
}
