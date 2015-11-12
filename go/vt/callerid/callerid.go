// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package callerid stores/retrives CallerIDs (immediate CallerID
// and effective CallerID) to/from the Context
package callerid

import (
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// The datatype for CallerID Context Keys
type callerIDKey int

var (
	// internal Context key for immediate CallerID
	immediateCallerIDKey callerIDKey = 0
	// internal Context key for effective CallerID
	effectiveCallerIDKey callerIDKey = 1
)

// NewImmediateCallerID creates a querypb.VTGateCallerID initialized with username
func NewImmediateCallerID(username string) *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: username}
}

// GetUsername returns the immediate caller of VTGate
func GetUsername(im *querypb.VTGateCallerID) string {
	if im == nil {
		return ""
	}
	return im.Username
}

// NewEffectiveCallerID creates a new vtpb.CallerID with principal, component and
// subComponent
func NewEffectiveCallerID(principal string, component string, subComponent string) *vtpb.CallerID {
	return &vtpb.CallerID{Principal: principal, Component: component, Subcomponent: subComponent}
}

// GetPrincipal returns the effective user identifier, which is usually filled in
// with whoever made the request to the appserver, if the request
// came from an automated job or another system component.
// If the request comes directly from the Internet, or if the Vitess client
// takes action on its own accord, it is okay for this method to
// return empty string.
func GetPrincipal(ef *vtpb.CallerID) string {
	if ef == nil {
		return ""
	}
	return ef.Principal
}

// GetComponent returns the running process of the effective caller.
// It can for instance return hostname:port of the servlet initiating the
// database call, or the container engine ID used by the servlet.
func GetComponent(ef *vtpb.CallerID) string {
	if ef == nil {
		return ""
	}
	return ef.Component
}

// GetSubcomponent returns a component inisde the process of effective caller,
// which is responsible for generating this request. Suggested values are a
// servlet name or an API endpoint name.
func GetSubcomponent(ef *vtpb.CallerID) string {
	if ef == nil {
		return ""
	}
	return ef.Subcomponent
}

// NewContext adds the provided EffectiveCallerID(vtpb.CallerID) and ImmediateCallerID(querypb.VTGateCallerID)
// into the Context
func NewContext(ctx context.Context, ef *vtpb.CallerID, im *querypb.VTGateCallerID) context.Context {
	ctx = context.WithValue(
		context.WithValue(ctx, effectiveCallerIDKey, ef),
		immediateCallerIDKey,
		im,
	)
	return ctx
}

// EffectiveCallerIDFromContext returns the EffectiveCallerID(vtpb.CallerID)
// stored in the Context, if any
func EffectiveCallerIDFromContext(ctx context.Context) *vtpb.CallerID {
	ef, ok := ctx.Value(effectiveCallerIDKey).(*vtpb.CallerID)
	if ok && ef != nil {
		return ef
	}
	return nil
}

// ImmediateCallerIDFromContext returns the ImmediateCallerID(querypb.VTGateCallerID)
// stored in the Context, if any
func ImmediateCallerIDFromContext(ctx context.Context) *querypb.VTGateCallerID {
	im, ok := ctx.Value(immediateCallerIDKey).(*querypb.VTGateCallerID)
	if ok && im != nil {
		return im
	}
	return nil
}
