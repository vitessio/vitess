// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package CallerID stores/retrives CallerIDs (immediate CallerID
// and effective CallerID) to/from the Context
package callerid

import (
	"golang.org/x/net/context"
)

// The methods in ImmediateCallerID and EffectiveCallerID interfaces
// are made to be Protobuf friendly so that if RPC is using protobuf, then
// the corresponding protobuf messages can be used directly as the interfaces

type ImmediateCallerID interface {
	// GetUsername returns the immediate caller of VTGate
	GetUsername() string
}

type EffectiveCallerID interface {
	// GetPrincipal returns the effective user identifier, which is usually filled in
	// with whoever made the request to the appserver, if the request
	// came from an automated job or another system component.
	// If the request comes directly from the Internet, or if the Vitess client
	// takes action on its own accord, it is okay for this method to
	// return empty string.
	GetPrincipal() string

	// GetComponent returns the running process of the effective caller.
	// It can for instance return hostname:port of the servlet initiating the
	// database call, or the container engine ID used by the servlet.
	GetComponent() string

	// GetSubcomponent returns a component inisde the process of effective caller,
	// which is responsible for generating this request. Suggested values are a
	// servlet name or an API endpoint name.
	GetSubcomponent() string
}

var (
	// internal Context key for immediate CallerID
	immediateCallerIDKey = "vtImmdiateCallerID"
	// internal Context key for effective CallerID
	effectiveCallerIDKey = "vtEffectiveCallerID"
)

// NewContext adds the provided EffectiveCallerID and ImmediateCallerID into the Context
func NewContext(ctx context.Context, ef EffectiveCallerID, im ImmediateCallerID) context.Context {
	ctx = context.WithValue(
		context.WithValue(ctx, effectiveCallerIDKey, ef),
		immediateCallerIDKey,
		im)
	return ctx
}

// EffectiveCallerIDFromContext returns the EffectiveCallerID stored in the Context, if any
func EffectiveCallerIDFromContext(ctx context.Context) (EffectiveCallerID, bool) {
	ef, ok := ctx.Value(effectiveCallerIDKey).(EffectiveCallerID)
	if ef != nil {
		return ef, ok
	}
	return nil, false
}

// ImmediateCallerIDFromContext returns the ImmediateCallerID stored in the Context, if any
func ImmediateCallerIDFromContext(ctx context.Context) (ImmediateCallerID, bool) {
	im, ok := ctx.Value(immediateCallerIDKey).(ImmediateCallerID)
	if im != nil {
		return im, ok
	}
	return nil, false
}
