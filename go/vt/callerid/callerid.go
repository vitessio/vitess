/*
Copyright 2019 The Vitess Authors.

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

// Package callerid stores/retrieves CallerIDs (immediate CallerID
// and effective CallerID) to/from the Context
package callerid

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// The datatype for CallerID Context Keys
type callerIDKey int

var (
	// internal Context key for immediate CallerID
	immediateCallerIDKey callerIDKey
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

// NewEffectiveCallerID creates a new vtrpcpb.CallerID with principal, component and
// subComponent
func NewEffectiveCallerID(principal string, component string, subComponent string) *vtrpcpb.CallerID {
	return &vtrpcpb.CallerID{Principal: principal, Component: component, Subcomponent: subComponent}
}

// GetPrincipal returns the effective user identifier, which is usually filled in
// with whoever made the request to the appserver, if the request
// came from an automated job or another system component.
// If the request comes directly from the Internet, or if the Vitess client
// takes action on its own accord, it is okay for this method to
// return empty string.
func GetPrincipal(ef *vtrpcpb.CallerID) string {
	if ef == nil {
		return ""
	}
	return ef.Principal
}

// GetComponent returns the running process of the effective caller.
// It can for instance return hostname:port of the servlet initiating the
// database call, or the container engine ID used by the servlet.
func GetComponent(ef *vtrpcpb.CallerID) string {
	if ef == nil {
		return ""
	}
	return ef.Component
}

// GetSubcomponent returns a component inside the process of effective caller,
// which is responsible for generating this request. Suggested values are a
// servlet name or an API endpoint name.
func GetSubcomponent(ef *vtrpcpb.CallerID) string {
	if ef == nil {
		return ""
	}
	return ef.Subcomponent
}

// NewContext adds the provided EffectiveCallerID(vtrpcpb.CallerID) and ImmediateCallerID(querypb.VTGateCallerID)
// into the Context
func NewContext(ctx context.Context, ef *vtrpcpb.CallerID, im *querypb.VTGateCallerID) context.Context {
	ctx = context.WithValue(
		context.WithValue(ctx, effectiveCallerIDKey, ef),
		immediateCallerIDKey,
		im,
	)
	return ctx
}

// EffectiveCallerIDFromContext returns the EffectiveCallerID(vtrpcpb.CallerID)
// stored in the Context, if any
func EffectiveCallerIDFromContext(ctx context.Context) *vtrpcpb.CallerID {
	ef, ok := ctx.Value(effectiveCallerIDKey).(*vtrpcpb.CallerID)
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
