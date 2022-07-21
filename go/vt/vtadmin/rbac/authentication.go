/*
Copyright 2021 The Vitess Authors.

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

package rbac

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"plugin"
	"sync"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Authenticator defines the interface vtadmin authentication plugins must
// implement. Authenticators are installed at the grpc interceptor and http
// middleware layers.
type Authenticator interface {
	// Authenticate returns an Actor given a context. This method is called
	// from the stream and unary grpc server interceptors, and are passed the
	// stream and request contexts, respectively.
	//
	// Returning an error from the authenticator will fail the request. To
	// denote an authenticated request, return (nil, nil) instead.
	Authenticate(ctx context.Context) (*Actor, error)
	// AuthenticateHTTP returns an actor given an http.Request.
	//
	// Returning an error from the authenticator will fail the request. To
	// denote an authenticated request, return (nil, nil) instead.
	AuthenticateHTTP(r *http.Request) (*Actor, error)
}

// AuthenticationStreamInterceptor returns a grpc.StreamServerInterceptor that
// uses the given Authenticator create an Actor from the stream's context, which
// is then stored in the stream context for later use.
//
// If the authenticator returns an error, the overall streaming rpc returns an
// UNAUTHENTICATED error.
func AuthenticationStreamInterceptor(authn Authenticator) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		actor, err := authn.Authenticate(ss.Context())
		if err != nil {
			return vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "%s", err)
		}

		wrappedStream := grpc_middleware.WrapServerStream(ss)
		wrappedStream.WrappedContext = NewContext(ss.Context(), actor)

		return handler(srv, wrappedStream)
	}
}

// AuthenticationUnaryInterceptor returns a grpc.UnaryServerInterceptor that
// uses the given Authenticator create an Actor from the request context, which
// is then stored in the request context for later use.
//
// If the authenticator returns an error, the overall unary rpc returns an
// UNAUTHENTICATED error.
func AuthenticationUnaryInterceptor(authn Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		actor, err := authn.Authenticate(ctx)
		if err != nil {
			return nil, vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "%s", err)
		}

		ctx = NewContext(ctx, actor)
		return handler(ctx, req)
	}
}

// Actor represents the subject in the "subject action resource" of an
// authorization check. It has a name and many roles.
type Actor struct {
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

type actorkey struct{}

// NewContext returns a context with the given actor stored in it. This is used
// to pass actor information from the authentication middleware and interceptors
// to the actual vtadmin api methods.
func NewContext(ctx context.Context, actor *Actor) context.Context {
	return context.WithValue(ctx, actorkey{}, actor)
}

// FromContext extracts an actor from the context, if one exists.
func FromContext(ctx context.Context) (*Actor, bool) {
	actor, ok := ctx.Value(actorkey{}).(*Actor)
	if !ok {
		return nil, false
	}

	return actor, true
}

var (
	// ErrUnregisteredAuthenticationImpl is returned when an RBAC config
	// specifies an authenticator name that was not registered.
	ErrUnregisteredAuthenticationImpl = errors.New("unregistered Authenticator implementation")
	authenticators                    = map[string]func() Authenticator{}
	authenticatorsM                   sync.Mutex
)

// RegisterAuthenticator registers an authenticator implementation by name. It
// is not safe for concurrent use.
//
// Plugin-based authenticators are loaded separately, and need not call this
// function.
func RegisterAuthenticator(name string, f func() Authenticator) {
	if _, ok := authenticators[name]; ok {
		panic(fmt.Sprintf("authenticator already registered with name: %s", name))
	}

	authenticators[name] = f
}

func loadAuthenticatorPlugin(path string) (Authenticator, error) {
	authenticatorsM.Lock()
	defer authenticatorsM.Unlock()

	if f, ok := authenticators[path]; ok {
		return f(), nil
	}

	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	sym, err := p.Lookup("NewAuthenticator")
	if err != nil {
		return nil, err
	}

	f, ok := sym.(func() Authenticator)
	if !ok {
		return nil, fmt.Errorf("symbol NewAuthenticator must be of type `func() Authenticator`; have %T", sym)
	}

	authenticators[path] = f
	return f(), nil
}
