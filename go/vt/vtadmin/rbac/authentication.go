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
	"net/http"
	"plugin"
	"sync"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type Authenticator interface {
	Authenticate(ctx context.Context) (*Actor, error)
	AuthenticateHTTP(r *http.Request) (*Actor, error)
}

func AuthenticationStreamInterceptor(authn Authenticator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		actor, err := authn.Authenticate(ss.Context())
		if err != nil {
			return vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "%s", err)
		}

		wrappedStream := grpc_middleware.WrapServerStream(ss)
		wrappedStream.WrappedContext = NewContext(ss.Context(), actor)

		return handler(srv, wrappedStream)
	}
}

func AuthenticationUnaryInterceptor(authn Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		actor, err := authn.Authenticate(ctx)
		if err != nil {
			return nil, vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "%s", err)
		}

		ctx = NewContext(ctx, actor)
		return handler(ctx, req)
	}
}

type Actor struct {
	Name  string
	Roles []string
}

type actorkey struct{}

func NewContext(ctx context.Context, actor *Actor) context.Context {
	return context.WithValue(ctx, actorkey{}, actor)
}

func FromContext(ctx context.Context) (*Actor, bool) {
	actor, ok := ctx.Value(actorkey{}).(*Actor)
	if !ok {
		return nil, false
	}

	return actor, true
}

var (
	ErrUnregisteredAuthenticationImpl = errors.New("unregistered Authenticator implementation")
	authenticators                    = map[string]func() Authenticator{}
	authenticatorsM                   sync.Mutex
)

func RegisterAuthenticator(name string, f func() Authenticator) {
	if _, ok := authenticators[name]; ok {
		panic("TODO")
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
		return nil, nil // TODO: error
	}

	authenticators[path] = f
	return f(), nil
}
