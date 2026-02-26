/*
Copyright 2026 The Vitess Authors.

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

package tabletmanager

import (
	"context"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
)

type seedDialer struct {
	target string
}

func (d seedDialer) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	conn, err := d.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Invoke(ctx, method, args, reply, opts...)
}

func (d seedDialer) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, err := d.dial(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := conn.NewStream(ctx, desc, method, opts...)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return stream, nil
}

func (d seedDialer) dial(ctx context.Context) (*grpc.ClientConn, error) {
	conn, err := grpcclient.DialContext(ctx, d.target, grpcclient.FailFast(false))
	if err != nil {
		log.Errorf("gossip dial failed to %s: %v", d.target, err)
		return nil, err
	}
	return conn, nil
}
