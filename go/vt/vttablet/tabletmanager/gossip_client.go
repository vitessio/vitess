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
