package dynamic

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cluster"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// StreamServerInterceptor returns a StreamServerInterceptor that redirects a
// streaming RPC to a dynamic API if the incoming context has a cluster spec in
// its metadata. Otherwise, the interceptor is a no-op, and the original stream
// handler is invoked.
func StreamServerInterceptor(api API) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		c, id, ok, err := clusterFromIncomingContextMetadata(ss.Context())
		switch { // TODO: see if it's possible to de-duplicate this somehow. Unfortunately the callbacks have different signatures which might make it impossible.
		case !ok:
			// No dynamic cluster metadata, proceed directly to handler.
			return handler(srv, ss)
		case id == "":
			// There was a cluster spec in the metadata, but we couldn't even
			// get an id out of it. Warn and fallback to static API.
			log.Warningf("failed to unmarshal dynamic cluster spec from incoming context metadata; falling back to static API; error: %v", err)
			return handler(srv, ss)
		}

		if err != nil {
			log.Warningf("failed to extract valid cluster from incoming metadata; attempting to use existing cluster with id=%s; error: %v", id, err)
		}

		dynamicAPI := api.WithCluster(c, id)
		streamHandler := streamHandlersByName[nameFromFullMethod(info.FullMethod)]
		return streamHandler(dynamicAPI, ss)
	}
}

// UnaryServerInterceptor returns a UnaryServerInterceptor that redirects a
// unary RPC to a dynamic API if the incoming context has a cluster spec in its
// metadata. Otherwise, the interceptor is a no-op, and the original method
// handler is invoked.
func UnaryServerInterceptor(api API) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		c, id, ok, err := clusterFromIncomingContextMetadata(ctx)
		switch {
		case !ok:
			// No dynamic cluster metadata, proceed directly to handler.
			return handler(ctx, req)
		case id == "":
			// There was a cluster spec in the metadata, but we couldn't even
			// get an id out of it. Warn and fallback to static API.
			log.Warningf("failed to unmarshal dynamic cluster spec from incoming context metadata; falling back to static API; error: %v", err)
			return handler(ctx, req)
		}

		if err != nil {
			log.Warningf("failed to extract valid cluster from incoming metadata; attempting to use existing cluster with id=%s; error: %v", id, err)
		}

		dynamicAPI := api.WithCluster(c, id)
		method := methodHandlersByName[nameFromFullMethod(info.FullMethod)]

		// NOTE: because we don't have access to the interceptor
		// chain (but we _could_ if we wanted to add a method to the
		// DynamicAPI interface), this MUST be the last interceptor
		// in the chain.
		return method(dynamicAPI, ctx, dec(req), nil)
	}
}

func clusterFromIncomingContextMetadata(ctx context.Context) (*cluster.Cluster, string, bool, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, "", false, nil
	}

	clusterMetadata := md.Get("cluster")
	if len(clusterMetadata) != 1 {
		return nil, "", false, nil
	}

	c, id, err := ClusterFromString(ctx, clusterMetadata[0])
	return c, id, true, err
}

// dec returns a function that merges the src proto.Message into dst.
func dec(src interface{}) func(dst interface{}) error {
	return func(dst interface{}) error {
		// gRPC handlers expect a function called `dec` which
		// decodes an arbitrary req into a req of the correct type
		// for the particular handler.
		//
		// Because we are doing a lookup that matches on method
		// name, we know that the `req` passed to the interceptor
		// and the `req2` passed to `dec` are the same type, we're
		// just going to proto.Merge blindly and return nil.
		proto.Merge(dst.(proto.Message), src.(proto.Message))
		return nil
	}
}

func nameFromFullMethod(fullMethod string) string {
	parts := strings.Split(fullMethod, "/")
	return parts[len(parts)-1]
}

var (
	methodHandlersByName = map[string]methodHandler{}
	streamHandlersByName = map[string]grpc.StreamHandler{}
)

// for whatever reason, grpc exports the StreamHandler type but _not_ the
// methodHandler type, but this is an identical type. Furthermore, the cast in
// the init() below will fail to compile if our types ever stop aligning.
//
// c.f. https://github.com/grpc/grpc-go/blob/v1.39.0/server.go#L81
type methodHandler func(srv interface{}, ctx context.Context, dec func(in interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error)

func init() {
	for _, m := range vtadminpb.VTAdmin_ServiceDesc.Methods {
		methodHandlersByName[m.MethodName] = methodHandler(m.Handler)
	}

	for _, s := range vtadminpb.VTAdmin_ServiceDesc.Streams {
		streamHandlersByName[s.StreamName] = s.Handler
	}
}
