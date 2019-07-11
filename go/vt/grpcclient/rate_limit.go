package grpcclient

import (
	"github.com/platinummonkey/go-concurrency-limits/core"
	grpc_limits "github.com/platinummonkey/go-concurrency-limits/grpc"
	"github.com/platinummonkey/go-concurrency-limits/limit"
	"github.com/platinummonkey/go-concurrency-limits/limiter"
	"github.com/platinummonkey/go-concurrency-limits/strategy"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
)

func rateLimitingOptions(name string) []grpc.DialOption {
	logger := servenv.NewLogLimitLogger("client.go")
	metricRegistry := &core.EmptyMetricRegistry{}
	strat := strategy.NewSimpleStrategy(100 /*this number will be overwritten by whatever the Vegas algo thinks is the estimated limit*/)
	vegasLimit := limit.NewDefaultVegasLimit(name, logger, metricRegistry)
	limiterObj, err := limiter.NewDefaultLimiter(vegasLimit, 1000000000, 100000000000, 0, 50, strat, logger, metricRegistry)
	if err != nil {
		log.Error(vterrors.Errorf(vtrpc.Code_INTERNAL, "failed to load rate limiter plugin"))
		return []grpc.DialOption{}
	}
	switch *rateLimitGrpcClient {
	case "on":
		withLimiter := grpc_limits.WithLimiter(limiterObj)
		return []grpc.DialOption{grpc.WithUnaryInterceptor(grpc_limits.UnaryClientInterceptor(withLimiter))}
	case "log":
		return []grpc.DialOption{grpc.WithUnaryInterceptor(LoggingUnaryClientInterceptor(limiterObj))}
	default:
		return []grpc.DialOption{}
	}
}

// LoggingUnaryClientInterceptor logs instead of actually pushing back.
func LoggingUnaryClientInterceptor(limiter core.Limiter) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		token, ok := limiter.Acquire(ctx)
		if !ok {
			if token != nil {
				log.Info("rate limit reached. would have pushed back now")
				token.OnDropped()
			}
		}

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil && ok {
			token.OnSuccess()
		}
		if err != nil && token != nil {
			token.OnDropped()
		}

		return err
	}
}
