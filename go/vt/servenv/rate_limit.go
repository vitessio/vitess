package servenv

import (
	"fmt"

	"github.com/platinummonkey/go-concurrency-limits/core"
	grpclimits "github.com/platinummonkey/go-concurrency-limits/grpc"
	"github.com/platinummonkey/go-concurrency-limits/limit"
	"github.com/platinummonkey/go-concurrency-limits/limiter"
	"github.com/platinummonkey/go-concurrency-limits/strategy"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func addRateLimiting(builder *serverInterceptorBuilder) {
	logger := NewLogLimitLogger("grpc_server")
	metricRegistry := &core.EmptyMetricRegistry{}
	strat := strategy.NewSimpleStrategy(100 /*this number will be overwritten by whatever the Vegas algo thinks is the estimated limit*/)
	vegasLimit := limit.NewDefaultVegasLimit("le limit", logger, metricRegistry)

	limiterObj, err := limiter.NewDefaultLimiter(vegasLimit, 1000000000, 100000000000, 0, 50, strat, logger, metricRegistry)
	if err != nil {
		log.Error(vterrors.Errorf(vtrpc.Code_INTERNAL, "failed to instantiate vegas limiter"))
		return
	}

	switch *GRPCRateLimitServer {
	case "on":
		withLimiter := grpclimits.WithLimiter(limiterObj)
		builder.AddUnary(grpclimits.UnaryServerInterceptor(withLimiter))
	case "log":
		builder.AddUnary(LoggingUnaryServerInterceptor(limiterObj))
	}
}

// NewLogLimitLogger creates a logger for the Limiter.
func NewLogLimitLogger(name string) limit.Logger {
	return &limitLogger{Name: name}
}

type limitLogger struct {
	Name string
}

func (ll *limitLogger) Debugf(msg string, params ...interface{}) {
	log.InfoDepth(1, ll.Name+" "+fmt.Sprintf(msg, params...))
}

func (ll *limitLogger) IsDebugEnabled() bool {
	return true
}

// LoggingUnaryServerInterceptor creates a UnaryServerInterceptor that only logs.
func LoggingUnaryServerInterceptor(limiter core.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		token, ok := limiter.Acquire(ctx)
		if !ok {
			if token != nil {
				log.Info("rate limit reached. would have pushed back now")
				token.OnDropped()
			}
		}
		resp, err := handler(ctx, req)
		if err != nil && ok {
			token.OnDropped()
		}
		if err == nil && ok {
			token.OnSuccess()
		}
		return resp, err
	}
}
