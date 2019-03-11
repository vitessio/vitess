package trace

import (
  "io"

  "github.com/opentracing/opentracing-go"
  "github.com/uber/jaeger-client-go/config"
  "golang.org/x/net/context"
)

type JaegerSpan struct {
  otSpan opentracing.Span
}

func (js JaegerSpan) StartLocal(label string) {
  panic("implement me")
}

func (js JaegerSpan) StartClient(label string) {
  panic("implement me")
}

func (js JaegerSpan) StartServer(label string) {
  panic("implement me")
}

func (js JaegerSpan) Finish() {
  js.otSpan.Finish()
}

func (js JaegerSpan) Annotate(key string, value interface{}) {
  js.otSpan.SetTag(key, value)
}

type OpenTracingFactory struct {
  Tracer opentracing.Tracer
}

// NewJagerTracerFromEnv will instantiate a SpanFactory implemented by Jaeger,
// taking configuration from environment variables, as described:
func NewJagerTracerFromEnv(serviceName string) (opentracing.Tracer, io.Closer, error) {

  cfg, err := config.FromEnv()
  if cfg.ServiceName == "" {
    cfg.ServiceName = serviceName
  }

  tracer, closer, err := cfg.NewTracer()

  if err != nil {
    return nil, &nilCloser{}, err
  }

  return tracer, closer, nil
}

func (jf OpenTracingFactory) New(parent Span, label string, spanType SpanType) Span {
  var innerSpan opentracing.Span
  if parent == nil {
    innerSpan = jf.Tracer.StartSpan(label)
  } else {
    jaegerParent := parent.(JaegerSpan)
    span := jaegerParent.otSpan
    innerSpan = jf.Tracer.StartSpan(label, opentracing.ChildOf(span.Context()))
  }
  return JaegerSpan{otSpan: innerSpan}
}

func (jf OpenTracingFactory) FromContext(ctx context.Context) (Span, bool) {
  innerSpan := opentracing.SpanFromContext(ctx)

  if innerSpan != nil {
    return JaegerSpan{otSpan: innerSpan}, true
  } else {
    return nil, false
  }
}

func (jf OpenTracingFactory) NewContext(parent context.Context, s Span) context.Context {
  span, ok := s.(JaegerSpan)
  if !ok {
    return nil
  }
  return opentracing.ContextWithSpan(parent, span.otSpan)
}

type nilCloser struct {
}

func (c *nilCloser) Close() error { return nil }