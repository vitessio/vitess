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

package trace

import (
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Span = (*openTracingSpan)(nil)

type openTracingSpan struct {
	otSpan opentracing.Span
}

// Finish will mark a span as finished
func (js openTracingSpan) Finish() {
	js.otSpan.Finish()
}

// Annotate will add information to an existing span
func (js openTracingSpan) Annotate(key string, value interface{}) {
	js.otSpan.SetTag(key, value)
}

var _ tracingService = (*openTracingService)(nil)

type tracer interface {
	GetOpenTracingTracer() opentracing.Tracer
}

type openTracingService struct {
	Tracer tracer
}

// AddGrpcServerOptions is part of an interface implementation
func (jf openTracingService) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	ot := jf.Tracer.GetOpenTracingTracer()
	addInterceptors(otgrpc.OpenTracingStreamServerInterceptor(ot), otgrpc.OpenTracingServerInterceptor(ot))
}

// AddGrpcClientOptions is part of an interface implementation
func (jf openTracingService) AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor)) {
	ot := jf.Tracer.GetOpenTracingTracer()
	addInterceptors(otgrpc.OpenTracingStreamClientInterceptor(ot), otgrpc.OpenTracingClientInterceptor(ot))
}

// NewClientSpan is part of an interface implementation
func (jf openTracingService) NewClientSpan(parent Span, serviceName, label string) Span {
	span := jf.New(parent, label)
	span.Annotate("peer.service", serviceName)
	return span
}

// New is part of an interface implementation
func (jf openTracingService) New(parent Span, label string) Span {
	var innerSpan opentracing.Span
	if parent == nil {
		innerSpan = jf.Tracer.GetOpenTracingTracer().StartSpan(label)
	} else {
		jaegerParent := parent.(openTracingSpan)
		span := jaegerParent.otSpan
		innerSpan = jf.Tracer.GetOpenTracingTracer().StartSpan(label, opentracing.ChildOf(span.Context()))
	}
	return openTracingSpan{otSpan: innerSpan}
}

func extractMapFromString(in string) (opentracing.TextMapCarrier, error) {
	result := map[string]string{}
	if in == "" {
		return result, nil
	}

	// the following code is a little state machine that uses these
	// three variables to hold it's state as it scans the input string
	readingKey := true
	var currentKey string
	var currentValue string

	addCurrentKV := func() error {
		if readingKey {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "every element in the context string has to be in the form key=value")
		}
		result[currentKey] = currentValue
		currentKey = ""
		currentValue = ""
		readingKey = true
		return nil
	}
	addChar := func(char uint8) {
		if readingKey {
			currentKey += string(char)
		} else {
			currentValue += string(char)
		}
	}
	size := len(in)
	for i := 0; i < size; i++ {
		atEnd := i == size-1
		char := in[i]
		switch char {
		case '=':
			readingKey = false
		case ':':
			err := addCurrentKV()
			if err != nil {
				return nil, err
			}

		case '\\':
			if atEnd { // can't end with a trailing escape char
				return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "malformed escaping - cannot end with an escape character ")
			}
			nextChar := in[i+1]
			i++
			switch nextChar {
			case '\\':
				addChar('\\')
			case ':':
				addChar(':')
			case '=':
				addChar('=')
			default:
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "malformed escaping - [\\%s] is invalid", string(nextChar))
			}
		default:
			addChar(char)
		}
	}
	err := addCurrentKV()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (jf openTracingService) NewFromString(parent, label string) (Span, error) {
	carrier, err := extractMapFromString(parent)
	if err != nil {
		return nil, err
	}
	spanContext, err := jf.Tracer.GetOpenTracingTracer().Extract(opentracing.TextMap, carrier)
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to deserialize span context")
	}
	innerSpan := jf.Tracer.GetOpenTracingTracer().StartSpan(label, opentracing.ChildOf(spanContext))
	return openTracingSpan{otSpan: innerSpan}, nil
}

// FromContext is part of an interface implementation
func (jf openTracingService) FromContext(ctx context.Context) (Span, bool) {
	innerSpan := opentracing.SpanFromContext(ctx)

	if innerSpan == nil {
		return nil, false
	}
	return openTracingSpan{otSpan: innerSpan}, true
}

// NewContext is part of an interface implementation
func (jf openTracingService) NewContext(parent context.Context, s Span) context.Context {
	span, ok := s.(openTracingSpan)
	if !ok {
		return nil
	}
	return opentracing.ContextWithSpan(parent, span.otSpan)
}
