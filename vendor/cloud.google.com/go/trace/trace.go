// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package trace is a Google Stackdriver Trace library.
//
// This package is still experimental and subject to change.
//
// See https://cloud.google.com/trace/api/#data_model for a discussion of traces
// and spans.
//
// To initialize a client that connects to the Stackdriver Trace server, use the
// NewClient function. Generally you will want to do this on program
// initialization.
//
//   import "cloud.google.com/go/trace"
//   ...
//   traceClient, err = trace.NewClient(ctx, projectID)
//
// Calling SpanFromRequest will create a new trace span for an incoming HTTP
// request.  If the request contains a trace context header, it is used to
// determine the trace ID.  Otherwise, a new trace ID is created.
//
//   func handler(w http.ResponseWriter, r *http.Request) {
//     span := traceClient.SpanFromRequest(r)
//     defer span.Finish()
//     ...
//   }
//
// SpanFromRequest returns nil if the *Client is nil, so you can disable
// tracing by not initializing your *Client variable.  All of the exported
// functions on *Span do nothing when the *Span is nil.
//
// Although a trace span object is created for every request, only a subset of
// traces are uploaded to the server, for efficiency.  By default, the requests
// that are traced are those with the tracing bit set in the options field of
// the trace context header.  Ideally, you should override this behaviour by
// calling SetSamplingPolicy.  NewLimitedSampler returns an implementation of
// SamplingPolicy which traces requests that have the tracing bit set, and also
// randomly traces a specified fraction of requests.  Additionally, it sets a
// limit on the number of requests traced per second.  The following example
// traces one in every thousand requests, up to a limit of 5 per second.
//
//   p, err := trace.NewLimitedSampler(0.001, 5)
//   traceClient.SetSamplingPolicy(p)
//
// You can create a new span as a child of an existing span with NewChild.
//
//   childSpan := span.NewChild(name)
//   ...
//   childSpan.Finish()
//
// When sending an HTTP request to another server, NewRemoteChild will create
// a span to represent the time the current program waits for the request to
// complete, and attach a header to the outgoing request so that the trace will
// be propagated to the destination server.
//
//   childSpan := span.NewRemoteChild(&httpRequest)
//   ...
//   childSpan.Finish()
//
// Spans can contain a map from keys to values that have useful information
// about the span.  The elements of this map are called labels.  Some labels,
// whose keys all begin with the string "trace.cloud.google.com/", are set
// automatically in the following ways:
// - SpanFromRequest sets some labels to data about the incoming request.
// - NewRemoteChild sets some labels to data about the outgoing request.
// - Finish sets a label to a stack trace, if the stack trace option is enabled
//   in the incoming trace header.
// - The WithResponse option sets some labels to data about a response.
// You can also set labels using SetLabel.  If a label is given a value
// automatically and by SetLabel, the automatically-set value is used.
//
//   span.SetLabel(key, value)
//
// The WithResponse option can be used when Finish is called.
//
//   childSpan := span.NewRemoteChild(outgoingReq)
//   resp, err := http.DefaultClient.Do(outgoingReq)
//   ...
//   childSpan.Finish(trace.WithResponse(resp))
//
// When a span created by SpanFromRequest is finished, the finished spans in the
// corresponding trace -- the span itself and its descendants -- are uploaded
// to the Stackdriver Trace server using the *Client that created the span.
// Finish returns immediately, and uploading occurs asynchronously.  You can use
// the FinishWait function instead to wait until uploading has finished.
//
//   err := span.FinishWait()
//
// Using contexts to pass *trace.Span objects through your program will often
// be a better approach than passing them around explicitly.  This allows trace
// spans, and other request-scoped or part-of-request-scoped values, to be
// easily passed through API boundaries.  Various Google Cloud libraries will
// retrieve trace spans from contexts and automatically create child spans for
// API requests.
// See https://blog.golang.org/context for more discussion of contexts.
// A derived context containing a trace span can be created using NewContext.
//
//   span := traceClient.SpanFromRequest(r)
//   ctx = trace.NewContext(ctx, span)
//
// The span can be retrieved from a context elsewhere in the program using
// FromContext.
//
//   func foo(ctx context.Context) {
//     newSpan := trace.FromContext(ctx).NewChild("in foo")
//     defer newSpan.Finish()
//     ...
//   }
//
package trace // import "cloud.google.com/go/trace"

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	api "google.golang.org/api/cloudtrace/v1"
	"google.golang.org/api/gensupport"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"
	"google.golang.org/api/transport"
	"google.golang.org/grpc"
)

const (
	httpHeader          = `X-Cloud-Trace-Context`
	userAgent           = `gcloud-golang-trace/20160501`
	cloudPlatformScope  = `https://www.googleapis.com/auth/cloud-platform`
	spanKindClient      = `RPC_CLIENT`
	spanKindServer      = `RPC_SERVER`
	spanKindUnspecified = `SPAN_KIND_UNSPECIFIED`
	maxStackFrames      = 20
	labelHost           = `trace.cloud.google.com/http/host`
	labelMethod         = `trace.cloud.google.com/http/method`
	labelStackTrace     = `trace.cloud.google.com/stacktrace`
	labelStatusCode     = `trace.cloud.google.com/http/status_code`
	labelURL            = `trace.cloud.google.com/http/url`
	labelSamplingPolicy = `trace.cloud.google.com/sampling_policy`
	labelSamplingWeight = `trace.cloud.google.com/sampling_weight`
)

type contextKey struct{}

type stackLabelValue struct {
	Frames []stackFrame `json:"stack_frame"`
}

type stackFrame struct {
	Class    string `json:"class_name,omitempty"`
	Method   string `json:"method_name"`
	Filename string `json:"file_name"`
	Line     int64  `json:"line_number"`
}

var (
	spanIDCounter   uint64
	spanIDIncrement uint64
)

func init() {
	// Set spanIDCounter and spanIDIncrement to random values.  nextSpanID will
	// return an arithmetic progression using these values, skipping zero.  We set
	// the LSB of spanIDIncrement to 1, so that the cycle length is 2^64.
	binary.Read(rand.Reader, binary.LittleEndian, &spanIDCounter)
	binary.Read(rand.Reader, binary.LittleEndian, &spanIDIncrement)
	spanIDIncrement |= 1
	// Attach hook for autogenerated Google API calls.  This will automatically
	// create trace spans for API calls if there is a trace in the context.
	gensupport.RegisterHook(requestHook)
}

func requestHook(ctx context.Context, req *http.Request) func(resp *http.Response) {
	span := FromContext(ctx)
	if span == nil || req == nil {
		return nil
	}
	span = span.NewRemoteChild(req)
	return func(resp *http.Response) {
		if resp != nil {
			span.Finish(WithResponse(resp))
		} else {
			span.Finish()
		}
	}
}

// EnableGRPCTracingDialOption enables tracing of requests that are sent over a
// gRPC connection.
// The functionality in gRPC that this relies on is currently experimental.
var EnableGRPCTracingDialOption grpc.DialOption = grpc.WithUnaryInterceptor(grpc.UnaryClientInterceptor(grpcUnaryInterceptor))

// EnableGRPCTracing enables tracing of requests for clients that use gRPC
// connections.
// The functionality in gRPC that this relies on is currently experimental.
var EnableGRPCTracing option.ClientOption = option.WithGRPCDialOption(EnableGRPCTracingDialOption)

func grpcUnaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// TODO: also intercept streams.
	span := FromContext(ctx).NewChild(method)
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		// TODO: standardize gRPC label names?
		span.SetLabel("error", err.Error())
	}
	span.Finish()
	return err
}

// nextSpanID returns a new span ID.  It will never return zero.
func nextSpanID() uint64 {
	var id uint64
	for id == 0 {
		id = atomic.AddUint64(&spanIDCounter, spanIDIncrement)
	}
	return id
}

// nextTraceID returns a new trace ID.
func nextTraceID() string {
	id1 := nextSpanID()
	id2 := nextSpanID()
	return fmt.Sprintf("%016x%016x", id1, id2)
}

// Client is a client for uploading traces to the Google Stackdriver Trace server.
type Client struct {
	service   *api.Service
	projectID string
	policy    SamplingPolicy
	bundler   *bundler.Bundler
}

// NewClient creates a new Google Stackdriver Trace client.
func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	o := []option.ClientOption{
		option.WithScopes(cloudPlatformScope),
		option.WithUserAgent(userAgent),
	}
	o = append(o, opts...)
	hc, basePath, err := transport.NewHTTPClient(ctx, o...)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client for Google Stackdriver Trace API: %v", err)
	}
	apiService, err := api.New(hc)
	if err != nil {
		return nil, fmt.Errorf("creating Google Stackdriver Trace API client: %v", err)
	}
	if basePath != "" {
		// An option set a basepath, so override api.New's default.
		apiService.BasePath = basePath
	}
	c := &Client{
		service:   apiService,
		projectID: projectID,
	}
	bundler := bundler.NewBundler((*api.Trace)(nil), func(bundle interface{}) {
		traces := bundle.([]*api.Trace)
		err := c.upload(traces)
		if err != nil {
			log.Printf("failed to upload %d traces to the Cloud Trace server.", len(traces))
		}
	})
	bundler.DelayThreshold = 2 * time.Second
	bundler.BundleCountThreshold = 100
	// We're not measuring bytes here, we're counting traces and spans as one "byte" each.
	bundler.BundleByteThreshold = 1000
	bundler.BundleByteLimit = 1000
	bundler.BufferedByteLimit = 10000
	c.bundler = bundler
	return c, nil
}

// SetSamplingPolicy sets the SamplingPolicy that determines how often traces
// are initiated by this client.
func (c *Client) SetSamplingPolicy(p SamplingPolicy) {
	if c != nil {
		c.policy = p
	}
}

// SpanFromRequest returns a new trace span.
//
// It returns nil iff the client is nil.
//
// If the incoming HTTP request contains a trace context header, the trace ID,
// parent span ID, and tracing options will be read from that header.
// Otherwise, a new trace ID is made and the parent span ID is zero.
//
// If a non-nil sampling policy has been set in the client, it can override the
// options set in the header and choose whether to trace the request.
//
// If the request is not being traced, then a *Span is returned anyway, but it
// will not be uploaded to the server -- it is only useful for propagating
// trace context to child requests and for getting the TraceID.  All its
// methods can still be called -- the Finish, FinishWait, and SetLabel methods
// do nothing.  NewChild does nothing, and returns the same *Span.  TraceID
// works as usual.
func (client *Client) SpanFromRequest(r *http.Request) *Span {
	if client == nil {
		return nil
	}
	traceID, parentSpanID, options, hasTraceHeader := traceInfoFromRequest(r)
	if !hasTraceHeader {
		traceID = nextTraceID()
	}
	t := &trace{
		traceID:       traceID,
		client:        client,
		globalOptions: options,
		localOptions:  options,
	}
	span := startNewChildWithRequest(r, t, parentSpanID)
	span.span.Kind = spanKindServer
	span.rootSpan = true
	if client.policy != nil {
		d := client.policy.Sample(Parameters{HasTraceHeader: hasTraceHeader})
		if d.Trace {
			// Turn on tracing locally, and in child requests.
			span.trace.localOptions |= optionTrace
			span.trace.globalOptions |= optionTrace
		} else {
			// Turn off tracing locally.
			span.trace.localOptions = 0
			return span
		}
		if d.Sample {
			// This trace is in the random sample, so set the labels.
			span.SetLabel(labelSamplingPolicy, d.Policy)
			span.SetLabel(labelSamplingWeight, fmt.Sprint(d.Weight))
		}
	}

	return span
}

// NewContext returns a derived context containing the span.
func NewContext(ctx context.Context, s *Span) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, s)
}

// FromContext returns the span contained in the context, or nil.
func FromContext(ctx context.Context) *Span {
	s, _ := ctx.Value(contextKey{}).(*Span)
	return s
}

func traceInfoFromRequest(r *http.Request) (string, uint64, optionFlags, bool) {
	// See https://cloud.google.com/trace/docs/faq for the header format.
	h := r.Header.Get(httpHeader)
	// Return if the header is empty or missing, or if the header is unreasonably
	// large, to avoid making unnecessary copies of a large string.
	if h == "" || len(h) > 200 {
		return "", 0, 0, false
	}

	// Parse the trace id field.
	slash := strings.Index(h, `/`)
	if slash == -1 {
		return "", 0, 0, false
	}
	traceID, h := h[:slash], h[slash+1:]

	// Parse the span id field.
	semicolon := strings.Index(h, `;`)
	if semicolon == -1 {
		return "", 0, 0, false
	}
	spanstr, h := h[:semicolon], h[semicolon+1:]
	spanID, err := strconv.ParseUint(spanstr, 10, 64)
	if err != nil {
		return "", 0, 0, false
	}

	// Parse the options field.
	if !strings.HasPrefix(h, "o=") {
		return "", 0, 0, false
	}
	o, err := strconv.ParseUint(h[2:], 10, 64)
	if err != nil {
		return "", 0, 0, false
	}
	options := optionFlags(o)
	return traceID, spanID, options, true
}

type optionFlags uint32

const (
	optionTrace optionFlags = 1 << iota
	optionStack
)

type trace struct {
	mu            sync.Mutex
	client        *Client
	traceID       string
	globalOptions optionFlags // options that will be passed to any child requests
	localOptions  optionFlags // options applied in this server
	spans         []*Span     // finished spans for this trace.
}

// finish appends s to t.spans.  If s is the root span, uploads the trace to the
// server.
func (t *trace) finish(s *Span, wait bool, opts ...FinishOption) error {
	for _, o := range opts {
		o.modifySpan(s)
	}
	s.end = time.Now()
	t.mu.Lock()
	t.spans = append(t.spans, s)
	spans := t.spans
	t.mu.Unlock()
	if s.rootSpan {
		if wait {
			return t.client.upload([]*api.Trace{t.constructTrace(spans)})
		}
		go func() {
			tr := t.constructTrace(spans)
			err := t.client.bundler.Add(tr, 1+len(spans))
			if err == bundler.ErrOversizedItem {
				err = t.client.upload([]*api.Trace{tr})
			}
			if err != nil {
				log.Println("error uploading trace:", err)
			}
		}()
	}
	return nil
}

func (t *trace) constructTrace(spans []*Span) *api.Trace {
	apiSpans := make([]*api.TraceSpan, len(spans))
	for i, sp := range spans {
		sp.span.StartTime = sp.start.In(time.UTC).Format(time.RFC3339Nano)
		sp.span.EndTime = sp.end.In(time.UTC).Format(time.RFC3339Nano)
		if t.localOptions&optionStack != 0 {
			sp.setStackLabel()
		}
		sp.SetLabel(labelHost, sp.host)
		sp.SetLabel(labelURL, sp.url)
		sp.SetLabel(labelMethod, sp.method)
		if sp.statusCode != 0 {
			sp.SetLabel(labelStatusCode, strconv.Itoa(sp.statusCode))
		}
		apiSpans[i] = &sp.span
	}

	return &api.Trace{
		ProjectId: t.client.projectID,
		TraceId:   t.traceID,
		Spans:     apiSpans,
	}
}

func (c *Client) upload(traces []*api.Trace) error {
	_, err := c.service.Projects.PatchTraces(c.projectID, &api.Traces{Traces: traces}).Do()
	return err
}

// Span contains information about one span of a trace.
type Span struct {
	trace      *trace
	span       api.TraceSpan
	start      time.Time
	end        time.Time
	rootSpan   bool
	stack      [maxStackFrames]uintptr
	host       string
	method     string
	url        string
	statusCode int
}

func (s *Span) tracing() bool {
	return s.trace.localOptions&optionTrace != 0
}

// NewChild creates a new span with the given name as a child of s.
// If s is nil, does nothing and returns nil.
func (s *Span) NewChild(name string) *Span {
	if s == nil {
		return nil
	}
	if !s.tracing() {
		return s
	}
	return startNewChild(name, s.trace, s.span.SpanId)
}

// NewRemoteChild creates a new span as a child of s.
//
// Some labels in the span are set from the outgoing *http.Request r.
//
// A header is set in r so that the trace context is propagated to the
// destination.  The parent span ID in that header is set as follows:
// - If the request is being traced, then the ID of s is used.
// - If the request is not being traced, but there was a trace context header
//   in the incoming request for this trace (the request passed to
//   SpanFromRequest), the parent span ID in that header is used.
// - Otherwise, the parent span ID is zero.
// The tracing bit in the options is set if tracing is enabled, or if it was
// set in the incoming request.
//
// If s is nil, does nothing and returns nil.
func (s *Span) NewRemoteChild(r *http.Request) *Span {
	if s == nil {
		return nil
	}
	if !s.tracing() {
		r.Header[httpHeader] = []string{spanHeader(s.trace.traceID, s.span.ParentSpanId, s.trace.globalOptions)}
		return s
	}
	newSpan := startNewChildWithRequest(r, s.trace, s.span.SpanId)
	r.Header[httpHeader] = []string{spanHeader(s.trace.traceID, newSpan.span.SpanId, s.trace.globalOptions)}
	return newSpan
}

func startNewChildWithRequest(r *http.Request, trace *trace, parentSpanId uint64) *Span {
	newSpan := startNewChild(r.URL.Path, trace, parentSpanId)
	if r.Host == "" {
		newSpan.host = r.URL.Host
	} else {
		newSpan.host = r.Host
	}
	newSpan.method = r.Method
	newSpan.url = r.URL.String()
	return newSpan
}

func startNewChild(name string, trace *trace, parentSpanId uint64) *Span {
	spanID := nextSpanID()
	for spanID == parentSpanId {
		spanID = nextSpanID()
	}
	newSpan := &Span{
		trace: trace,
		span: api.TraceSpan{
			Kind:         spanKindClient,
			Name:         name,
			ParentSpanId: parentSpanId,
			SpanId:       spanID,
		},
		start: time.Now(),
	}
	if trace.localOptions&optionStack != 0 {
		_ = runtime.Callers(1, newSpan.stack[:])
	}
	return newSpan
}

// TraceID returns the ID of the trace to which s belongs.
func (s *Span) TraceID() string {
	if s == nil {
		return ""
	}
	return s.trace.traceID
}

// SetLabel sets the label for the given key to the given value.
// If the value is empty, the label for that key is deleted.
// If a label is given a value automatically and by SetLabel, the
// automatically-set value is used.
// If s is nil, does nothing.
func (s *Span) SetLabel(key, value string) {
	if s == nil {
		return
	}
	if !s.tracing() {
		return
	}
	if value == "" {
		if s.span.Labels != nil {
			delete(s.span.Labels, key)
		}
		return
	}
	if s.span.Labels == nil {
		s.span.Labels = make(map[string]string)
	}
	s.span.Labels[key] = value
}

type FinishOption interface {
	modifySpan(s *Span)
}

type withResponse struct {
	*http.Response
}

// WithResponse returns an option that can be passed to Finish that indicates
// that some labels for the span should be set using the given *http.Response.
func WithResponse(resp *http.Response) FinishOption {
	return withResponse{resp}
}
func (u withResponse) modifySpan(s *Span) {
	if u.Response != nil {
		s.statusCode = u.StatusCode
	}
}

// Finish declares that the span has finished.
//
// If s is nil, Finish does nothing and returns nil.
//
// If the option trace.WithResponse(resp) is passed, then some labels are set
// for s using information in the given *http.Response.  This is useful when the
// span is for an outgoing http request; s will typically have been created by
// NewRemoteChild in this case.
//
// If s is a root span (one created by SpanFromRequest) then s, and all its
// descendant spans that have finished, are uploaded to the Google Stackdriver
// Trace server asynchronously.
func (s *Span) Finish(opts ...FinishOption) {
	if s == nil {
		return
	}
	if !s.tracing() {
		return
	}
	s.trace.finish(s, false, opts...)
}

// FinishWait is like Finish, but if s is a root span, it waits until uploading
// is finished, then returns an error if one occurred.
func (s *Span) FinishWait(opts ...FinishOption) error {
	if s == nil {
		return nil
	}
	if !s.tracing() {
		return nil
	}
	return s.trace.finish(s, true, opts...)
}

func spanHeader(traceID string, spanID uint64, options optionFlags) string {
	// See https://cloud.google.com/trace/docs/faq for the header format.
	return fmt.Sprintf("%s/%d;o=%d", traceID, spanID, options)
}

func (s *Span) setStackLabel() {
	var stack stackLabelValue
	lastSigPanic, inTraceLibrary := false, true
	for _, pc := range s.stack {
		if pc == 0 {
			break
		}
		if !lastSigPanic {
			pc--
		}
		fn := runtime.FuncForPC(pc)
		file, line := fn.FileLine(pc)
		// Name has one of the following forms:
		// path/to/package.Foo
		// path/to/package.(Type).Foo
		// For the first form, we store the whole name in the Method field of the
		// stack frame.  For the second form, we set the Method field to "Foo" and
		// the Class field to "path/to/package.(Type)".
		name := fn.Name()
		if inTraceLibrary && !strings.HasPrefix(name, "cloud.google.com/go/trace.") {
			inTraceLibrary = false
		}
		var class string
		if i := strings.Index(name, ")."); i != -1 {
			class, name = name[:i+1], name[i+2:]
		}
		frame := stackFrame{
			Class:    class,
			Method:   name,
			Filename: file,
			Line:     int64(line),
		}
		if inTraceLibrary && len(stack.Frames) == 1 {
			stack.Frames[0] = frame
		} else {
			stack.Frames = append(stack.Frames, frame)
		}
		lastSigPanic = fn.Name() == "runtime.sigpanic"
	}
	if label, err := json.Marshal(stack); err == nil {
		s.SetLabel(labelStackTrace, string(label))
	}
}
