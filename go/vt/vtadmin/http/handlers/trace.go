package handlers

import (
	"net/http"

	"github.com/gorilla/mux"
	"vitess.io/vitess/go/trace"
)

// TraceHandler is a mux.MiddlewareFunc which creates a span with the route's
// name, as set by (mux.*Route).Name(), embeds it in the request context, and invokes
// the next middleware in the chain.
//
// It also annotates the span with the route_path_template, if it exists, and
// the route_uri. To add additional spans, extract the span in your VTFunHTTPHandler
// like:
//
//		func Handler(ctx context.Context, r Request, api *API) *JSONResponse {
//			span, _ := trace.FromContext(ctx)
//			span.Annotate("foo", "bar")
//
//			return NewJSONResponse(api.Something(ctx))
//		}
//
// An unnamed route will get a span named "vtfun:http:<unnamed route>".
func TraceHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := mux.CurrentRoute(r)

		name := route.GetName()
		if name == "" {
			next.ServeHTTP(w, r)
			return
		}

		span, ctx := trace.NewSpan(r.Context(), "vtadmin:http:"+name)
		defer span.Finish()

		span.Annotate("route_uri", r.RequestURI)

		if tmpl, err := route.GetPathTemplate(); err != nil {
			span.Annotate("route_path_template", tmpl)
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
