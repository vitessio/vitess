package servenv

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
)

var mux = http.NewServeMux()

func HTTPHandle(pattern string, handler http.Handler) {
	mux.Handle(pattern, handler)
}

func HTTPHandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	mux.HandleFunc(pattern, handler)
}

func HTTPServe(l net.Listener) error {
	err := http.Serve(l, mux)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func HTTPTestServer() *httptest.Server {
	return httptest.NewServer(mux)
}

func HTTPRegisterProfile() {
	HTTPHandleFunc("/debug/pprof/", pprof.Index)
	HTTPHandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	HTTPHandleFunc("/debug/pprof/profile", pprof.Profile)
	HTTPHandleFunc("/debug/pprof/symbol", pprof.Symbol)
	HTTPHandleFunc("/debug/pprof/trace", pprof.Trace)
}
