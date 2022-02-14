package vtctld

import (
	"flag"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWebApp(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, appPrefix, nil)
	w := httptest.NewRecorder()

	webAppHandler(w, req)
	res := w.Result()

	assert.Equal(t, http.StatusOK, res.StatusCode)

	defer res.Body.Close()

	data, err := ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(data), "<!doctype html>")
}

func TestWebAppDisabled(t *testing.T) {
	flag.Set("enable_vtctld_ui", "false")
	defer flag.Set("enable_vtctld_ui", "true")

	req := httptest.NewRequest(http.MethodGet, appPrefix, nil)
	w := httptest.NewRecorder()

	webAppHandler(w, req)
	res := w.Result()

	assert.Equal(t, http.StatusNotFound, res.StatusCode)

	defer res.Body.Close()

	data, err := ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, "404 page not found\n", string(data))
}
