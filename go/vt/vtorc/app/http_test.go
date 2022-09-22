package app

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/config"
)

func captureOutput(f func()) string {
	reader, writer, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	stdout := os.Stdout
	stderr := os.Stderr
	defer func() {
		os.Stdout = stdout
		os.Stderr = stderr
		log.SetOutput(os.Stderr)
	}()
	os.Stdout = writer
	os.Stderr = writer
	log.SetOutput(writer)
	out := make(chan string)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		var buf bytes.Buffer
		wg.Done()
		_, _ = io.Copy(&buf, reader)
		out <- buf.String()
	}()
	wg.Wait()
	f()
	_ = writer.Close()
	return <-out
}

func TestStandardHTTPLogging(t *testing.T) {
	defaultListenAddress := config.Config.ListenAddress
	listenAddress := ":17000"
	config.Config.ListenAddress = listenAddress
	defer func() {
		config.Config.ListenAddress = defaultListenAddress
	}()

	logOutput := captureOutput(func() {
		go standardHTTP(false)
		time.Sleep(10 * time.Second)
		// Make a API request to check if something logged
		makeAPICall(t, "http://localhost:17000/api/health")
	})
	require.NotContains(t, logOutput, "martini")
}

// makeAPICall is used make an API call given the url. It returns the status and the body of the response received
func makeAPICall(t *testing.T, url string) (status int, response string) {
	t.Helper()
	res, err := http.Get(url)
	require.NoError(t, err)
	bodyBytes, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	body := string(bodyBytes)
	return res.StatusCode, body
}
