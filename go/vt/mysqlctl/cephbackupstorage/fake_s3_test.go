/*
Copyright 2026 The Vitess Authors.

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

package cephbackupstorage

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// fakeS3 is an in-memory S3 server that implements the subset of the API the
// ceph backup plugin uses: HeadBucket, CreateBucket, PutObject, GetObject,
// DeleteObject and ListObjectsV2 (with an optional "/" delimiter). It is
// path-style only: the bucket is the first path segment. It verifies each
// request's AWS Signature V4 the way a gateway does, and records every
// request so tests can assert on how the client talked to it.
type fakeS3 struct {
	accessKey string
	secretKey string
	mu        sync.Mutex
	buckets   map[string]map[string][]byte // bucket -> key -> body
	requests  []*http.Request
	server    *httptest.Server
}

func newFakeS3(accessKey, secretKey string) *fakeS3 {
	f := &fakeS3{
		accessKey: accessKey,
		secretKey: secretKey,
		buckets:   map[string]map[string][]byte{},
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

// endpoint returns host:port without a scheme, the form the plugin's config
// file carries in endPoint.
func (f *fakeS3) endpoint() string {
	return strings.TrimPrefix(f.server.URL, "http://")
}

func (f *fakeS3) close() { f.server.Close() }

// objects returns the keys stored in bucket, sorted.
func (f *fakeS3) objects(bucket string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.buckets[bucket]))
	for k := range f.buckets[bucket] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// recorded returns a copy of every request seen so far.
func (f *fakeS3) recorded() []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*http.Request(nil), f.requests...)
}

// verifySigV4 checks the request's AWS Signature V4 the way a gateway does:
// it rebuilds the canonical request from what actually arrived on the wire
// and compares signatures. A header the client signed but never sent, such
// as a Content-Length on a body that went out chunked, therefore fails here
// just as it would against Ceph RGW. The payload hash is taken from
// x-amz-content-sha256 verbatim, so an unsigned payload verifies without
// reading the body.
func (f *fakeS3) verifySigV4(r *http.Request) error {
	const algorithm = "AWS4-HMAC-SHA256"
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, algorithm+" ") {
		return fmt.Errorf("not SigV4: %q", auth)
	}
	fields := map[string]string{}
	for part := range strings.SplitSeq(strings.TrimPrefix(auth, algorithm+" "), ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return fmt.Errorf("malformed Authorization: %q", auth)
		}
		fields[k] = v
	}

	// Credential=<accessKey>/<date>/<region>/s3/aws4_request
	cred := strings.Split(fields["Credential"], "/")
	if len(cred) != 5 || cred[0] != f.accessKey || cred[3] != "s3" || cred[4] != "aws4_request" {
		return fmt.Errorf("unexpected credential %q", fields["Credential"])
	}
	date, region := cred[1], cred[2]
	scope := strings.Join(cred[1:], "/")

	signedHeaders := fields["SignedHeaders"]
	var canonicalHeaders strings.Builder
	for name := range strings.SplitSeq(signedHeaders, ";") {
		var values []string
		switch name {
		case "host":
			values = []string{r.Host}
		case "content-length":
			if r.Header.Get("Content-Length") == "" {
				return errors.New("content-length signed but not sent")
			}
			values = r.Header.Values(name)
		default:
			values = r.Header.Values(name)
		}
		cleaned := make([]string, 0, len(values))
		for _, v := range values {
			cleaned = append(cleaned, strings.Join(strings.Fields(v), " "))
		}
		canonicalHeaders.WriteString(name + ":" + strings.Join(cleaned, ",") + "\n")
	}

	query := r.URL.Query()
	for k := range query {
		sort.Strings(query[k])
	}
	canonicalRequest := strings.Join([]string{
		r.Method,
		r.URL.EscapedPath(),
		strings.ReplaceAll(query.Encode(), "+", "%20"),
		canonicalHeaders.String(),
		signedHeaders,
		r.Header.Get("X-Amz-Content-Sha256"),
	}, "\n")
	stringToSign := strings.Join([]string{
		algorithm,
		r.Header.Get("X-Amz-Date"),
		scope,
		hex.EncodeToString(sha256Sum([]byte(canonicalRequest))),
	}, "\n")

	key := []byte("AWS4" + f.secretKey)
	for _, part := range []string{date, region, "s3", "aws4_request"} {
		key = hmacSHA256(key, []byte(part))
	}
	want := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))
	if want != fields["Signature"] {
		return fmt.Errorf("signature mismatch for %s %s (SignedHeaders=%s)", r.Method, r.URL.RequestURI(), signedHeaders)
	}
	return nil
}

func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func (f *fakeS3) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Clone(r.Context()))
	f.mu.Unlock()

	if err := f.verifySigV4(r); err != nil {
		writeS3Error(w, http.StatusForbidden, "SignatureDoesNotMatch", err.Error())
		return
	}

	// Path style: /bucket or /bucket/key...
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	objs, bucketExists := f.buckets[bucket]

	switch {
	case key == "" && r.Method == http.MethodHead:
		if !bucketExists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case key == "" && r.Method == http.MethodPut:
		if !bucketExists {
			f.buckets[bucket] = map[string][]byte{}
		}
		w.WriteHeader(http.StatusOK)
	case key == "" && r.Method == http.MethodGet:
		if !bucketExists {
			writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
			return
		}
		f.list(w, r, objs)
	case r.Method == http.MethodPut:
		if !bucketExists {
			writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
			return
		}
		// The body is the raw payload. The AWS SDK only wraps a PutObject body
		// in aws-chunked framing (with a trailing checksum) over HTTPS, and this
		// server is plain HTTP, so there is no framing to strip here. Over plain
		// HTTP the SDK rejects an unseekable body client-side unless the payload
		// is sent as UNSIGNED-PAYLOAD, in which case it arrives unmodified.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		objs[key] = body
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet:
		body, ok := objs[key]
		if !bucketExists || !ok {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	case r.Method == http.MethodDelete:
		delete(objs, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unsupported: "+r.Method+" "+r.URL.Path, http.StatusNotImplemented)
	}
}

// list answers ListObjectsV2. With delimiter "/" it groups keys under the
// prefix by their next path segment into CommonPrefixes, the way S3 does.
func (f *fakeS3) list(w http.ResponseWriter, r *http.Request, objs map[string][]byte) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")

	type object struct {
		Key  string `xml:"Key"`
		Size int    `xml:"Size"`
	}
	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	type result struct {
		XMLName        xml.Name       `xml:"ListBucketResult"`
		Prefix         string         `xml:"Prefix"`
		Delimiter      string         `xml:"Delimiter,omitempty"`
		IsTruncated    bool           `xml:"IsTruncated"`
		KeyCount       int            `xml:"KeyCount"`
		Contents       []object       `xml:"Contents"`
		CommonPrefixes []commonPrefix `xml:"CommonPrefixes"`
	}

	res := result{Prefix: prefix, Delimiter: delimiter}
	seenPrefixes := map[string]bool{}
	keys := make([]string, 0, len(objs))
	for k := range objs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := strings.TrimPrefix(k, prefix)
		if delimiter != "" {
			if i := strings.Index(rest, delimiter); i >= 0 {
				cp := prefix + rest[:i+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					res.CommonPrefixes = append(res.CommonPrefixes, commonPrefix{Prefix: cp})
				}
				continue
			}
		}
		res.Contents = append(res.Contents, object{Key: k, Size: len(objs[k])})
	}
	res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(res)
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	type s3Error struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3Error{Code: code, Message: message})
}
