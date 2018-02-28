// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"google.golang.org/api/google-api-go-generator/internal/disco"
)

const googleDiscoveryURL = "https://www.googleapis.com/discovery/v1/apis"

var (
	apiToGenerate = flag.String("api", "*", "The API ID to generate, like 'tasks:v1'. A value of '*' means all.")
	useCache      = flag.Bool("cache", true, "Use cache of discovered Google API discovery documents.")
	genDir        = flag.String("gendir", "", "Directory to use to write out generated Go files")
	build         = flag.Bool("build", false, "Compile generated packages.")
	install       = flag.Bool("install", false, "Install generated packages.")
	apisURL       = flag.String("discoveryurl", googleDiscoveryURL, "URL to root discovery document")

	publicOnly = flag.Bool("publiconly", true, "Only build public, released APIs. Only applicable for Google employees.")

	jsonFile       = flag.String("api_json_file", "", "If non-empty, the path to a local file on disk containing the API to generate. Exclusive with setting --api.")
	output         = flag.String("output", "", "(optional) Path to source output file. If not specified, the API name and version are used to construct an output path (e.g. tasks/v1).")
	apiPackageBase = flag.String("api_pkg_base", "google.golang.org/api", "Go package prefix to use for all generated APIs.")
	baseURL        = flag.String("base_url", "", "(optional) Override the default service API URL. If empty, the service's root URL will be used.")
	headerPath     = flag.String("header_path", "", "If non-empty, prepend the contents of this file to generated services.")

	contextHTTPPkg = flag.String("ctxhttp_pkg", "golang.org/x/net/context/ctxhttp", "Go package path of the 'ctxhttp' package.")
	contextPkg     = flag.String("context_pkg", "golang.org/x/net/context", "Go package path of the 'context' package.")
	gensupportPkg  = flag.String("gensupport_pkg", "google.golang.org/api/gensupport", "Go package path of the 'api/gensupport' support package.")
	googleapiPkg   = flag.String("googleapi_pkg", "google.golang.org/api/googleapi", "Go package path of the 'api/googleapi' support package.")
)

// API represents an API to generate, as well as its state while it's
// generating.
type API struct {
	// Fields needed before generating code, to select and find the APIs
	// to generate.
	// These fields usually come from the "directory item" JSON objects
	// that are provided by the googleDiscoveryURL. We unmarshal a directory
	// item directly into this struct.
	ID            string `json:"id"`
	Name          string `json:"name"`
	Version       string `json:"version"`
	DiscoveryLink string `json:"discoveryRestUrl"` // absolute

	doc *disco.Document
	// TODO(jba): remove m when we've fully converted to using disco.
	m map[string]interface{}

	forceJSON     []byte // if non-nil, the JSON schema file. else fetched.
	usedNames     namePool
	schemas       map[string]*Schema // apiName -> schema
	responseTypes map[string]bool

	p  func(format string, args ...interface{}) // print raw
	pn func(format string, args ...interface{}) // print with newline
}

func (a *API) sortedSchemaNames() (names []string) {
	for name := range a.schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	return
}

func (a *API) Schema(name string) *Schema {
	return a.schemas[name]
}

type generateError struct {
	api   *API
	error error
}

func (e *generateError) Error() string {
	return fmt.Sprintf("API %s failed to generate code: %v", e.api.ID, e.error)
}

type compileError struct {
	api    *API
	output string
}

func (e *compileError) Error() string {
	return fmt.Sprintf("API %s failed to compile:\n%v", e.api.ID, e.output)
}

func main() {
	flag.Parse()

	if *install {
		*build = true
	}

	var (
		apiIds  = []string{}
		matches = []*API{}
		errors  = []error{}
	)
	for _, api := range getAPIs() {
		apiIds = append(apiIds, api.ID)
		if !api.want() {
			continue
		}
		matches = append(matches, api)
		log.Printf("Generating API %s", api.ID)
		err := api.WriteGeneratedCode()
		if err != nil {
			errors = append(errors, &generateError{api, err})
			continue
		}
		if *build {
			var args []string
			if *install {
				args = append(args, "install")
			} else {
				args = append(args, "build")
			}
			args = append(args, api.Target())
			out, err := exec.Command("go", args...).CombinedOutput()
			if err != nil {
				errors = append(errors, &compileError{api, string(out)})
			}
		}
	}

	if len(matches) == 0 {
		log.Fatalf("No APIs matched %q; options are %v", *apiToGenerate, apiIds)
	}

	if len(errors) > 0 {
		log.Printf("%d API(s) failed to generate or compile:", len(errors))
		for _, ce := range errors {
			log.Printf(ce.Error())
		}
		os.Exit(1)
	}
}

func (a *API) want() bool {
	if *jsonFile != "" {
		// Return true early, before calling a.JSONFile()
		// which will require a GOPATH be set.  This is for
		// integration with Google's build system genrules
		// where there is no GOPATH.
		return true
	}
	// Skip this API if we're in cached mode and the files don't exist on disk.
	if *useCache {
		if _, err := os.Stat(a.JSONFile()); os.IsNotExist(err) {
			return false
		}
	}
	return *apiToGenerate == "*" || *apiToGenerate == a.ID
}

func getAPIs() []*API {
	if *jsonFile != "" {
		return getAPIsFromFile()
	}
	var bytes []byte
	var source string
	apiListFile := filepath.Join(genDirRoot(), "api-list.json")
	if *useCache {
		if !*publicOnly {
			log.Fatalf("-cached=true not compatible with -publiconly=false")
		}
		var err error
		bytes, err = ioutil.ReadFile(apiListFile)
		if err != nil {
			log.Fatal(err)
		}
		source = apiListFile
	} else {
		bytes = slurpURL(*apisURL)
		if *publicOnly {
			if err := writeFile(apiListFile, bytes); err != nil {
				log.Fatal(err)
			}
		}
		source = *apisURL
	}
	apis, err := unmarshalAPIs(bytes)
	if err != nil {
		log.Fatalf("error decoding JSON in %s: %v", source, err)
	}
	if !*publicOnly && *apiToGenerate != "*" {
		apis = append(apis, apiFromID(*apiToGenerate))
	}
	return apis
}

func unmarshalAPIs(bytes []byte) ([]*API, error) {
	var itemObj struct{ Items []*API }
	if err := json.Unmarshal(bytes, &itemObj); err != nil {
		return nil, err
	}
	return itemObj.Items, nil
}

func apiFromID(apiID string) *API {
	parts := strings.Split(apiID, ":")
	if len(parts) != 2 {
		log.Fatalf("malformed API name: %q", apiID)
	}
	return &API{
		ID:      apiID,
		Name:    parts[0],
		Version: parts[1],
	}
}

// getAPIsFromFile handles the case of generating exactly one API
// from the flag given in --api_json_file
func getAPIsFromFile() []*API {
	if *apiToGenerate != "*" {
		log.Fatalf("Can't set --api with --api_json_file.")
	}
	if !*publicOnly {
		log.Fatalf("Can't set --publiconly with --api_json_file.")
	}
	a, err := apiFromFile(*jsonFile)
	if err != nil {
		log.Fatal(err)
	}
	return []*API{a}
}

func apiFromFile(file string) (*API, error) {
	jsonBytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("Error reading %s: %v", file, err)
	}
	doc, err := disco.NewDocument(jsonBytes)
	if err != nil {
		return nil, fmt.Errorf("reading document from %q: %v", file, err)
	}
	a := &API{
		ID:        doc.ID,
		Name:      doc.Name,
		Version:   doc.Version,
		forceJSON: jsonBytes,
		doc:       doc,
	}
	return a, nil
}

func writeFile(file string, contents []byte) error {
	// Don't write it if the contents are identical.
	existing, err := ioutil.ReadFile(file)
	if err == nil && (bytes.Equal(existing, contents) || basicallyEqual(existing, contents)) {
		return nil
	}
	outdir := filepath.Dir(file)
	if err = os.MkdirAll(outdir, 0755); err != nil {
		return fmt.Errorf("failed to Mkdir %s: %v", outdir, err)
	}
	return ioutil.WriteFile(file, contents, 0644)
}

var ignoreLines = regexp.MustCompile(`(?m)^\s+"(?:etag|revision)": ".+\n`)

// basicallyEqual reports whether a and b are equal except for boring
// differences like ETag updates.
func basicallyEqual(a, b []byte) bool {
	return ignoreLines.Match(a) && ignoreLines.Match(b) &&
		bytes.Equal(ignoreLines.ReplaceAll(a, nil), ignoreLines.ReplaceAll(b, nil))
}

func slurpURL(urlStr string) []byte {
	if *useCache {
		log.Fatalf("Invalid use of slurpURL in cached mode for URL %s", urlStr)
	}
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		log.Fatal(err)
	}
	if *publicOnly {
		req.Header.Add("X-User-IP", "0.0.0.0") // hack
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Error fetching URL %s: %v", urlStr, err)
	}
	bs, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Error reading body of URL %s: %v", urlStr, err)
	}
	return bs
}

func panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// namePool keeps track of used names and assigns free ones based on a
// preferred name
type namePool struct {
	m map[string]bool // lazily initialized
}

// oddVersionRE matches unusual API names like directory_v1.
var oddVersionRE = regexp.MustCompile(`^(.+)_(v[\d\.]+)$`)

// renameVersion conditionally rewrites the provided version such
// that the final path component of the import path doesn't look
// like a Go identifier. This keeps the consistency that import paths
// for the generated Go packages look like:
//     google.golang.org/api/NAME/v<version>
// and have package NAME.
// See https://github.com/google/google-api-go-client/issues/78
func renameVersion(version string) string {
	if version == "alpha" || version == "beta" {
		return "v0." + version
	}
	if m := oddVersionRE.FindStringSubmatch(version); m != nil {
		return m[1] + "/" + m[2]
	}
	return version
}

func (p *namePool) Get(preferred string) string {
	if p.m == nil {
		p.m = make(map[string]bool)
	}
	name := preferred
	tries := 0
	for p.m[name] {
		tries++
		name = fmt.Sprintf("%s%d", preferred, tries)
	}
	p.m[name] = true
	return name
}

func genDirRoot() string {
	if *genDir != "" {
		return *genDir
	}
	paths := filepath.SplitList(os.Getenv("GOPATH"))
	if len(paths) == 0 {
		log.Fatalf("No GOPATH set.")
	}
	return filepath.Join(paths[0], "src", "google.golang.org", "api")
}

func (a *API) SourceDir() string {
	return filepath.Join(genDirRoot(), a.Package(), renameVersion(a.Version))
}

func (a *API) DiscoveryURL() string {
	if a.DiscoveryLink == "" {
		log.Fatalf("API %s has no DiscoveryLink", a.ID)
	}
	return a.DiscoveryLink
}

func (a *API) Package() string {
	return strings.ToLower(a.Name)
}

func (a *API) Target() string {
	return fmt.Sprintf("%s/%s/%s", *apiPackageBase, a.Package(), renameVersion(a.Version))
}

// ServiceType returns the name of the type to use for the root API struct
// (typically "Service").
func (a *API) ServiceType() string {
	switch a.Name {
	case "appengine", "content", "servicemanagement":
		return "APIService"
	default:
		return "Service"
	}
}

// GetName returns a free top-level function/type identifier in the package.
// It tries to return your preferred match if it's free.
func (a *API) GetName(preferred string) string {
	return a.usedNames.Get(preferred)
}

func (a *API) apiBaseURL() string {
	var base, rel string
	switch {
	case *baseURL != "":
		base, rel = *baseURL, a.doc.BasePath
	case a.doc.RootURL != "":
		base, rel = a.doc.RootURL, a.doc.ServicePath
	default:
		base, rel = *apisURL, a.doc.BasePath
	}
	return resolveRelative(base, rel)
}

func (a *API) needsDataWrapper() bool {
	for _, feature := range a.doc.Features {
		if feature == "dataWrapper" {
			return true
		}
	}
	return false
}

func (a *API) jsonBytes() []byte {
	if v := a.forceJSON; v != nil {
		return v
	}
	if *useCache {
		slurp, err := ioutil.ReadFile(a.JSONFile())
		if err != nil {
			log.Fatal(err)
		}
		return slurp
	}
	return slurpURL(a.DiscoveryURL())
}

func (a *API) JSONFile() string {
	return filepath.Join(a.SourceDir(), a.Package()+"-api.json")
}

func (a *API) WriteGeneratedCode() error {
	genfilename := *output
	if genfilename == "" {
		if err := writeFile(a.JSONFile(), a.jsonBytes()); err != nil {
			return err
		}
		outdir := a.SourceDir()
		err := os.MkdirAll(outdir, 0755)
		if err != nil {
			return fmt.Errorf("failed to Mkdir %s: %v", outdir, err)
		}
		pkg := a.Package()
		genfilename = filepath.Join(outdir, pkg+"-gen.go")
	}

	code, err := a.GenerateCode()
	errw := writeFile(genfilename, code)
	if err == nil {
		err = errw
	}
	return err
}

var docsLink string

func (a *API) GenerateCode() ([]byte, error) {
	pkg := a.Package()

	a.m = make(map[string]interface{})
	m := a.m
	jsonBytes := a.jsonBytes()
	var err error
	if a.doc == nil {
		a.doc, err = disco.NewDocument(jsonBytes)
		if err != nil {
			return nil, err
		}
	}
	// TODO(jba): remove when we can rely completely on a.doc
	err = json.Unmarshal(jsonBytes, &a.m)
	if err != nil {
		return nil, err
	}

	// Buffer the output in memory, for gofmt'ing later.
	var buf bytes.Buffer
	a.p = func(format string, args ...interface{}) {
		_, err := fmt.Fprintf(&buf, format, args...)
		if err != nil {
			panic(err)
		}
	}
	a.pn = func(format string, args ...interface{}) {
		a.p(format+"\n", args...)
	}
	wf := func(path string) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(&buf, f)
		return err
	}

	p, pn := a.p, a.pn

	if *headerPath != "" {
		if err := wf(*headerPath); err != nil {
			return nil, err
		}
	}

	pn("// Package %s provides access to the %s.", pkg, jstr(m, "title"))
	docsLink = jstr(m, "documentationLink")
	if docsLink != "" {
		pn("//")
		pn("// See %s", docsLink)
	}
	pn("//\n// Usage example:")
	pn("//")
	pn("//   import %q", a.Target())
	pn("//   ...")
	pn("//   %sService, err := %s.New(oauthHttpClient)", pkg, pkg)

	pn("package %s // import %q", pkg, a.Target())
	p("\n")
	pn("import (")
	for _, imp := range []struct {
		pkg   string
		lname string
	}{
		{"bytes", ""},
		{"encoding/json", ""},
		{"errors", ""},
		{"fmt", ""},
		{"io", ""},
		{"net/http", ""},
		{"net/url", ""},
		{"strconv", ""},
		{"strings", ""},
		{*contextHTTPPkg, "ctxhttp"},
		{*contextPkg, "context"},
		{*gensupportPkg, "gensupport"},
		{*googleapiPkg, "googleapi"},
	} {
		if imp.lname == "" {
			pn("  %q", imp.pkg)
		} else {
			pn("  %s %q", imp.lname, imp.pkg)
		}
	}
	pn(")")
	pn("\n// Always reference these packages, just in case the auto-generated code")
	pn("// below doesn't.")
	pn("var _ = bytes.NewBuffer")
	pn("var _ = strconv.Itoa")
	pn("var _ = fmt.Sprintf")
	pn("var _ = json.NewDecoder")
	pn("var _ = io.Copy")
	pn("var _ = url.Parse")
	pn("var _ = gensupport.MarshalJSON")
	pn("var _ = googleapi.Version")
	pn("var _ = errors.New")
	pn("var _ = strings.Replace")
	pn("var _ = context.Canceled")
	pn("var _ = ctxhttp.Do")
	pn("")
	pn("const apiId = %q", jstr(m, "id"))
	pn("const apiName = %q", jstr(m, "name"))
	pn("const apiVersion = %q", jstr(m, "version"))
	pn("const basePath = %q", a.apiBaseURL())

	a.generateScopeConstants()

	service := a.ServiceType()

	// Reserve names (ignore return value; we're the first caller).
	a.GetName("New")
	a.GetName(service)

	pn("func New(client *http.Client) (*%s, error) {", service)
	pn("if client == nil { return nil, errors.New(\"client is nil\") }")
	pn("s := &%s{client: client, BasePath: basePath}", service)
	for _, res := range a.doc.Resources { // add top level resources.
		pn("s.%s = New%s(s)", resourceGoField(res), resourceGoType(res))
	}
	pn("return s, nil")
	pn("}")

	pn("\ntype %s struct {", service)
	pn(" client *http.Client")
	pn(" BasePath string // API endpoint base URL")
	pn(" UserAgent string // optional additional User-Agent fragment")

	for _, res := range a.doc.Resources {
		pn("\n\t%s\t*%s", resourceGoField(res), resourceGoType(res))
	}
	pn("}")
	pn("\nfunc (s *%s) userAgent() string {", service)
	pn(` if s.UserAgent == "" { return googleapi.UserAgent }`)
	pn(` return googleapi.UserAgent + " " + s.UserAgent`)
	pn("}\n")

	for _, res := range a.doc.Resources {
		a.generateResource(res)
	}

	a.PopulateSchemas()

	a.responseTypes = make(map[string]bool)
	for _, meth := range a.APIMethods() {
		meth.cacheResponseTypes(a)
	}
	for _, res := range a.doc.Resources {
		a.cacheResourceResponseTypes(res)
	}

	for _, name := range a.sortedSchemaNames() {
		a.schemas[name].writeSchemaCode(a)
	}

	for _, meth := range a.APIMethods() {
		meth.generateCode()
	}

	for _, res := range a.doc.Resources {
		a.generateResourceMethods(res)
	}

	clean, err := format.Source(buf.Bytes())
	if err != nil {
		return buf.Bytes(), err
	}
	return clean, nil
}

func (a *API) generateScopeConstants() {
	scopes := a.doc.Auth.OAuth2Scopes
	if len(scopes) == 0 {
		return
	}

	a.pn("// OAuth2 scopes used by this API.")
	a.pn("const (")
	n := 0
	for _, scope := range scopes {
		if n > 0 {
			a.p("\n")
		}
		n++
		ident := scopeIdentifierFromURL(scope.URL)
		if scope.Description != "" {
			a.p("%s", asComment("\t", scope.Description))
		}
		a.pn("\t%s = %q", ident, scope.URL)
	}
	a.p(")\n\n")
}

func scopeIdentifierFromURL(urlStr string) string {
	const prefix = "https://www.googleapis.com/auth/"
	if !strings.HasPrefix(urlStr, prefix) {
		const https = "https://"
		if !strings.HasPrefix(urlStr, https) {
			log.Fatalf("Unexpected oauth2 scope %q doesn't start with %q", urlStr, https)
		}
		ident := validGoIdentifer(depunct(urlStr[len(https):], true)) + "Scope"
		return ident
	}
	ident := validGoIdentifer(initialCap(urlStr[len(prefix):])) + "Scope"
	return ident
}

// Schema is a disco.Schema that has been bestowed an identifier, whether by
// having an "id" field at the top of the schema or with an
// automatically generated one in populateSubSchemas.
//
// TODO: While sub-types shouldn't need to be promoted to schemas,
// API.GenerateCode iterates over API.schemas to figure out what
// top-level Go types to write.  These should be separate concerns.
type Schema struct {
	api *API

	typ *disco.Schema // lazily populated by Type

	apiName      string // the native API-defined name of this type
	goName       string // lazily populated by GoName
	goReturnType string // lazily populated by GoReturnType
}

type Property struct {
	s       *Schema       // property of which schema
	apiName string        // the native API-defined name of this property
	typ     *disco.Schema // lazily populated by Type
}

func (p *Property) Type() *disco.Schema {
	return p.typ
}

func (p *Property) GoName() string {
	return initialCap(p.apiName)
}

func (p *Property) APIName() string {
	return p.apiName
}

func (p *Property) Default() string {
	return p.typ.Default
}

func (p *Property) Description() string {
	return p.typ.Description
}

func (p *Property) Enum() ([]string, bool) {
	if p.typ.Enums != nil {
		return p.typ.Enums, true
	}
	// Check if this has an array of string enums.
	if p.typ.ItemSchema != nil {
		if enums := p.typ.ItemSchema.Enums; enums != nil && p.typ.ItemSchema.Type == "string" {
			return enums, true
		}
	}
	return nil, false
}

func (p *Property) EnumDescriptions() []string {
	if desc := p.typ.EnumDescriptions; desc != nil {
		return desc
	}
	// Check if this has an array of string enum descriptions.
	if items := p.typ.ItemSchema; items != nil {
		if desc := items.EnumDescriptions; desc != nil {
			return desc
		}
	}
	return nil
}

func (p *Property) Pattern() (string, bool) {
	return p.typ.Pattern, (p.typ.Pattern != "")
}

func (p *Property) TypeAsGo() string {
	return p.s.api.typeAsGo(p.typ, false)
}

// A FieldName uniquely identifies a field within a Schema struct for an API.
type fieldName struct {
	api    string // The ID of an API.
	schema string // The Go name of a Schema struct.
	field  string // The Go name of a field.
}

// pointerFields is a list of fields that should use a pointer type.
// This makes it possible to distinguish between a field being unset vs having
// an empty value.
var pointerFields = []fieldName{
	{api: "cloudmonitoring:v2beta2", schema: "Point", field: "BoolValue"},
	{api: "cloudmonitoring:v2beta2", schema: "Point", field: "DoubleValue"},
	{api: "cloudmonitoring:v2beta2", schema: "Point", field: "Int64Value"},
	{api: "cloudmonitoring:v2beta2", schema: "Point", field: "StringValue"},
	{api: "compute:v1", schema: "MetadataItems", field: "Value"},
	{api: "content:v2", schema: "AccountUser", field: "Admin"},
	{api: "datastore:v1beta2", schema: "Property", field: "BlobKeyValue"},
	{api: "datastore:v1beta2", schema: "Property", field: "BlobValue"},
	{api: "datastore:v1beta2", schema: "Property", field: "BooleanValue"},
	{api: "datastore:v1beta2", schema: "Property", field: "DateTimeValue"},
	{api: "datastore:v1beta2", schema: "Property", field: "DoubleValue"},
	{api: "datastore:v1beta2", schema: "Property", field: "Indexed"},
	{api: "datastore:v1beta2", schema: "Property", field: "IntegerValue"},
	{api: "datastore:v1beta2", schema: "Property", field: "StringValue"},
	{api: "datastore:v1beta3", schema: "Value", field: "BlobValue"},
	{api: "datastore:v1beta3", schema: "Value", field: "BooleanValue"},
	{api: "datastore:v1beta3", schema: "Value", field: "DoubleValue"},
	{api: "datastore:v1beta3", schema: "Value", field: "IntegerValue"},
	{api: "datastore:v1beta3", schema: "Value", field: "StringValue"},
	{api: "datastore:v1beta3", schema: "Value", field: "TimestampValue"},
	{api: "genomics:v1beta2", schema: "Dataset", field: "IsPublic"},
	{api: "monitoring:v3", schema: "TypedValue", field: "BoolValue"},
	{api: "monitoring:v3", schema: "TypedValue", field: "DoubleValue"},
	{api: "monitoring:v3", schema: "TypedValue", field: "Int64Value"},
	{api: "monitoring:v3", schema: "TypedValue", field: "StringValue"},
	{api: "servicecontrol:v1", schema: "MetricValue", field: "BoolValue"},
	{api: "servicecontrol:v1", schema: "MetricValue", field: "DoubleValue"},
	{api: "servicecontrol:v1", schema: "MetricValue", field: "Int64Value"},
	{api: "servicecontrol:v1", schema: "MetricValue", field: "StringValue"},
	{api: "tasks:v1", schema: "Task", field: "Completed"},
	{api: "youtube:v3", schema: "ChannelSectionSnippet", field: "Position"},
}

// forcePointerType reports whether p should be represented as a pointer type in its parent schema struct.
func (p *Property) forcePointerType() bool {
	if p.UnfortunateDefault() {
		return true
	}

	name := fieldName{api: p.s.api.ID, schema: p.s.GoName(), field: p.GoName()}
	for _, pf := range pointerFields {
		if pf == name {
			return true
		}
	}
	return false
}

// UnfortunateDefault reports whether p may be set to a zero value, but has a non-zero default.
func (p *Property) UnfortunateDefault() bool {
	switch p.TypeAsGo() {
	default:
		return false

	case "bool":
		return p.Default() == "true"

	case "string":
		if p.Default() == "" {
			return false
		}
		// String fields are considered to "allow" a zero value if either:
		//  (a) they are an enum, and one of the permitted enum values is the empty string, or
		//  (b) they have a validation pattern which matches the empty string.
		pattern, hasPat := p.Pattern()
		enum, hasEnum := p.Enum()
		if hasPat && hasEnum {
			log.Printf("Encountered enum property which also has a pattern: %#v", p)
			return false // don't know how to handle this, so ignore.
		}
		return (hasPat && emptyPattern(pattern)) ||
			(hasEnum && emptyEnum(enum))

	case "float64", "int64", "uint64", "int32", "uint32":
		if p.Default() == "" {
			return false
		}
		if f, err := strconv.ParseFloat(p.Default(), 64); err == nil {
			return f != 0.0
		}
		// The default value has an unexpected form.  Whatever it is, it's non-zero.
		return true
	}
}

// emptyPattern reports whether a pattern matches the empty string.
func emptyPattern(pattern string) bool {
	if re, err := regexp.Compile(pattern); err == nil {
		return re.MatchString("")
	}
	log.Printf("Encountered bad pattern: %s", pattern)
	return false
}

// emptyEnum reports whether a property enum list contains the empty string.
func emptyEnum(enum []string) bool {
	for _, val := range enum {
		if val == "" {
			return true
		}
	}
	return false
}

func isIntAsString(s *disco.Schema) bool {
	return s.Type == "string" && strings.Contains(s.Format, "int")
}

func (a *API) typeAsGo(s *disco.Schema, elidePointers bool) string {
	switch s.Kind {
	case disco.SimpleKind:
		return mustSimpleTypeConvert(s.Type, s.Format)
	case disco.ArrayKind:
		as := s.ElementSchema()
		if as.Type == "string" {
			switch as.Format {
			case "int64":
				return "googleapi.Int64s"
			case "uint64":
				return "googleapi.Uint64s"
			case "int32":
				return "googleapi.Int32s"
			case "uint32":
				return "googleapi.Uint32s"
			case "float64":
				return "googleapi.Float64s"
			}
		}
		return "[]" + a.typeAsGo(as, elidePointers)
	case disco.ReferenceKind:
		rs := s.RefSchema
		if rs.Kind == disco.SimpleKind {
			// Simple top-level schemas get named types (see writeSchemaCode).
			// Use the name instead of using the equivalent simple Go type.
			return a.schemaNamed(rs.Name).GoName()
		}
		return a.typeAsGo(rs, elidePointers)
	case disco.MapKind:
		// Due to historical baggage (maps used to be a separate code path),
		// the element types of maps never have pointers in them.  From this
		// level down, elide pointers in types.
		return "map[string]" + a.typeAsGo(s.ElementSchema(), true)
	case disco.AnyStructKind:
		return "googleapi.RawMessage"
	case disco.StructKind:
		tls := a.schemaNamed(s.Name)
		if elidePointers || s.Variant != nil {
			return tls.GoName()
		}
		return "*" + tls.GoName()
	default:
		panic(fmt.Sprintf("unhandled typeAsGo for %+v", s))
	}
}

func (a *API) schemaNamed(name string) *Schema {
	s := a.schemas[name]
	if s == nil {
		panicf("no top-level schema named %q", name)
	}
	return s
}

func (s *Schema) properties() []*Property {
	if s.typ.Kind != disco.StructKind {
		panic("called properties on non-object schema")
	}
	pl := []*Property{}
	propMap := s.typ.Properties
	for _, name := range sortedKeys(propMap) {
		pl = append(pl, &Property{
			s:       s,
			typ:     propMap[name],
			apiName: name,
		})
	}
	return pl
}

func (s *Schema) HasContentType() bool {
	for _, p := range s.properties() {
		if p.GoName() == "ContentType" && p.TypeAsGo() == "string" {
			return true
		}
	}
	return false
}

func (s *Schema) populateSubSchemas() (outerr error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		outerr = fmt.Errorf("%v", r)
	}()

	addSubStruct := func(subApiName string, t *disco.Schema) {
		if s.api.schemas[subApiName] != nil {
			panic("dup schema apiName: " + subApiName)
		}
		if t.Name != "" {
			panic("subtype already has name: " + t.Name)
		}
		t.Name = subApiName
		subs := &Schema{
			api:     s.api,
			typ:     t,
			apiName: subApiName,
		}
		s.api.schemas[subApiName] = subs
		err := subs.populateSubSchemas()
		if err != nil {
			panicf("in sub-struct %q: %v", subApiName, err)
		}
	}

	switch s.typ.Kind {
	case disco.StructKind:
		for _, p := range s.properties() {
			subApiName := fmt.Sprintf("%s.%s", s.apiName, p.apiName)
			switch p.Type().Kind {
			case disco.SimpleKind, disco.ReferenceKind, disco.AnyStructKind:
				// Do nothing.
			case disco.MapKind:
				mt := p.Type().ElementSchema()
				if mt.Kind == disco.SimpleKind || mt.Kind == disco.ReferenceKind {
					continue
				}
				addSubStruct(subApiName, mt)
			case disco.ArrayKind:
				at := p.Type().ElementSchema()
				if at.Kind == disco.SimpleKind || at.Kind == disco.ReferenceKind {
					continue
				}
				addSubStruct(subApiName, at)
			case disco.StructKind:
				addSubStruct(subApiName, p.Type())
			default:
				panicf("Unknown type for %q: %s", subApiName, p.Type())
			}
		}
	case disco.ArrayKind:
		subApiName := fmt.Sprintf("%s.Item", s.apiName)
		switch at := s.typ.ElementSchema(); at.Kind {
		case disco.SimpleKind, disco.ReferenceKind, disco.AnyStructKind:
			// Do nothing.
		case disco.MapKind:
			mt := at.ElementSchema()
			if k := mt.Kind; k != disco.SimpleKind && k != disco.ReferenceKind {
				addSubStruct(subApiName, mt)
			}
		case disco.ArrayKind:
			at := at.ElementSchema()
			if k := at.Kind; k != disco.SimpleKind && k != disco.ReferenceKind {
				addSubStruct(subApiName, at)
			}
		case disco.StructKind:
			addSubStruct(subApiName, at)
		default:
			panicf("Unknown array type for %q: %s", subApiName, at)
		}
	case disco.AnyStructKind, disco.MapKind, disco.SimpleKind, disco.ReferenceKind:
		// Do nothing.
	default:
		fmt.Fprintf(os.Stderr, "in populateSubSchemas, schema is: %v", s.typ)
		panicf("populateSubSchemas: unsupported type for schema %q", s.apiName)
		panic("unreachable")
	}
	return nil
}

// GoName returns (or creates and returns) the bare Go name
// of the apiName, making sure that it's a proper Go identifier
// and doesn't conflict with an existing name.
func (s *Schema) GoName() string {
	if s.goName == "" {
		if s.typ.Kind == disco.MapKind {
			s.goName = s.api.typeAsGo(s.typ, false)
		} else {
			base := initialCap(s.apiName)
			s.goName = s.api.GetName(base)
			if base == "Service" && s.goName != "Service" {
				// Detect the case where a resource is going to clash with the
				// root service object.
				panicf("Clash on name Service")
			}
		}
	}
	return s.goName
}

// GoReturnType returns the Go type to use as the return type.
// If a type is a struct, it will return *StructType,
// for a map it will return map[string]ValueType,
// for (not yet supported) slices it will return []ValueType.
func (s *Schema) GoReturnType() string {
	if s.goReturnType == "" {
		if s.typ.Kind == disco.MapKind {
			s.goReturnType = s.GoName()
		} else {
			s.goReturnType = "*" + s.GoName()
		}
	}
	return s.goReturnType
}

func (s *Schema) writeSchemaCode(api *API) {
	switch s.typ.Kind {
	case disco.SimpleKind:
		apitype := s.typ.Type
		typ := mustSimpleTypeConvert(apitype, s.typ.Format)
		s.api.pn("\ntype %s %s", s.GoName(), typ)
	case disco.StructKind:
		s.writeSchemaStruct(api)
	case disco.MapKind, disco.AnyStructKind:
		// Do nothing.
	case disco.ArrayKind:
		log.Printf("TODO writeSchemaCode for arrays for %s", s.GoName())
	default:
		fmt.Fprintf(os.Stderr, "in writeSchemaCode, schema is: %+v", s.typ)
		panicf("writeSchemaCode: unsupported type for schema %q", s.apiName)
	}
}

func (s *Schema) writeVariant(api *API, v *disco.Variant) {
	s.api.p("\ntype %s map[string]interface{}\n\n", s.GoName())

	// Write out the "Type" method that identifies the variant type.
	s.api.pn("func (t %s) Type() string {", s.GoName())
	s.api.pn("  return googleapi.VariantType(t)")
	s.api.p("}\n\n")

	// Write out helper methods to convert each possible variant.
	for _, m := range v.Map {
		if m.TypeValue == "" && m.Ref == "" {
			log.Printf("TODO variant %s ref %s not yet supported.", m.TypeValue, m.Ref)
			continue
		}

		s.api.pn("func (t %s) %s() (r %s, ok bool) {", s.GoName(), initialCap(m.TypeValue), m.Ref)
		s.api.pn(" if t.Type() != %q {", initialCap(m.TypeValue))
		s.api.pn("  return r, false")
		s.api.pn(" }")
		s.api.pn(" ok = googleapi.ConvertVariant(map[string]interface{}(t), &r)")
		s.api.pn(" return r, ok")
		s.api.p("}\n\n")
	}
}

func (s *Schema) Description() string {
	return s.typ.Description
}

func (s *Schema) writeSchemaStruct(api *API) {
	if v := s.typ.Variant; v != nil {
		s.writeVariant(api, v)
		return
	}
	s.api.p("\n")
	des := s.Description()
	if des != "" {
		s.api.p("%s", asComment("", fmt.Sprintf("%s: %s", s.GoName(), des)))
	}
	s.api.pn("type %s struct {", s.GoName())

	np := new(namePool)
	forceSendName := np.Get("ForceSendFields")
	nullFieldsName := np.Get("NullFields")
	if s.isResponseType() {
		np.Get("ServerResponse") // reserve the name
	}

	firstFieldName := "" // used to store a struct field name for use in documentation.
	for i, p := range s.properties() {
		if i > 0 {
			s.api.p("\n")
		}
		pname := np.Get(p.GoName())
		des := p.Description()
		if des != "" {
			s.api.p("%s", asComment("\t", fmt.Sprintf("%s: %s", pname, des)))
		}
		addFieldValueComments(s.api.p, p, "\t", des != "")

		var extraOpt string
		if isIntAsString(p.Type()) {
			extraOpt += ",string"
		}

		typ := p.TypeAsGo()
		if p.forcePointerType() {
			typ = "*" + typ
		}

		s.api.pn(" %s %s `json:\"%s,omitempty%s\"`", pname, typ, p.APIName(), extraOpt)
		if firstFieldName == "" {
			firstFieldName = pname
		}
	}

	if s.isResponseType() {
		if firstFieldName != "" {
			s.api.p("\n")
		}
		s.api.p("%s", asComment("\t", "ServerResponse contains the HTTP response code and headers from the server."))
		s.api.pn(" googleapi.ServerResponse `json:\"-\"`")
	}

	if firstFieldName == "" {
		// There were no fields in the struct, so there is no point
		// adding any custom JSON marshaling code.
		s.api.pn("}")
		return
	}

	commentFmtStr := "%s is a list of field names (e.g. %q) to " +
		"unconditionally include in API requests. By default, fields " +
		"with empty values are omitted from API requests. However, " +
		"any non-pointer, non-interface field appearing in %s will " +
		"be sent to the server regardless of whether the field is " +
		"empty or not. This may be used to include empty fields in " +
		"Patch requests."
	comment := fmt.Sprintf(commentFmtStr, forceSendName, firstFieldName, forceSendName)
	s.api.p("\n")
	s.api.p("%s", asComment("\t", comment))

	s.api.pn("\t%s []string `json:\"-\"`", forceSendName)

	commentFmtStr = "%s is a list of field names (e.g. %q) to " +
		"include in API requests with the JSON null value. " +
		"By default, fields with empty values are omitted from API requests. However, " +
		"any field with an empty value appearing in %s will be sent to the server as null. " +
		"It is an error if a field in this list has a non-empty value. This may be used to " +
		"include null fields in Patch requests."
	comment = fmt.Sprintf(commentFmtStr, nullFieldsName, firstFieldName, nullFieldsName)
	s.api.p("\n")
	s.api.p("%s", asComment("\t", comment))

	s.api.pn("\t%s []string `json:\"-\"`", nullFieldsName)

	s.api.pn("}")
	s.writeSchemaMarshal(forceSendName, nullFieldsName)
}

// writeSchemaMarshal writes a custom MarshalJSON function for s, which allows
// fields to be explicitly transmitted by listing them in the field identified
// by forceSendFieldName, and allows fields to be transmitted with the null value
// by listing them in the field identified by nullFieldsName.
func (s *Schema) writeSchemaMarshal(forceSendFieldName, nullFieldsName string) {
	s.api.pn("func (s *%s) MarshalJSON() ([]byte, error) {", s.GoName())
	s.api.pn("\ttype noMethod %s", s.GoName())
	// pass schema as methodless type to prevent subsequent calls to MarshalJSON from recursing indefinitely.
	s.api.pn("\traw := noMethod(*s)")
	s.api.pn("\treturn gensupport.MarshalJSON(raw, s.%s, s.%s)", forceSendFieldName, nullFieldsName)
	s.api.pn("}")
}

// isResponseType returns true for all types that are used as a response.
func (s *Schema) isResponseType() bool {
	return s.api.responseTypes["*"+s.goName]
}

// PopulateSchemas reads all the API types ("schemas") from the JSON file
// and converts them to *Schema instances, returning an identically
// keyed map, additionally containing subresources.  For instance,
//
// A resource "Foo" of type "object" with a property "bar", also of type
// "object" (an anonymous sub-resource), will get a synthetic API name
// of "Foo.bar".
//
// A resource "Foo" of type "array" with an "items" of type "object"
// will get a synthetic API name of "Foo.Item".
func (a *API) PopulateSchemas() {
	if a.schemas != nil {
		panic("")
	}
	a.schemas = make(map[string]*Schema)
	for name, ds := range a.doc.Schemas {
		s := &Schema{
			api:     a,
			apiName: name,
			typ:     ds,
		}
		a.schemas[name] = s
		err := s.populateSubSchemas()
		if err != nil {
			panicf("Error populating schema with API name %q: %v", name, err)
		}
	}
}

func (a *API) generateResource(r *disco.Resource) {
	pn := a.pn
	t := resourceGoType(r)
	pn(fmt.Sprintf("func New%s(s *%s) *%s {", t, a.ServiceType(), t))
	pn("rs := &%s{s : s}", t)
	for _, res := range r.Resources {
		pn("rs.%s = New%s(s)", resourceGoField(res), resourceGoType(res))
	}
	pn("return rs")
	pn("}")

	pn("\ntype %s struct {", t)
	pn(" s *%s", a.ServiceType())
	for _, res := range r.Resources {
		pn("\n\t%s\t*%s", resourceGoField(res), resourceGoType(res))
	}
	pn("}")

	for _, res := range r.Resources {
		a.generateResource(res)
	}
}

func (a *API) cacheResourceResponseTypes(r *disco.Resource) {
	for _, meth := range a.resourceMethods(r) {
		meth.cacheResponseTypes(a)
	}
	for _, res := range r.Resources {
		a.cacheResourceResponseTypes(res)
	}
}

func (a *API) generateResourceMethods(r *disco.Resource) {
	for _, meth := range a.resourceMethods(r) {
		meth.generateCode()
	}
	for _, res := range r.Resources {
		a.generateResourceMethods(res)
	}
}

func resourceGoField(r *disco.Resource) string {
	return initialCap(r.Name)
}

func resourceGoType(r *disco.Resource) string {
	return initialCap(r.FullName + "Service")
}

func (a *API) resourceMethods(r *disco.Resource) []*Method {
	ms := []*Method{}
	for _, m := range r.Methods {
		ms = append(ms, &Method{
			api: a,
			r:   r,
			m:   m,
		})
	}
	return ms
}

type Method struct {
	api *API
	r   *disco.Resource // or nil if a API-level (top-level) method
	m   *disco.Method

	params []*Param // all Params, of each type, lazily set by first access to Parameters
}

func (m *Method) Id() string {
	return m.m.ID
}

func (m *Method) responseType() *Schema {
	return m.api.schemas[m.m.Response.Ref]
}

func (m *Method) supportsMediaUpload() bool {
	return m.m.MediaUpload != nil
}

func (m *Method) mediaUploadPath() string {
	return m.m.MediaUpload.Protocols["simple"].Path
}

func (m *Method) supportsMediaDownload() bool {
	if m.supportsMediaUpload() {
		// storage.objects.insert claims support for download in
		// addition to upload but attempting to do so fails.
		// This situation doesn't apply to any other methods.
		return false
	}
	return m.m.SupportsMediaDownload
}

func (m *Method) supportsPaging() (callField, respField string, ok bool) {
	if m.m.HTTPMethod != "GET" {
		// Probably a POST, like "calendar.acl.watch",
		// which, despite having a pageToken parameter,
		// isn't actually a paged method.
		return "", "", false
	}
	matches := m.grepParams(func(p *Param) bool { return p.p.Name == "pageToken" })
	if len(matches) == 0 {
		return "", "", false
	} else {
		pt := matches[0]
		if pt.p.Required {
			// The page token is a required parameter (e.g. because there is
			// a separate API call to start an iteration), and so the relevant
			// call factory method takes the page token instead.
			return "", "", false
		}
	}

	// Check that the response type has the next page token.
	// It may appear under different names.
	s := m.responseType()
	if s == nil || s.typ.Kind != disco.StructKind {
		return "", "", false
	}
	props := s.properties()

	opts := [...]string{
		"nextPageToken",
		"pageToken",
	}
	for _, n := range opts {
		for _, prop := range props {
			if prop.apiName == n && prop.Type().Type == "string" {
				return "PageToken", prop.GoName(), true
			}
		}
	}

	return "", "", false
}

func (m *Method) Params() []*Param {
	if m.params == nil {
		for _, p := range m.m.Parameters {
			m.params = append(m.params, &Param{
				method: m,
				p:      p,
			})
		}
	}
	return m.params
}

func (m *Method) grepParams(f func(*Param) bool) []*Param {
	matches := make([]*Param, 0)
	for _, param := range m.Params() {
		if f(param) {
			matches = append(matches, param)
		}
	}
	return matches
}

func (m *Method) NamedParam(name string) *Param {
	matches := m.grepParams(func(p *Param) bool {
		return p.p.Name == name
	})
	if len(matches) < 1 {
		log.Panicf("failed to find named parameter %q", name)
	}
	if len(matches) > 1 {
		log.Panicf("found multiple parameters for parameter name %q", name)
	}
	return matches[0]
}

func (m *Method) OptParams() []*Param {
	return m.grepParams(func(p *Param) bool {
		return !p.p.Required
	})
}

func (meth *Method) cacheResponseTypes(api *API) {
	if retType := responseType(api, meth.m); retType != "" && strings.HasPrefix(retType, "*") {
		api.responseTypes[retType] = true
	}
}

// convertMultiParams builds a []string temp variable from a slice
// of non-strings and returns the name of the temp variable.
func convertMultiParams(a *API, param string) string {
	a.pn(" var %v_ []string", param)
	a.pn(" for _, v := range %v {", param)
	a.pn("  %v_ = append(%v_, fmt.Sprint(v))", param, param)
	a.pn(" }")
	return param + "_"
}

func (meth *Method) generateCode() {
	res := meth.r // may be nil if a top-level method
	a := meth.api
	p, pn := a.p, a.pn

	pn("\n// method id %q:", meth.Id())

	retType := responseType(a, meth.m)
	retTypeComma := retType
	if retTypeComma != "" {
		retTypeComma += ", "
	}

	args := meth.NewArguments()
	methodName := initialCap(meth.m.Name)
	prefix := ""
	if res != nil {
		prefix = initialCap(res.FullName)
	}
	callName := a.GetName(prefix + methodName + "Call")

	pn("\ntype %s struct {", callName)
	pn(" s *%s", a.ServiceType())
	for _, arg := range args.l {
		if arg.location != "query" {
			pn(" %s %s", arg.goname, arg.gotype)
		}
	}
	pn(" urlParams_ gensupport.URLParams")
	httpMethod := meth.m.HTTPMethod
	if httpMethod == "GET" {
		pn(" ifNoneMatch_ string")
	}

	if meth.supportsMediaUpload() {
		// At most one of media_ and resumbableBuffer_ will be set.
		pn(" media_     io.Reader")
		pn(" mediaBuffer_ *gensupport.MediaBuffer")
		pn(" mediaType_ string")
		pn(" mediaSize_  int64 // mediaSize, if known.  Used only for calls to progressUpdater_.")
		pn(" progressUpdater_  googleapi.ProgressUpdater")
	}
	pn(" ctx_ context.Context")
	pn(" header_ http.Header")
	pn("}")

	p("\n%s", asComment("", methodName+": "+meth.m.Description))
	if res != nil {
		if url := canonicalDocsURL[fmt.Sprintf("%v%v/%v", docsLink, res.Name, meth.m.Name)]; url != "" {
			pn("// For details, see %v", url)
		}
	}

	var servicePtr string
	if res == nil {
		pn("func (s *Service) %s(%s) *%s {", methodName, args, callName)
		servicePtr = "s"
	} else {
		pn("func (r *%s) %s(%s) *%s {", resourceGoType(res), methodName, args, callName)
		servicePtr = "r.s"
	}

	pn(" c := &%s{s: %s, urlParams_: make(gensupport.URLParams)}", callName, servicePtr)
	for _, arg := range args.l {
		// TODO(gmlewis): clean up and consolidate this section.
		// See: https://code-review.googlesource.com/#/c/3520/18/google-api-go-generator/gen.go
		if arg.location == "query" {
			switch arg.gotype {
			case "[]string":
				pn(" c.urlParams_.SetMulti(%q, append([]string{}, %v...))", arg.apiname, arg.goname)
			case "string":
				pn(" c.urlParams_.Set(%q, %v)", arg.apiname, arg.goname)
			default:
				if strings.HasPrefix(arg.gotype, "[]") {
					tmpVar := convertMultiParams(a, arg.goname)
					pn(" c.urlParams_.SetMulti(%q, %v)", arg.apiname, tmpVar)
				} else {
					pn(" c.urlParams_.Set(%q, fmt.Sprint(%v))", arg.apiname, arg.goname)
				}
			}
			continue
		}
		if arg.gotype == "[]string" {
			pn(" c.%s = append([]string{}, %s...)", arg.goname, arg.goname) // Make a copy of the []string.
			continue
		}
		pn(" c.%s = %s", arg.goname, arg.goname)
	}
	pn(" return c")
	pn("}")

	for _, opt := range meth.OptParams() {
		if opt.p.Location != "query" {
			panicf("optional parameter has unsupported location %q", opt.p.Location)
		}
		setter := initialCap(opt.p.Name)
		des := opt.p.Description
		des = strings.Replace(des, "Optional.", "", 1)
		des = strings.TrimSpace(des)
		p("\n%s", asComment("", fmt.Sprintf("%s sets the optional parameter %q: %s", setter, opt.p.Name, des)))
		addFieldValueComments(p, opt, "", true)
		np := new(namePool)
		np.Get("c") // take the receiver's name
		paramName := np.Get(validGoIdentifer(opt.p.Name))
		typePrefix := ""
		if opt.p.Repeated {
			typePrefix = "..."
		}
		pn("func (c *%s) %s(%s %s%s) *%s {", callName, setter, paramName, typePrefix, opt.GoType(), callName)
		if opt.p.Repeated {
			if opt.GoType() == "string" {
				pn("c.urlParams_.SetMulti(%q, append([]string{}, %v...))", opt.p.Name, paramName)
			} else {
				tmpVar := convertMultiParams(a, paramName)
				pn(" c.urlParams_.SetMulti(%q, %v)", opt.p.Name, tmpVar)
			}
		} else {
			if opt.GoType() == "string" {
				pn("c.urlParams_.Set(%q, %v)", opt.p.Name, paramName)
			} else {
				pn("c.urlParams_.Set(%q, fmt.Sprint(%v))", opt.p.Name, paramName)
			}
		}
		pn("return c")
		pn("}")
	}

	if meth.supportsMediaUpload() {
		comment := "Media specifies the media to upload in one or more chunks. " +
			"The chunk size may be controlled by supplying a MediaOption generated by googleapi.ChunkSize. " +
			"The chunk size defaults to googleapi.DefaultUploadChunkSize." +
			"The Content-Type header used in the upload request will be determined by sniffing the contents of r, " +
			"unless a MediaOption generated by googleapi.ContentType is supplied." +
			"\nAt most one of Media and ResumableMedia may be set."
		// TODO(mcgreevy): Ensure that r is always closed before Do returns, and document this.
		// See comments on https://code-review.googlesource.com/#/c/3970/
		p("\n%s", asComment("", comment))
		pn("func (c *%s) Media(r io.Reader, options ...googleapi.MediaOption) *%s {", callName, callName)
		// We check if the body arg, if any, has a content type and apply it here.
		// In practice, this only happens for the storage API today.
		// TODO(djd): check if we can cope with the developer setting the body's Content-Type field
		// after they've made this call.
		if ba := args.bodyArg(); ba != nil {
			if ba.schema.HasContentType() {
				pn("  if ct := c.%s.ContentType; ct != \"\" {", ba.goname)
				pn("   options = append([]googleapi.MediaOption{googleapi.ContentType(ct)}, options...)")
				pn("  }")
			}
		}
		pn(" opts := googleapi.ProcessMediaOptions(options)")
		pn(" chunkSize := opts.ChunkSize")
		pn(" if !opts.ForceEmptyContentType {")
		pn("  r, c.mediaType_ = gensupport.DetermineContentType(r, opts.ContentType)")
		pn(" }")
		pn(" c.media_, c.mediaBuffer_ = gensupport.PrepareUpload(r, chunkSize)")
		pn(" return c")
		pn("}")
		comment = "ResumableMedia specifies the media to upload in chunks and can be canceled with ctx. " +
			"\n\nDeprecated: use Media instead." +
			"\n\nAt most one of Media and ResumableMedia may be set. " +
			`mediaType identifies the MIME media type of the upload, such as "image/png". ` +
			`If mediaType is "", it will be auto-detected. ` +
			`The provided ctx will supersede any context previously provided to ` +
			`the Context method.`
		p("\n%s", asComment("", comment))
		pn("func (c *%s) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *%s {", callName, callName)
		pn(" c.ctx_ = ctx")
		pn(" rdr := gensupport.ReaderAtToReader(r, size)")
		pn(" rdr, c.mediaType_ = gensupport.DetermineContentType(rdr, mediaType)")
		pn(" c.mediaBuffer_ = gensupport.NewMediaBuffer(rdr, googleapi.DefaultUploadChunkSize)")
		pn(" c.media_ = nil")
		pn(" c.mediaSize_ = size")
		pn(" return c")
		pn("}")
		comment = "ProgressUpdater provides a callback function that will be called after every chunk. " +
			"It should be a low-latency function in order to not slow down the upload operation. " +
			"This should only be called when using ResumableMedia (as opposed to Media)."
		p("\n%s", asComment("", comment))
		pn("func (c *%s) ProgressUpdater(pu googleapi.ProgressUpdater) *%s {", callName, callName)
		pn(`c.progressUpdater_ = pu`)
		pn("return c")
		pn("}")
	}

	comment := "Fields allows partial responses to be retrieved. " +
		"See https://developers.google.com/gdata/docs/2.0/basics#PartialResponse " +
		"for more information."
	p("\n%s", asComment("", comment))
	pn("func (c *%s) Fields(s ...googleapi.Field) *%s {", callName, callName)
	pn(`c.urlParams_.Set("fields", googleapi.CombineFields(s))`)
	pn("return c")
	pn("}")
	if httpMethod == "GET" {
		// Note that non-GET responses are excluded from supporting If-None-Match.
		// See https://github.com/google/google-api-go-client/issues/107 for more info.
		comment := "IfNoneMatch sets the optional parameter which makes the operation fail if " +
			"the object's ETag matches the given value. This is useful for getting updates " +
			"only after the object has changed since the last request. " +
			"Use googleapi.IsNotModified to check whether the response error from Do " +
			"is the result of In-None-Match."
		p("\n%s", asComment("", comment))
		pn("func (c *%s) IfNoneMatch(entityTag string) *%s {", callName, callName)
		pn(" c.ifNoneMatch_ = entityTag")
		pn(" return c")
		pn("}")
	}

	doMethod := "Do method"
	if meth.supportsMediaDownload() {
		doMethod = "Do and Download methods"
	}
	commentFmtStr := "Context sets the context to be used in this call's %s. " +
		"Any pending HTTP request will be aborted if the provided context is canceled."
	comment = fmt.Sprintf(commentFmtStr, doMethod)
	p("\n%s", asComment("", comment))
	if meth.supportsMediaUpload() {
		comment = "This context will supersede any context previously provided to " +
			"the ResumableMedia method."
		p("%s", asComment("", comment))
	}
	pn("func (c *%s) Context(ctx context.Context) *%s {", callName, callName)
	pn(`c.ctx_ = ctx`)
	pn("return c")
	pn("}")

	comment = "Header returns an http.Header that can be modified by the caller to add " +
		"HTTP headers to the request."
	p("\n%s", asComment("", comment))
	pn("func (c *%s) Header() http.Header {", callName)
	pn(" if c.header_ == nil {")
	pn("  c.header_ = make(http.Header)")
	pn(" }")
	pn(" return c.header_")
	pn("}")

	pn("\nfunc (c *%s) doRequest(alt string) (*http.Response, error) {", callName)
	pn(`reqHeaders := make(http.Header)`)
	pn("for k, v := range c.header_ {")
	pn(" reqHeaders[k] = v")
	pn("}")
	pn(`reqHeaders.Set("User-Agent",c.s.userAgent())`)
	if httpMethod == "GET" {
		pn(`if c.ifNoneMatch_ != "" {`)
		pn(` reqHeaders.Set("If-None-Match",  c.ifNoneMatch_)`)
		pn("}")
	}
	pn("var body io.Reader = nil")
	if ba := args.bodyArg(); ba != nil && httpMethod != "GET" {
		style := "WithoutDataWrapper"
		if a.needsDataWrapper() {
			style = "WithDataWrapper"
		}
		pn("body, err := googleapi.%s.JSONReader(c.%s)", style, ba.goname)
		pn("if err != nil { return nil, err }")
		pn(`reqHeaders.Set("Content-Type", "application/json")`)
	}
	pn(`c.urlParams_.Set("alt", alt)`)

	pn("urls := googleapi.ResolveRelative(c.s.BasePath, %q)", meth.m.Path)
	if meth.supportsMediaUpload() {
		pn("if c.media_ != nil || c.mediaBuffer_ != nil{")
		// Hack guess, since we get a 404 otherwise:
		//pn("urls = googleapi.ResolveRelative(%q, %q)", a.apiBaseURL(), meth.mediaUploadPath())
		// Further hack.  Discovery doc is wrong?
		pn("  urls = strings.Replace(urls, %q, %q, 1)", "https://www.googleapis.com/", "https://www.googleapis.com/upload/")
		pn(`  protocol := "multipart"`)
		pn("  if c.mediaBuffer_ != nil {")
		pn(`   protocol = "resumable"`)
		pn("  }")
		pn(`  c.urlParams_.Set("uploadType", protocol)`)
		pn("}")

		pn("if body == nil {")
		pn(" body = new(bytes.Buffer)")
		pn(` reqHeaders.Set("Content-Type", "application/json")`)
		pn("}")
		pn(`if c.media_ != nil {`)
		pn(`  combined, ctype := gensupport.CombineBodyMedia(body, "application/json", c.media_, c.mediaType_)`)
		pn("  defer combined.Close()")
		pn(`  reqHeaders.Set("Content-Type", ctype)`)
		pn("  body = combined")
		pn("}")
		pn(`if c.mediaBuffer_ != nil && c.mediaType_ != ""{`)
		pn(` reqHeaders.Set("X-Upload-Content-Type", c.mediaType_)`)
		pn("}")
	}
	pn("urls += \"?\" + c.urlParams_.Encode()")
	pn("req, _ := http.NewRequest(%q, urls, body)", httpMethod)
	pn("req.Header = reqHeaders")

	// Replace param values after NewRequest to avoid reencoding them.
	// E.g. Cloud Storage API requires '%2F' in entity param to be kept, but url.Parse replaces it with '/'.
	argsForLocation := args.forLocation("path")
	if len(argsForLocation) > 0 {
		pn(`googleapi.Expand(req.URL, map[string]string{`)
		for _, arg := range argsForLocation {
			pn(`"%s": %s,`, arg.apiname, arg.exprAsString("c."))
		}
		pn(`})`)
	}

	pn("return gensupport.SendRequest(c.ctx_, c.s.client, req)")
	pn("}")

	if meth.supportsMediaDownload() {
		pn("\n// Download fetches the API endpoint's \"media\" value, instead of the normal")
		pn("// API response value. If the returned error is nil, the Response is guaranteed to")
		pn("// have a 2xx status code. Callers must close the Response.Body as usual.")
		pn("func (c *%s) Download(opts ...googleapi.CallOption) (*http.Response, error) {", callName)
		pn(`gensupport.SetOptions(c.urlParams_, opts...)`)
		pn(`res, err := c.doRequest("media")`)
		pn("if err != nil { return nil, err }")
		pn("if err := googleapi.CheckMediaResponse(res); err != nil {")
		pn("res.Body.Close()")
		pn("return nil, err")
		pn("}")
		pn("return res, nil")
		pn("}")
	}

	mapRetType := strings.HasPrefix(retTypeComma, "map[")
	pn("\n// Do executes the %q call.", meth.m.ID)
	if retTypeComma != "" && !mapRetType {
		commentFmtStr := "Exactly one of %v or error will be non-nil. " +
			"Any non-2xx status code is an error. " +
			"Response headers are in either %v.ServerResponse.Header " +
			"or (if a response was returned at all) in error.(*googleapi.Error).Header. " +
			"Use googleapi.IsNotModified to check whether the returned error was because " +
			"http.StatusNotModified was returned."
		comment := fmt.Sprintf(commentFmtStr, retType, retType)
		p("%s", asComment("", comment))
	}
	pn("func (c *%s) Do(opts ...googleapi.CallOption) (%serror) {", callName, retTypeComma)
	nilRet := ""
	if retTypeComma != "" {
		nilRet = "nil, "
	}
	pn(`gensupport.SetOptions(c.urlParams_, opts...)`)
	pn(`res, err := c.doRequest("json")`)

	if retTypeComma != "" && !mapRetType {
		pn("if res != nil && res.StatusCode == http.StatusNotModified {")
		pn(" if res.Body != nil { res.Body.Close() }")
		pn(" return nil, &googleapi.Error{")
		pn("  Code: res.StatusCode,")
		pn("  Header: res.Header,")
		pn(" }")
		pn("}")
	}
	pn("if err != nil { return %serr }", nilRet)
	pn("defer googleapi.CloseBody(res)")
	pn("if err := googleapi.CheckResponse(res); err != nil { return %serr }", nilRet)
	if meth.supportsMediaUpload() {
		pn("if c.mediaBuffer_ != nil {")
		pn(` loc := res.Header.Get("Location")`)
		pn(" rx := &gensupport.ResumableUpload{")
		pn("  Client:        c.s.client,")
		pn("  UserAgent:     c.s.userAgent(),")
		pn("  URI:           loc,")
		pn("  Media:         c.mediaBuffer_,")
		pn("  MediaType:     c.mediaType_,")
		pn("  Callback:      func(curr int64){")
		pn("   if c.progressUpdater_ != nil {")
		pn("    c.progressUpdater_(curr, c.mediaSize_)")
		pn("   }")
		pn("  },")
		pn(" }")
		pn(" ctx := c.ctx_")
		pn(" if ctx == nil {")
		// TODO(mcgreevy): Require context when calling Media, or Do.
		pn("  ctx = context.TODO()")
		pn(" }")
		pn(" res, err = rx.Upload(ctx)")
		pn(" if err != nil { return %serr }", nilRet)
		pn(" defer res.Body.Close()")
		pn(" if err := googleapi.CheckResponse(res); err != nil { return %serr }", nilRet)
		pn("}")
	}
	if retTypeComma == "" {
		pn("return nil")
	} else {
		if mapRetType {
			pn("var ret %s", responseType(a, meth.m))
		} else {
			pn("ret := &%s{", responseTypeLiteral(a, meth.m))
			pn(" ServerResponse: googleapi.ServerResponse{")
			pn("  Header: res.Header,")
			pn("  HTTPStatusCode: res.StatusCode,")
			pn(" },")
			pn("}")
		}
		if a.needsDataWrapper() {
			pn("target := &struct {")
			pn("  Data %s `json:\"data\"`", responseType(a, meth.m))
			pn("}{ret}")
		} else {
			pn("target := &ret")
		}

		pn("if err := json.NewDecoder(res.Body).Decode(target); err != nil { return nil, err }")
		pn("return ret, nil")
	}

	bs, _ := json.MarshalIndent(meth.m.JSONMap, "\t// ", "  ")
	pn("// %s\n", string(bs))
	pn("}")

	if cname, rname, ok := meth.supportsPaging(); ok {
		// We can assume retType is non-empty.
		pn("")
		pn("// Pages invokes f for each page of results.")
		pn("// A non-nil error returned from f will halt the iteration.")
		pn("// The provided context supersedes any context provided to the Context method.")
		pn("func (c *%s) Pages(ctx context.Context, f func(%s) error) error {", callName, retType)
		pn(" c.ctx_ = ctx")
		pn(` defer c.%s(c.urlParams_.Get(%q)) // reset paging to original point`, cname, "pageToken")
		pn(" for {")
		pn("  x, err := c.Do()")
		pn("  if err != nil { return err }")
		pn("  if err := f(x); err != nil { return err }")
		pn(`  if x.%s == "" { return nil }`, rname)
		pn("  c.%s(x.%s)", cname, rname)
		pn(" }")
		pn("}")
	}
}

// A Field provides methods that describe the characteristics of a Param or Property.
type Field interface {
	Default() string
	Enum() ([]string, bool)
	EnumDescriptions() []string
	UnfortunateDefault() bool
}

type Param struct {
	method        *Method
	p             *disco.Parameter
	callFieldName string // empty means to use the default
}

func (p *Param) Default() string {
	return p.p.Default
}

func (p *Param) Enum() ([]string, bool) {
	if e := p.p.Enums; e != nil {
		return e, true
	}
	return nil, false
}

func (p *Param) EnumDescriptions() []string {
	return p.p.EnumDescriptions
}

func (p *Param) UnfortunateDefault() bool {
	// We do not do anything special for Params with unfortunate defaults.
	return false
}

func (p *Param) GoType() string {
	typ, format := p.p.Type, p.p.Format
	if typ == "string" && strings.Contains(format, "int") && p.p.Location != "query" {
		panic("unexpected int parameter encoded as string, not in query: " + p.p.Name)
	}
	t, ok := simpleTypeConvert(typ, format)
	if !ok {
		panic("failed to convert parameter type " + fmt.Sprintf("type=%q, format=%q", typ, format))
	}
	return t
}

// goCallFieldName returns the name of this parameter's field in a
// method's "Call" struct.
func (p *Param) goCallFieldName() string {
	if p.callFieldName != "" {
		return p.callFieldName
	}
	return validGoIdentifer(p.p.Name)
}

// APIMethods returns top-level ("API-level") methods. They don't have an associated resource.
func (a *API) APIMethods() []*Method {
	meths := []*Method{}
	for _, m := range a.doc.Methods {
		meths = append(meths, &Method{
			api: a,
			r:   nil, // to be explicit
			m:   m,
		})
	}
	return meths
}

func resolveRelative(basestr, relstr string) string {
	u, err := url.Parse(basestr)
	if err != nil {
		panicf("Error parsing base URL %q: %v", basestr, err)
	}
	rel, err := url.Parse(relstr)
	if err != nil {
		panicf("Error parsing relative URL %q: %v", relstr, err)
	}
	u = u.ResolveReference(rel)
	return u.String()
}

func (meth *Method) NewArguments() (args *arguments) {
	args = &arguments{
		method: meth,
		m:      make(map[string]*argument),
	}
	po := meth.m.ParameterOrder
	if len(po) > 0 {
		for _, pname := range po {
			arg := meth.NewArg(pname, meth.NamedParam(pname))
			args.AddArg(arg)
		}
	}
	if ro := meth.m.Request; ro != nil {
		args.AddArg(meth.NewBodyArg(ro))
	}
	return
}

func (meth *Method) NewBodyArg(m map[string]interface{}) *argument {
	reftype := jstr(m, "$ref")
	schem := meth.api.Schema(reftype)
	if schem == nil {
		panicf("unable to find schema for type %q", reftype)
	}
	return &argument{
		goname:   validGoIdentifer(strings.ToLower(reftype)),
		apiname:  "REQUEST",
		gotype:   "*" + schem.GoName(),
		apitype:  reftype,
		location: "body",
		schema:   schem,
	}
}

func (meth *Method) NewArg(apiname string, p *Param) *argument {
	apitype := p.p.Type
	des := p.p.Description
	goname := validGoIdentifer(apiname) // but might be changed later, if conflicts
	if strings.Contains(des, "identifier") && !strings.HasSuffix(strings.ToLower(goname), "id") {
		goname += "id" // yay
		p.callFieldName = goname
	}
	gotype := mustSimpleTypeConvert(apitype, p.p.Format)
	if p.p.Repeated {
		gotype = "[]" + gotype
	}
	return &argument{
		apiname:  apiname,
		apitype:  apitype,
		goname:   goname,
		gotype:   gotype,
		location: p.p.Location,
	}
}

type argument struct {
	method           *Method
	schema           *Schema // Set if location == "body".
	apiname, apitype string
	goname, gotype   string
	location         string // "path", "query", "body"
}

func (a *argument) String() string {
	return a.goname + " " + a.gotype
}

func (a *argument) exprAsString(prefix string) string {
	switch a.gotype {
	case "[]string":
		log.Printf("TODO(bradfitz): only including the first parameter in path query.")
		return prefix + a.goname + `[0]`
	case "string":
		return prefix + a.goname
	case "integer", "int64":
		return "strconv.FormatInt(" + prefix + a.goname + ", 10)"
	case "uint64":
		return "strconv.FormatUint(" + prefix + a.goname + ", 10)"
	}
	log.Panicf("unknown type: apitype=%q, gotype=%q", a.apitype, a.gotype)
	return ""
}

// arguments are the arguments that a method takes
type arguments struct {
	l      []*argument
	m      map[string]*argument
	method *Method
}

func (args *arguments) forLocation(loc string) []*argument {
	matches := make([]*argument, 0)
	for _, arg := range args.l {
		if arg.location == loc {
			matches = append(matches, arg)
		}
	}
	return matches
}

func (args *arguments) bodyArg() *argument {
	for _, arg := range args.l {
		if arg.location == "body" {
			return arg
		}
	}
	return nil
}

func (args *arguments) AddArg(arg *argument) {
	n := 1
	oname := arg.goname
	for {
		_, present := args.m[arg.goname]
		if !present {
			args.m[arg.goname] = arg
			args.l = append(args.l, arg)
			return
		}
		n++
		arg.goname = fmt.Sprintf("%s%d", oname, n)
	}
}

func (a *arguments) String() string {
	var buf bytes.Buffer
	for i, arg := range a.l {
		if i != 0 {
			buf.Write([]byte(", "))
		}
		buf.Write([]byte(arg.String()))
	}
	return buf.String()
}

var urlRE = regexp.MustCompile(`^http\S+$`)

func asComment(pfx, c string) string {
	var buf bytes.Buffer
	const maxLen = 70
	r := strings.NewReplacer(
		"\n", "\n"+pfx+"// ",
		"`\"", `"`,
		"\"`", `"`,
	)
	for len(c) > 0 {
		line := c
		if len(line) < maxLen {
			fmt.Fprintf(&buf, "%s// %s\n", pfx, r.Replace(line))
			break
		}
		// Don't break URLs.
		if !urlRE.MatchString(line[:maxLen]) {
			line = line[:maxLen]
		}
		si := strings.LastIndex(line, " ")
		if nl := strings.Index(line, "\n"); nl != -1 && nl < si {
			si = nl
		}
		if si != -1 {
			line = line[:si]
		}
		fmt.Fprintf(&buf, "%s// %s\n", pfx, r.Replace(line))
		c = c[len(line):]
		if si != -1 {
			c = c[1:]
		}
	}
	return buf.String()
}

func simpleTypeConvert(apiType, format string) (gotype string, ok bool) {
	// From http://tools.ietf.org/html/draft-zyp-json-schema-03#section-5.1
	switch apiType {
	case "boolean":
		gotype = "bool"
	case "string":
		gotype = "string"
		switch format {
		case "int64", "uint64", "int32", "uint32":
			gotype = format
		}
	case "number":
		gotype = "float64"
	case "integer":
		gotype = "int64"
	case "any":
		gotype = "interface{}"
	}
	return gotype, gotype != ""
}

func mustSimpleTypeConvert(apiType, format string) string {
	if gotype, ok := simpleTypeConvert(apiType, format); ok {
		return gotype
	}
	panic(fmt.Sprintf("failed to simpleTypeConvert(%q, %q)", apiType, format))
}

func responseType(api *API, m *disco.Method) string {
	ref := m.Response.Ref
	if ref != "" {
		if s := api.schemas[ref]; s != nil {
			return s.GoReturnType()
		}
		return "*" + ref
	}
	return ""
}

// Strips the leading '*' from a type name so that it can be used to create a literal.
func responseTypeLiteral(api *API, m *disco.Method) string {
	v := responseType(api, m)
	if strings.HasPrefix(v, "*") {
		return v[1:]
	}
	return v
}

// initialCap returns the identifier with a leading capital letter.
// it also maps "foo-bar" to "FooBar".
func initialCap(ident string) string {
	if ident == "" {
		panic("blank identifier")
	}
	return depunct(ident, true)
}

func validGoIdentifer(ident string) string {
	id := depunct(ident, false)
	switch id {
	case "break", "default", "func", "interface", "select",
		"case", "defer", "go", "map", "struct",
		"chan", "else", "goto", "package", "switch",
		"const", "fallthrough", "if", "range", "type",
		"continue", "for", "import", "return", "var":
		return id + "_"
	}
	return id
}

// depunct removes '-', '.', '$', '/', '_' from identifers, making the
// following character uppercase. Multiple '_' are preserved.
func depunct(ident string, needCap bool) string {
	var buf bytes.Buffer
	preserve_ := false
	for i, c := range ident {
		if c == '_' {
			if preserve_ || strings.HasPrefix(ident[i:], "__") {
				preserve_ = true
			} else {
				needCap = true
				continue
			}
		} else {
			preserve_ = false
		}
		if c == '-' || c == '.' || c == '$' || c == '/' {
			needCap = true
			continue
		}
		if needCap {
			c = unicode.ToUpper(c)
			needCap = false
		}
		buf.WriteByte(byte(c))
	}
	return buf.String()

}

func prettyJSON(m map[string]interface{}) string {
	bs, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Sprintf("[JSON error %v on %#v]", err, m)
	}
	return string(bs)
}

func jstr(m map[string]interface{}, key string) string {
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

func sortedKeys(m interface{}) (keys []string) {
	// TODO(jba): get rid of this type switch when there is no more JSON,
	// hence no more map[string]interface{}.
	switch m := m.(type) {
	case map[string]interface{}:
		keys = keysMI(m)
	case map[string]*disco.Schema:
		keys = keysMS(m)
	default:
		panicf("bad map type %T", m)
	}
	sort.Strings(keys)
	return
}

func keysMI(m map[string]interface{}) (keys []string) {
	for key := range m {
		keys = append(keys, key)
	}
	return
}

func keysMS(m map[string]*disco.Schema) (keys []string) {
	for key := range m {
		keys = append(keys, key)
	}
	return
}

func addFieldValueComments(p func(format string, args ...interface{}), field Field, indent string, blankLine bool) {
	var lines []string

	if enum, ok := field.Enum(); ok {
		desc := field.EnumDescriptions()
		lines = append(lines, asComment(indent, "Possible values:"))
		defval := field.Default()
		for i, v := range enum {
			more := ""
			if v == defval {
				more = " (default)"
			}
			if len(desc) > i && desc[i] != "" {
				more = more + " - " + desc[i]
			}
			lines = append(lines, asComment(indent, `  "`+v+`"`+more))
		}
	} else if field.UnfortunateDefault() {
		lines = append(lines, asComment("\t", fmt.Sprintf("Default: %s", field.Default())))
	}
	if blankLine && len(lines) > 0 {
		p(indent + "//\n")
	}
	for _, l := range lines {
		p("%s", l)
	}
}
