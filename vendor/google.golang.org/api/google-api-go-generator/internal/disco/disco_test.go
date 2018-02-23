// Copyright 2016 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package disco

import (
	"io/ioutil"
	"reflect"
	"testing"
)

var stringSchema = &Schema{
	Type: "string",
	Kind: SimpleKind,
}

func TestDocument(t *testing.T) {
	bytes, err := ioutil.ReadFile("testdata/test-api.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := NewDocument(bytes)
	if err != nil {
		t.Fatal(err)
	}
	want := &Document{
		ID:          "storage:v1",
		Name:        "storage",
		Version:     "v1",
		Title:       "Cloud Storage JSON API",
		RootURL:     "https://www.googleapis.com/",
		ServicePath: "storage/v1/",
		BasePath:    "/storage/v1/",
		Auth: Auth{
			OAuth2Scopes: []Scope{
				{"https://www.googleapis.com/auth/cloud-platform",
					"View and manage your data across Google Cloud Platform services"},
				{"https://www.googleapis.com/auth/cloud-platform.read-only",
					"View your data across Google Cloud Platform services"},
				{"https://www.googleapis.com/auth/devstorage.full_control",
					"Manage your data and permissions in Google Cloud Storage"},
				{"https://www.googleapis.com/auth/devstorage.read_only",
					"View your data in Google Cloud Storage"},
				{"https://www.googleapis.com/auth/devstorage.read_write",
					"Manage your data in Google Cloud Storage"},
			},
		},
		Features: []string{"dataWrapper"},
		Schemas: map[string]*Schema{
			"Bucket": &Schema{
				Name:        "Bucket",
				ID:          "Bucket",
				Type:        "object",
				Description: "A bucket.",
				Kind:        StructKind,
				Properties: map[string]*Schema{
					"id": stringSchema,
					"kind": &Schema{
						Type:    "string",
						Kind:    SimpleKind,
						Default: "storage#bucket",
					},
					"cors": &Schema{
						Type: "array",
						Kind: ArrayKind,
						ItemSchema: &Schema{
							Type: "object",
							Kind: StructKind,
							Properties: map[string]*Schema{
								"maxAgeSeconds": &Schema{
									Type:   "integer",
									Format: "int32",
									Kind:   SimpleKind,
								},
								"method": &Schema{
									Type:       "array",
									Kind:       ArrayKind,
									ItemSchema: stringSchema,
								},
							},
						},
					},
				},
			},
			"Buckets": &Schema{
				ID:   "Buckets",
				Name: "Buckets",
				Type: "object",
				Kind: StructKind,
				Properties: map[string]*Schema{
					"items": &Schema{
						Type: "array",
						Kind: ArrayKind,
						ItemSchema: &Schema{
							Kind:      ReferenceKind,
							Ref:       "Bucket",
							RefSchema: nil,
						},
					},
				},
			},
			"VariantExample": &Schema{
				ID:   "VariantExample",
				Name: "VariantExample",
				Type: "object",
				Kind: StructKind,
				Variant: &Variant{
					Discriminant: "type",
					Map: []*VariantMapItem{
						{TypeValue: "Bucket", Ref: "Bucket"},
						{TypeValue: "Buckets", Ref: "Buckets"},
					},
				},
			},
		},
		Methods: MethodList{
			&Method{
				Name:       "getCertForOpenIdConnect",
				ID:         "oauth2.getCertForOpenIdConnect",
				Path:       "oauth2/v1/certs",
				HTTPMethod: "GET",
				Response: struct {
					Ref string `json:"$ref"`
				}{"X509"},
			},
		},
		Resources: ResourceList{
			&Resource{
				Name:     "buckets",
				FullName: ".buckets",
				Methods: MethodList{
					&Method{
						Name:        "get",
						ID:          "storage.buckets.get",
						Path:        "b/{bucket}",
						HTTPMethod:  "GET",
						Description: "d",
						Parameters: ParameterList{
							&Parameter{
								Name: "bucket",
								Schema: Schema{
									Type: "string",
								},
								Required: true,
								Location: "path",
							},
							&Parameter{
								Name: "ifMetagenerationMatch",
								Schema: Schema{
									Type:   "string",
									Format: "int64",
								},
								Location: "query",
							},
							&Parameter{
								Name: "projection",
								Schema: Schema{
									Type:  "string",
									Enums: []string{"full", "noAcl"},
									EnumDescriptions: []string{
										"Include all properties.",
										"Omit owner, acl and defaultObjectAcl properties.",
									},
								},
								Location: "query",
							},
						},
						ParameterOrder: []string{"bucket"},
						Response: struct {
							Ref string `json:"$ref"`
						}{"Bucket"},
						Scopes: []string{
							"https://www.googleapis.com/auth/cloud-platform",
							"https://www.googleapis.com/auth/cloud-platform.read-only",
							"https://www.googleapis.com/auth/devstorage.full_control",
							"https://www.googleapis.com/auth/devstorage.read_only",
							"https://www.googleapis.com/auth/devstorage.read_write",
						},
						SupportsMediaDownload: true,
						MediaUpload: &MediaUpload{
							Accept:  []string{"application/octet-stream"},
							MaxSize: "1GB",
							Protocols: map[string]Protocol{
								"simple": Protocol{
									Multipart: true,
									Path:      "/upload/customDataSources/{customDataSourceId}/uploads",
								},
								"resumable": Protocol{
									Multipart: true,
									Path:      "/resumable/upload/customDataSources/{customDataSourceId}/uploads",
								},
							},
						},
					},
				},
			},
		},
	}
	// Resolve schema references.
	want.Schemas["Buckets"].Properties["items"].ItemSchema.RefSchema = want.Schemas["Bucket"]
	for k, gs := range got.Schemas {
		ws := want.Schemas[k]
		if !reflect.DeepEqual(gs, ws) {
			t.Fatalf("schema %s: got\n%+v\nwant\n%+v", k, gs, ws)
		}
	}
	if len(got.Schemas) != len(want.Schemas) {
		t.Errorf("want %d schemas, got %d", len(got.Schemas), len(want.Schemas))
	}
	compareMethodLists(t, got.Methods, want.Methods)
	for i, gr := range got.Resources {
		wr := want.Resources[i]
		compareMethodLists(t, gr.Methods, wr.Methods)
		if !reflect.DeepEqual(gr, wr) {
			t.Fatalf("resource %d: got\n%+v\nwant\n%+v", i, gr, wr)
		}
	}
	if len(got.Resources) != len(want.Resources) {
		t.Errorf("want %d resources, got %d", len(got.Resources), len(want.Resources))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got\n%+v\nwant\n%+v", got, want)
	}
}

func compareMethodLists(t *testing.T, got, want MethodList) {
	if len(got) != len(want) {
		t.Fatalf("got %d methods, want %d", len(got), len(want))
	}
	for i, gm := range got {
		gm.JSONMap = nil // don't compare the raw JSON
		wm := want[i]
		if !reflect.DeepEqual(gm, wm) {
			t.Errorf("#%d: got\n%+v\nwant\n%+v", i, gm, wm)
		}
	}
}

func TestDocumentErrors(t *testing.T) {
	for _, in := range []string{
		`{"name": "X"`, // malformed JSON
		`{"id": 3}`,    // ID is an int instead of a string
		`{"auth": "oauth2": { "scopes": "string" }}`, // wrong auth structure
	} {
		_, err := NewDocument([]byte(in))
		if err == nil {
			t.Errorf("%s: got nil, want error", in)
		}
	}
}

func TestSchemaErrors(t *testing.T) {
	for _, s := range []*Schema{
		{Type: "array"},                         // missing item schema
		{Type: "string", ItemSchema: &Schema{}}, // items w/o array
		{Type: "moose"},                         // bad kind
		{Ref: "Thing"},                          // unresolved reference
	} {
		if err := s.init(nil); err == nil {
			t.Errorf("%+v: got nil, want error", s)
		}
	}
}
