// Package fusiontables provides access to the Fusion Tables API.
//
// See https://developers.google.com/fusiontables
//
// Usage example:
//
//   import "google.golang.org/api/fusiontables/v1"
//   ...
//   fusiontablesService, err := fusiontables.New(oauthHttpClient)
package fusiontables // import "google.golang.org/api/fusiontables/v1"

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	context "golang.org/x/net/context"
	ctxhttp "golang.org/x/net/context/ctxhttp"
	gensupport "google.golang.org/api/gensupport"
	googleapi "google.golang.org/api/googleapi"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Always reference these packages, just in case the auto-generated code
// below doesn't.
var _ = bytes.NewBuffer
var _ = strconv.Itoa
var _ = fmt.Sprintf
var _ = json.NewDecoder
var _ = io.Copy
var _ = url.Parse
var _ = gensupport.MarshalJSON
var _ = googleapi.Version
var _ = errors.New
var _ = strings.Replace
var _ = context.Canceled
var _ = ctxhttp.Do

const apiId = "fusiontables:v1"
const apiName = "fusiontables"
const apiVersion = "v1"
const basePath = "https://www.googleapis.com/fusiontables/v1/"

// OAuth2 scopes used by this API.
const (
	// Manage your Fusion Tables
	FusiontablesScope = "https://www.googleapis.com/auth/fusiontables"

	// View your Fusion Tables
	FusiontablesReadonlyScope = "https://www.googleapis.com/auth/fusiontables.readonly"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Column = NewColumnService(s)
	s.Query = NewQueryService(s)
	s.Style = NewStyleService(s)
	s.Table = NewTableService(s)
	s.Task = NewTaskService(s)
	s.Template = NewTemplateService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Column *ColumnService

	Query *QueryService

	Style *StyleService

	Table *TableService

	Task *TaskService

	Template *TemplateService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewColumnService(s *Service) *ColumnService {
	rs := &ColumnService{s: s}
	return rs
}

type ColumnService struct {
	s *Service
}

func NewQueryService(s *Service) *QueryService {
	rs := &QueryService{s: s}
	return rs
}

type QueryService struct {
	s *Service
}

func NewStyleService(s *Service) *StyleService {
	rs := &StyleService{s: s}
	return rs
}

type StyleService struct {
	s *Service
}

func NewTableService(s *Service) *TableService {
	rs := &TableService{s: s}
	return rs
}

type TableService struct {
	s *Service
}

func NewTaskService(s *Service) *TaskService {
	rs := &TaskService{s: s}
	return rs
}

type TaskService struct {
	s *Service
}

func NewTemplateService(s *Service) *TemplateService {
	rs := &TemplateService{s: s}
	return rs
}

type TemplateService struct {
	s *Service
}

// Bucket: Specifies the minimum and maximum values, the color, opacity,
// icon and weight of a bucket within a StyleSetting.
type Bucket struct {
	// Color: Color of line or the interior of a polygon in #RRGGBB format.
	Color string `json:"color,omitempty"`

	// Icon: Icon name used for a point.
	Icon string `json:"icon,omitempty"`

	// Max: Maximum value in the selected column for a row to be styled
	// according to the bucket color, opacity, icon, or weight.
	Max float64 `json:"max,omitempty"`

	// Min: Minimum value in the selected column for a row to be styled
	// according to the bucket color, opacity, icon, or weight.
	Min float64 `json:"min,omitempty"`

	// Opacity: Opacity of the color: 0.0 (transparent) to 1.0 (opaque).
	Opacity float64 `json:"opacity,omitempty"`

	// Weight: Width of a line (in pixels).
	Weight int64 `json:"weight,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Color") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Color") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Bucket) MarshalJSON() ([]byte, error) {
	type noMethod Bucket
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Column: Specifies the id, name and type of a column in a table.
type Column struct {
	// BaseColumn: Optional identifier of the base column. If present, this
	// column is derived from the specified base column.
	BaseColumn *ColumnBaseColumn `json:"baseColumn,omitempty"`

	// ColumnId: Identifier for the column.
	ColumnId int64 `json:"columnId,omitempty"`

	// Description: Optional column description.
	Description string `json:"description,omitempty"`

	// GraphPredicate: Optional column predicate. Used to map table to graph
	// data model (subject,predicate,object) See
	// http://www.w3.org/TR/2014/REC-rdf11-concepts-20140225/#data-model
	GraphPredicate string `json:"graph_predicate,omitempty"`

	// Kind: Type name: a template for an individual column.
	Kind string `json:"kind,omitempty"`

	// Name: Required name of the column.
	Name string `json:"name,omitempty"`

	// Type: Required type of the column.
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "BaseColumn") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BaseColumn") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Column) MarshalJSON() ([]byte, error) {
	type noMethod Column
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ColumnBaseColumn: Optional identifier of the base column. If present,
// this column is derived from the specified base column.
type ColumnBaseColumn struct {
	// ColumnId: The id of the column in the base table from which this
	// column is derived.
	ColumnId int64 `json:"columnId,omitempty"`

	// TableIndex: Offset to the entry in the list of base tables in the
	// table definition.
	TableIndex int64 `json:"tableIndex,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ColumnId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ColumnId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ColumnBaseColumn) MarshalJSON() ([]byte, error) {
	type noMethod ColumnBaseColumn
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ColumnList: Represents a list of columns in a table.
type ColumnList struct {
	// Items: List of all requested columns.
	Items []*Column `json:"items,omitempty"`

	// Kind: Type name: a list of all columns.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result. No
	// token is displayed if there are no more pages left.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalItems: Total number of columns for the table.
	TotalItems int64 `json:"totalItems,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ColumnList) MarshalJSON() ([]byte, error) {
	type noMethod ColumnList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Geometry: Represents a Geometry object.
type Geometry struct {
	// Geometries: The list of geometries in this geometry collection.
	Geometries []interface{} `json:"geometries,omitempty"`

	Geometry interface{} `json:"geometry,omitempty"`

	// Type: Type: A collection of geometries.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Geometries") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Geometries") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Geometry) MarshalJSON() ([]byte, error) {
	type noMethod Geometry
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Import: Represents an import request.
type Import struct {
	// Kind: Type name: a template for an import request.
	Kind string `json:"kind,omitempty"`

	// NumRowsReceived: The number of rows received from the import request.
	NumRowsReceived int64 `json:"numRowsReceived,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Kind") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Kind") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Import) MarshalJSON() ([]byte, error) {
	type noMethod Import
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Line: Represents a line geometry.
type Line struct {
	// Coordinates: The coordinates that define the line.
	Coordinates [][]float64 `json:"coordinates,omitempty"`

	// Type: Type: A line geometry.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Coordinates") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Coordinates") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Line) MarshalJSON() ([]byte, error) {
	type noMethod Line
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LineStyle: Represents a LineStyle within a StyleSetting
type LineStyle struct {
	// StrokeColor: Color of the line in #RRGGBB format.
	StrokeColor string `json:"strokeColor,omitempty"`

	// StrokeColorStyler: Column-value, gradient or buckets styler that is
	// used to determine the line color and opacity.
	StrokeColorStyler *StyleFunction `json:"strokeColorStyler,omitempty"`

	// StrokeOpacity: Opacity of the line : 0.0 (transparent) to 1.0
	// (opaque).
	StrokeOpacity float64 `json:"strokeOpacity,omitempty"`

	// StrokeWeight: Width of the line in pixels.
	StrokeWeight int64 `json:"strokeWeight,omitempty"`

	// StrokeWeightStyler: Column-value or bucket styler that is used to
	// determine the width of the line.
	StrokeWeightStyler *StyleFunction `json:"strokeWeightStyler,omitempty"`

	// ForceSendFields is a list of field names (e.g. "StrokeColor") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "StrokeColor") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LineStyle) MarshalJSON() ([]byte, error) {
	type noMethod LineStyle
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Point: Represents a point object.
type Point struct {
	// Coordinates: The coordinates that define the point.
	Coordinates []float64 `json:"coordinates,omitempty"`

	// Type: Point: A point geometry.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Coordinates") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Coordinates") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Point) MarshalJSON() ([]byte, error) {
	type noMethod Point
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PointStyle: Represents a PointStyle within a StyleSetting
type PointStyle struct {
	// IconName: Name of the icon. Use values defined in
	// http://www.google.com/fusiontables/DataSource?dsrcid=308519
	IconName string `json:"iconName,omitempty"`

	// IconStyler: Column or a bucket value from which the icon name is to
	// be determined.
	IconStyler *StyleFunction `json:"iconStyler,omitempty"`

	// ForceSendFields is a list of field names (e.g. "IconName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "IconName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PointStyle) MarshalJSON() ([]byte, error) {
	type noMethod PointStyle
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Polygon: Represents a polygon object.
type Polygon struct {
	// Coordinates: The coordinates that define the polygon.
	Coordinates [][][]float64 `json:"coordinates,omitempty"`

	// Type: Type: A polygon geometry.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Coordinates") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Coordinates") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Polygon) MarshalJSON() ([]byte, error) {
	type noMethod Polygon
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PolygonStyle: Represents a PolygonStyle within a StyleSetting
type PolygonStyle struct {
	// FillColor: Color of the interior of the polygon in #RRGGBB format.
	FillColor string `json:"fillColor,omitempty"`

	// FillColorStyler: Column-value, gradient, or bucket styler that is
	// used to determine the interior color and opacity of the polygon.
	FillColorStyler *StyleFunction `json:"fillColorStyler,omitempty"`

	// FillOpacity: Opacity of the interior of the polygon: 0.0
	// (transparent) to 1.0 (opaque).
	FillOpacity float64 `json:"fillOpacity,omitempty"`

	// StrokeColor: Color of the polygon border in #RRGGBB format.
	StrokeColor string `json:"strokeColor,omitempty"`

	// StrokeColorStyler: Column-value, gradient or buckets styler that is
	// used to determine the border color and opacity.
	StrokeColorStyler *StyleFunction `json:"strokeColorStyler,omitempty"`

	// StrokeOpacity: Opacity of the polygon border: 0.0 (transparent) to
	// 1.0 (opaque).
	StrokeOpacity float64 `json:"strokeOpacity,omitempty"`

	// StrokeWeight: Width of the polyon border in pixels.
	StrokeWeight int64 `json:"strokeWeight,omitempty"`

	// StrokeWeightStyler: Column-value or bucket styler that is used to
	// determine the width of the polygon border.
	StrokeWeightStyler *StyleFunction `json:"strokeWeightStyler,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FillColor") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FillColor") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PolygonStyle) MarshalJSON() ([]byte, error) {
	type noMethod PolygonStyle
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Sqlresponse: Represents a response to an sql statement.
type Sqlresponse struct {
	// Columns: Columns in the table.
	Columns []string `json:"columns,omitempty"`

	// Kind: Type name: a template for an individual table.
	Kind string `json:"kind,omitempty"`

	// Rows: The rows in the table. For each cell we print out whatever cell
	// value (e.g., numeric, string) exists. Thus it is important that each
	// cell contains only one value.
	Rows [][]interface{} `json:"rows,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Columns") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Columns") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Sqlresponse) MarshalJSON() ([]byte, error) {
	type noMethod Sqlresponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StyleFunction: Represents a StyleFunction within a StyleSetting
type StyleFunction struct {
	// Buckets: Bucket function that assigns a style based on the range a
	// column value falls into.
	Buckets []*Bucket `json:"buckets,omitempty"`

	// ColumnName: Name of the column whose value is used in the style.
	ColumnName string `json:"columnName,omitempty"`

	// Gradient: Gradient function that interpolates a range of colors based
	// on column value.
	Gradient *StyleFunctionGradient `json:"gradient,omitempty"`

	// Kind: Stylers can be one of three kinds: "fusiontables#fromColumn" if
	// the column value is to be used as is, i.e., the column values can
	// have colors in #RRGGBBAA format or integer line widths or icon names;
	// "fusiontables#gradient" if the styling of the row is to be based on
	// applying the gradient function on the column value; or
	// "fusiontables#buckets" if the styling is to based on the bucket into
	// which the the column value falls.
	Kind string `json:"kind,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Buckets") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Buckets") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StyleFunction) MarshalJSON() ([]byte, error) {
	type noMethod StyleFunction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StyleFunctionGradient: Gradient function that interpolates a range of
// colors based on column value.
type StyleFunctionGradient struct {
	// Colors: Array with two or more colors.
	Colors []*StyleFunctionGradientColors `json:"colors,omitempty"`

	// Max: Higher-end of the interpolation range: rows with this value will
	// be assigned to colors[n-1].
	Max float64 `json:"max,omitempty"`

	// Min: Lower-end of the interpolation range: rows with this value will
	// be assigned to colors[0].
	Min float64 `json:"min,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Colors") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Colors") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StyleFunctionGradient) MarshalJSON() ([]byte, error) {
	type noMethod StyleFunctionGradient
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type StyleFunctionGradientColors struct {
	// Color: Color in #RRGGBB format.
	Color string `json:"color,omitempty"`

	// Opacity: Opacity of the color: 0.0 (transparent) to 1.0 (opaque).
	Opacity float64 `json:"opacity,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Color") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Color") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StyleFunctionGradientColors) MarshalJSON() ([]byte, error) {
	type noMethod StyleFunctionGradientColors
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StyleSetting: Represents a complete StyleSettings object. The primary
// key is a combination of the tableId and a styleId.
type StyleSetting struct {
	// Kind: Type name: an individual style setting. A StyleSetting contains
	// the style defintions for points, lines, and polygons in a table.
	// Since a table can have any one or all of them, a style definition can
	// have point, line and polygon style definitions.
	Kind string `json:"kind,omitempty"`

	// MarkerOptions: Style definition for points in the table.
	MarkerOptions *PointStyle `json:"markerOptions,omitempty"`

	// Name: Optional name for the style setting.
	Name string `json:"name,omitempty"`

	// PolygonOptions: Style definition for polygons in the table.
	PolygonOptions *PolygonStyle `json:"polygonOptions,omitempty"`

	// PolylineOptions: Style definition for lines in the table.
	PolylineOptions *LineStyle `json:"polylineOptions,omitempty"`

	// StyleId: Identifier for the style setting (unique only within
	// tables).
	StyleId int64 `json:"styleId,omitempty"`

	// TableId: Identifier for the table.
	TableId string `json:"tableId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Kind") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Kind") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StyleSetting) MarshalJSON() ([]byte, error) {
	type noMethod StyleSetting
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StyleSettingList: Represents a list of styles for a given table.
type StyleSettingList struct {
	// Items: All requested style settings.
	Items []*StyleSetting `json:"items,omitempty"`

	// Kind: Type name: in this case, a list of style settings.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result. No
	// token is displayed if there are no more pages left.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalItems: Total number of styles for the table.
	TotalItems int64 `json:"totalItems,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StyleSettingList) MarshalJSON() ([]byte, error) {
	type noMethod StyleSettingList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Table: Represents a table. Specifies the name, whether it is
// exportable, description, attribution, and attribution link.
type Table struct {
	// Attribution: Optional attribution assigned to the table.
	Attribution string `json:"attribution,omitempty"`

	// AttributionLink: Optional link for attribution.
	AttributionLink string `json:"attributionLink,omitempty"`

	// BaseTableIds: Optional base table identifier if this table is a view
	// or merged table.
	BaseTableIds []string `json:"baseTableIds,omitempty"`

	// Columns: Columns in the table.
	Columns []*Column `json:"columns,omitempty"`

	// Description: Optional description assigned to the table.
	Description string `json:"description,omitempty"`

	// IsExportable: Variable for whether table is exportable.
	IsExportable bool `json:"isExportable,omitempty"`

	// Kind: Type name: a template for an individual table.
	Kind string `json:"kind,omitempty"`

	// Name: Name assigned to a table.
	Name string `json:"name,omitempty"`

	// Sql: Optional sql that encodes the table definition for derived
	// tables.
	Sql string `json:"sql,omitempty"`

	// TableId: Encrypted unique alphanumeric identifier for the table.
	TableId string `json:"tableId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Attribution") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Attribution") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Table) MarshalJSON() ([]byte, error) {
	type noMethod Table
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableList: Represents a list of tables.
type TableList struct {
	// Items: List of all requested tables.
	Items []*Table `json:"items,omitempty"`

	// Kind: Type name: a list of all tables.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result. No
	// token is displayed if there are no more pages left.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TableList) MarshalJSON() ([]byte, error) {
	type noMethod TableList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Task: Specifies the identifier, name, and type of a task in a table.
type Task struct {
	// Kind: Type of the resource. This is always "fusiontables#task".
	Kind string `json:"kind,omitempty"`

	// Progress: An indication of task progress.
	Progress string `json:"progress,omitempty"`

	// Started: false while the table is busy with some other task. true if
	// this background task is currently running.
	Started bool `json:"started,omitempty"`

	// TaskId: Identifier for the task.
	TaskId int64 `json:"taskId,omitempty,string"`

	// Type: Type of background task. One of  DELETE_ROWS Deletes one or
	// more rows from the table. ADD_ROWS "Adds one or more rows to a table.
	// Includes importing data into a new table and importing more rows into
	// an existing table. ADD_COLUMN Adds a new column to the table.
	// CHANGE_TYPE Changes the type of a column.
	Type string `json:"type,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Kind") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Kind") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Task) MarshalJSON() ([]byte, error) {
	type noMethod Task
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TaskList: Represents a list of tasks for a table.
type TaskList struct {
	// Items: List of all requested tasks.
	Items []*Task `json:"items,omitempty"`

	// Kind: Type of the resource. This is always "fusiontables#taskList".
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result. No
	// token is displayed if there are no more pages left.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalItems: Total number of tasks for the table.
	TotalItems int64 `json:"totalItems,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TaskList) MarshalJSON() ([]byte, error) {
	type noMethod TaskList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Template: Represents the contents of InfoWindow templates.
type Template struct {
	// AutomaticColumnNames: List of columns from which the template is to
	// be automatically constructed. Only one of body or automaticColumns
	// can be specified.
	AutomaticColumnNames []string `json:"automaticColumnNames,omitempty"`

	// Body: Body of the template. It contains HTML with {column_name} to
	// insert values from a particular column. The body is sanitized to
	// remove certain tags, e.g., script. Only one of body or
	// automaticColumns can be specified.
	Body string `json:"body,omitempty"`

	// Kind: Type name: a template for the info window contents. The
	// template can either include an HTML body or a list of columns from
	// which the template is computed automatically.
	Kind string `json:"kind,omitempty"`

	// Name: Optional name assigned to a template.
	Name string `json:"name,omitempty"`

	// TableId: Identifier for the table for which the template is defined.
	TableId string `json:"tableId,omitempty"`

	// TemplateId: Identifier for the template, unique within the context of
	// a particular table.
	TemplateId int64 `json:"templateId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g.
	// "AutomaticColumnNames") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AutomaticColumnNames") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Template) MarshalJSON() ([]byte, error) {
	type noMethod Template
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TemplateList: Represents a list of templates for a given table.
type TemplateList struct {
	// Items: List of all requested templates.
	Items []*Template `json:"items,omitempty"`

	// Kind: Type name: a list of all templates.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token used to access the next page of this result. No
	// token is displayed if there are no more pages left.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalItems: Total number of templates for the table.
	TotalItems int64 `json:"totalItems,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TemplateList) MarshalJSON() ([]byte, error) {
	type noMethod TemplateList
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "fusiontables.column.delete":

type ColumnDeleteCall struct {
	s          *Service
	tableId    string
	columnId   string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes the column.
func (r *ColumnService) Delete(tableId string, columnId string) *ColumnDeleteCall {
	c := &ColumnDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.columnId = columnId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ColumnDeleteCall) Fields(s ...googleapi.Field) *ColumnDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ColumnDeleteCall) Context(ctx context.Context) *ColumnDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ColumnDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ColumnDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/columns/{columnId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":  c.tableId,
		"columnId": c.columnId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.column.delete" call.
func (c *ColumnDeleteCall) Do(opts ...googleapi.CallOption) error {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return err
	}
	return nil
	// {
	//   "description": "Deletes the column.",
	//   "httpMethod": "DELETE",
	//   "id": "fusiontables.column.delete",
	//   "parameterOrder": [
	//     "tableId",
	//     "columnId"
	//   ],
	//   "parameters": {
	//     "columnId": {
	//       "description": "Name or identifier for the column being deleted.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "tableId": {
	//       "description": "Table from which the column is being deleted.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/columns/{columnId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.column.get":

type ColumnGetCall struct {
	s            *Service
	tableId      string
	columnId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a specific column by its id.
func (r *ColumnService) Get(tableId string, columnId string) *ColumnGetCall {
	c := &ColumnGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.columnId = columnId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ColumnGetCall) Fields(s ...googleapi.Field) *ColumnGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ColumnGetCall) IfNoneMatch(entityTag string) *ColumnGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ColumnGetCall) Context(ctx context.Context) *ColumnGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ColumnGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ColumnGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/columns/{columnId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":  c.tableId,
		"columnId": c.columnId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.column.get" call.
// Exactly one of *Column or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Column.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ColumnGetCall) Do(opts ...googleapi.CallOption) (*Column, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Column{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a specific column by its id.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.column.get",
	//   "parameterOrder": [
	//     "tableId",
	//     "columnId"
	//   ],
	//   "parameters": {
	//     "columnId": {
	//       "description": "Name or identifier for the column that is being requested.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "tableId": {
	//       "description": "Table to which the column belongs.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/columns/{columnId}",
	//   "response": {
	//     "$ref": "Column"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// method id "fusiontables.column.insert":

type ColumnInsertCall struct {
	s          *Service
	tableId    string
	column     *Column
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Adds a new column to the table.
func (r *ColumnService) Insert(tableId string, column *Column) *ColumnInsertCall {
	c := &ColumnInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.column = column
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ColumnInsertCall) Fields(s ...googleapi.Field) *ColumnInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ColumnInsertCall) Context(ctx context.Context) *ColumnInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ColumnInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ColumnInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.column)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/columns")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.column.insert" call.
// Exactly one of *Column or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Column.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ColumnInsertCall) Do(opts ...googleapi.CallOption) (*Column, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Column{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Adds a new column to the table.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.column.insert",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table for which a new column is being added.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/columns",
	//   "request": {
	//     "$ref": "Column"
	//   },
	//   "response": {
	//     "$ref": "Column"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.column.list":

type ColumnListCall struct {
	s            *Service
	tableId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of columns.
func (r *ColumnService) List(tableId string) *ColumnListCall {
	c := &ColumnListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of columns to return.  Default is 5.
func (c *ColumnListCall) MaxResults(maxResults int64) *ColumnListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Continuation token
// specifying which result page to return.
func (c *ColumnListCall) PageToken(pageToken string) *ColumnListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ColumnListCall) Fields(s ...googleapi.Field) *ColumnListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ColumnListCall) IfNoneMatch(entityTag string) *ColumnListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ColumnListCall) Context(ctx context.Context) *ColumnListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ColumnListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ColumnListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/columns")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.column.list" call.
// Exactly one of *ColumnList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ColumnList.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ColumnListCall) Do(opts ...googleapi.CallOption) (*ColumnList, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &ColumnList{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a list of columns.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.column.list",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of columns to return. Optional. Default is 5.",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Continuation token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "tableId": {
	//       "description": "Table whose columns are being listed.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/columns",
	//   "response": {
	//     "$ref": "ColumnList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ColumnListCall) Pages(ctx context.Context, f func(*ColumnList) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "fusiontables.column.patch":

type ColumnPatchCall struct {
	s          *Service
	tableId    string
	columnId   string
	column     *Column
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates the name or type of an existing column. This method
// supports patch semantics.
func (r *ColumnService) Patch(tableId string, columnId string, column *Column) *ColumnPatchCall {
	c := &ColumnPatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.columnId = columnId
	c.column = column
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ColumnPatchCall) Fields(s ...googleapi.Field) *ColumnPatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ColumnPatchCall) Context(ctx context.Context) *ColumnPatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ColumnPatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ColumnPatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.column)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/columns/{columnId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":  c.tableId,
		"columnId": c.columnId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.column.patch" call.
// Exactly one of *Column or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Column.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ColumnPatchCall) Do(opts ...googleapi.CallOption) (*Column, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Column{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates the name or type of an existing column. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "fusiontables.column.patch",
	//   "parameterOrder": [
	//     "tableId",
	//     "columnId"
	//   ],
	//   "parameters": {
	//     "columnId": {
	//       "description": "Name or identifier for the column that is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "tableId": {
	//       "description": "Table for which the column is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/columns/{columnId}",
	//   "request": {
	//     "$ref": "Column"
	//   },
	//   "response": {
	//     "$ref": "Column"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.column.update":

type ColumnUpdateCall struct {
	s          *Service
	tableId    string
	columnId   string
	column     *Column
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates the name or type of an existing column.
func (r *ColumnService) Update(tableId string, columnId string, column *Column) *ColumnUpdateCall {
	c := &ColumnUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.columnId = columnId
	c.column = column
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ColumnUpdateCall) Fields(s ...googleapi.Field) *ColumnUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ColumnUpdateCall) Context(ctx context.Context) *ColumnUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ColumnUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ColumnUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.column)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/columns/{columnId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":  c.tableId,
		"columnId": c.columnId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.column.update" call.
// Exactly one of *Column or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Column.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ColumnUpdateCall) Do(opts ...googleapi.CallOption) (*Column, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Column{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates the name or type of an existing column.",
	//   "httpMethod": "PUT",
	//   "id": "fusiontables.column.update",
	//   "parameterOrder": [
	//     "tableId",
	//     "columnId"
	//   ],
	//   "parameters": {
	//     "columnId": {
	//       "description": "Name or identifier for the column that is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "tableId": {
	//       "description": "Table for which the column is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/columns/{columnId}",
	//   "request": {
	//     "$ref": "Column"
	//   },
	//   "response": {
	//     "$ref": "Column"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.query.sql":

type QuerySqlCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Sql: Executes an SQL SELECT/INSERT/UPDATE/DELETE/SHOW/DESCRIBE/CREATE
// statement.
func (r *QueryService) Sql(sql string) *QuerySqlCall {
	c := &QuerySqlCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("sql", sql)
	return c
}

// Hdrs sets the optional parameter "hdrs": Should column names be
// included (in the first row)?. Default is true.
func (c *QuerySqlCall) Hdrs(hdrs bool) *QuerySqlCall {
	c.urlParams_.Set("hdrs", fmt.Sprint(hdrs))
	return c
}

// Typed sets the optional parameter "typed": Should typed values be
// returned in the (JSON) response -- numbers for numeric values and
// parsed geometries for KML values? Default is true.
func (c *QuerySqlCall) Typed(typed bool) *QuerySqlCall {
	c.urlParams_.Set("typed", fmt.Sprint(typed))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *QuerySqlCall) Fields(s ...googleapi.Field) *QuerySqlCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do and Download
// methods. Any pending HTTP request will be aborted if the provided
// context is canceled.
func (c *QuerySqlCall) Context(ctx context.Context) *QuerySqlCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *QuerySqlCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *QuerySqlCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "query")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Download fetches the API endpoint's "media" value, instead of the normal
// API response value. If the returned error is nil, the Response is guaranteed to
// have a 2xx status code. Callers must close the Response.Body as usual.
func (c *QuerySqlCall) Download(opts ...googleapi.CallOption) (*http.Response, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("media")
	if err != nil {
		return nil, err
	}
	if err := googleapi.CheckMediaResponse(res); err != nil {
		res.Body.Close()
		return nil, err
	}
	return res, nil
}

// Do executes the "fusiontables.query.sql" call.
// Exactly one of *Sqlresponse or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Sqlresponse.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *QuerySqlCall) Do(opts ...googleapi.CallOption) (*Sqlresponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Sqlresponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Executes an SQL SELECT/INSERT/UPDATE/DELETE/SHOW/DESCRIBE/CREATE statement.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.query.sql",
	//   "parameterOrder": [
	//     "sql"
	//   ],
	//   "parameters": {
	//     "hdrs": {
	//       "description": "Should column names be included (in the first row)?. Default is true.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "sql": {
	//       "description": "An SQL SELECT/SHOW/DESCRIBE/INSERT/UPDATE/DELETE/CREATE statement.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "typed": {
	//       "description": "Should typed values be returned in the (JSON) response -- numbers for numeric values and parsed geometries for KML values? Default is true.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "query",
	//   "response": {
	//     "$ref": "Sqlresponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ],
	//   "supportsMediaDownload": true,
	//   "useMediaDownloadService": true
	// }

}

// method id "fusiontables.query.sqlGet":

type QuerySqlGetCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// SqlGet: Executes an SQL SELECT/SHOW/DESCRIBE statement.
func (r *QueryService) SqlGet(sql string) *QuerySqlGetCall {
	c := &QuerySqlGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("sql", sql)
	return c
}

// Hdrs sets the optional parameter "hdrs": Should column names be
// included (in the first row)?. Default is true.
func (c *QuerySqlGetCall) Hdrs(hdrs bool) *QuerySqlGetCall {
	c.urlParams_.Set("hdrs", fmt.Sprint(hdrs))
	return c
}

// Typed sets the optional parameter "typed": Should typed values be
// returned in the (JSON) response -- numbers for numeric values and
// parsed geometries for KML values? Default is true.
func (c *QuerySqlGetCall) Typed(typed bool) *QuerySqlGetCall {
	c.urlParams_.Set("typed", fmt.Sprint(typed))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *QuerySqlGetCall) Fields(s ...googleapi.Field) *QuerySqlGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *QuerySqlGetCall) IfNoneMatch(entityTag string) *QuerySqlGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do and Download
// methods. Any pending HTTP request will be aborted if the provided
// context is canceled.
func (c *QuerySqlGetCall) Context(ctx context.Context) *QuerySqlGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *QuerySqlGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *QuerySqlGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "query")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Download fetches the API endpoint's "media" value, instead of the normal
// API response value. If the returned error is nil, the Response is guaranteed to
// have a 2xx status code. Callers must close the Response.Body as usual.
func (c *QuerySqlGetCall) Download(opts ...googleapi.CallOption) (*http.Response, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("media")
	if err != nil {
		return nil, err
	}
	if err := googleapi.CheckMediaResponse(res); err != nil {
		res.Body.Close()
		return nil, err
	}
	return res, nil
}

// Do executes the "fusiontables.query.sqlGet" call.
// Exactly one of *Sqlresponse or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Sqlresponse.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *QuerySqlGetCall) Do(opts ...googleapi.CallOption) (*Sqlresponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Sqlresponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Executes an SQL SELECT/SHOW/DESCRIBE statement.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.query.sqlGet",
	//   "parameterOrder": [
	//     "sql"
	//   ],
	//   "parameters": {
	//     "hdrs": {
	//       "description": "Should column names be included (in the first row)?. Default is true.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "sql": {
	//       "description": "An SQL SELECT/SHOW/DESCRIBE statement.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "typed": {
	//       "description": "Should typed values be returned in the (JSON) response -- numbers for numeric values and parsed geometries for KML values? Default is true.",
	//       "location": "query",
	//       "type": "boolean"
	//     }
	//   },
	//   "path": "query",
	//   "response": {
	//     "$ref": "Sqlresponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ],
	//   "supportsMediaDownload": true,
	//   "useMediaDownloadService": true
	// }

}

// method id "fusiontables.style.delete":

type StyleDeleteCall struct {
	s          *Service
	tableId    string
	styleId    int64
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a style.
func (r *StyleService) Delete(tableId string, styleId int64) *StyleDeleteCall {
	c := &StyleDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.styleId = styleId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *StyleDeleteCall) Fields(s ...googleapi.Field) *StyleDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *StyleDeleteCall) Context(ctx context.Context) *StyleDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *StyleDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *StyleDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/styles/{styleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
		"styleId": strconv.FormatInt(c.styleId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.style.delete" call.
func (c *StyleDeleteCall) Do(opts ...googleapi.CallOption) error {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return err
	}
	return nil
	// {
	//   "description": "Deletes a style.",
	//   "httpMethod": "DELETE",
	//   "id": "fusiontables.style.delete",
	//   "parameterOrder": [
	//     "tableId",
	//     "styleId"
	//   ],
	//   "parameters": {
	//     "styleId": {
	//       "description": "Identifier (within a table) for the style being deleted",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "tableId": {
	//       "description": "Table from which the style is being deleted",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/styles/{styleId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.style.get":

type StyleGetCall struct {
	s            *Service
	tableId      string
	styleId      int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets a specific style.
func (r *StyleService) Get(tableId string, styleId int64) *StyleGetCall {
	c := &StyleGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.styleId = styleId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *StyleGetCall) Fields(s ...googleapi.Field) *StyleGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *StyleGetCall) IfNoneMatch(entityTag string) *StyleGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *StyleGetCall) Context(ctx context.Context) *StyleGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *StyleGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *StyleGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/styles/{styleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
		"styleId": strconv.FormatInt(c.styleId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.style.get" call.
// Exactly one of *StyleSetting or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *StyleSetting.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *StyleGetCall) Do(opts ...googleapi.CallOption) (*StyleSetting, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &StyleSetting{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Gets a specific style.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.style.get",
	//   "parameterOrder": [
	//     "tableId",
	//     "styleId"
	//   ],
	//   "parameters": {
	//     "styleId": {
	//       "description": "Identifier (integer) for a specific style in a table",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "tableId": {
	//       "description": "Table to which the requested style belongs",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/styles/{styleId}",
	//   "response": {
	//     "$ref": "StyleSetting"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// method id "fusiontables.style.insert":

type StyleInsertCall struct {
	s            *Service
	tableId      string
	stylesetting *StyleSetting
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Insert: Adds a new style for the table.
func (r *StyleService) Insert(tableId string, stylesetting *StyleSetting) *StyleInsertCall {
	c := &StyleInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.stylesetting = stylesetting
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *StyleInsertCall) Fields(s ...googleapi.Field) *StyleInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *StyleInsertCall) Context(ctx context.Context) *StyleInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *StyleInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *StyleInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.stylesetting)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/styles")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.style.insert" call.
// Exactly one of *StyleSetting or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *StyleSetting.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *StyleInsertCall) Do(opts ...googleapi.CallOption) (*StyleSetting, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &StyleSetting{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Adds a new style for the table.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.style.insert",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table for which a new style is being added",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/styles",
	//   "request": {
	//     "$ref": "StyleSetting"
	//   },
	//   "response": {
	//     "$ref": "StyleSetting"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.style.list":

type StyleListCall struct {
	s            *Service
	tableId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of styles.
func (r *StyleService) List(tableId string) *StyleListCall {
	c := &StyleListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of styles to return.  Default is 5.
func (c *StyleListCall) MaxResults(maxResults int64) *StyleListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Continuation token
// specifying which result page to return.
func (c *StyleListCall) PageToken(pageToken string) *StyleListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *StyleListCall) Fields(s ...googleapi.Field) *StyleListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *StyleListCall) IfNoneMatch(entityTag string) *StyleListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *StyleListCall) Context(ctx context.Context) *StyleListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *StyleListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *StyleListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/styles")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.style.list" call.
// Exactly one of *StyleSettingList or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *StyleSettingList.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *StyleListCall) Do(opts ...googleapi.CallOption) (*StyleSettingList, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &StyleSettingList{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a list of styles.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.style.list",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of styles to return. Optional. Default is 5.",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Continuation token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "tableId": {
	//       "description": "Table whose styles are being listed",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/styles",
	//   "response": {
	//     "$ref": "StyleSettingList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *StyleListCall) Pages(ctx context.Context, f func(*StyleSettingList) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "fusiontables.style.patch":

type StylePatchCall struct {
	s            *Service
	tableId      string
	styleId      int64
	stylesetting *StyleSetting
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Patch: Updates an existing style. This method supports patch
// semantics.
func (r *StyleService) Patch(tableId string, styleId int64, stylesetting *StyleSetting) *StylePatchCall {
	c := &StylePatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.styleId = styleId
	c.stylesetting = stylesetting
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *StylePatchCall) Fields(s ...googleapi.Field) *StylePatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *StylePatchCall) Context(ctx context.Context) *StylePatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *StylePatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *StylePatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.stylesetting)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/styles/{styleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
		"styleId": strconv.FormatInt(c.styleId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.style.patch" call.
// Exactly one of *StyleSetting or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *StyleSetting.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *StylePatchCall) Do(opts ...googleapi.CallOption) (*StyleSetting, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &StyleSetting{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates an existing style. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "fusiontables.style.patch",
	//   "parameterOrder": [
	//     "tableId",
	//     "styleId"
	//   ],
	//   "parameters": {
	//     "styleId": {
	//       "description": "Identifier (within a table) for the style being updated.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "tableId": {
	//       "description": "Table whose style is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/styles/{styleId}",
	//   "request": {
	//     "$ref": "StyleSetting"
	//   },
	//   "response": {
	//     "$ref": "StyleSetting"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.style.update":

type StyleUpdateCall struct {
	s            *Service
	tableId      string
	styleId      int64
	stylesetting *StyleSetting
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Update: Updates an existing style.
func (r *StyleService) Update(tableId string, styleId int64, stylesetting *StyleSetting) *StyleUpdateCall {
	c := &StyleUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.styleId = styleId
	c.stylesetting = stylesetting
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *StyleUpdateCall) Fields(s ...googleapi.Field) *StyleUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *StyleUpdateCall) Context(ctx context.Context) *StyleUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *StyleUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *StyleUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.stylesetting)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/styles/{styleId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
		"styleId": strconv.FormatInt(c.styleId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.style.update" call.
// Exactly one of *StyleSetting or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *StyleSetting.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *StyleUpdateCall) Do(opts ...googleapi.CallOption) (*StyleSetting, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &StyleSetting{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates an existing style.",
	//   "httpMethod": "PUT",
	//   "id": "fusiontables.style.update",
	//   "parameterOrder": [
	//     "tableId",
	//     "styleId"
	//   ],
	//   "parameters": {
	//     "styleId": {
	//       "description": "Identifier (within a table) for the style being updated.",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     },
	//     "tableId": {
	//       "description": "Table whose style is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/styles/{styleId}",
	//   "request": {
	//     "$ref": "StyleSetting"
	//   },
	//   "response": {
	//     "$ref": "StyleSetting"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.table.copy":

type TableCopyCall struct {
	s          *Service
	tableId    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Copy: Copies a table.
func (r *TableService) Copy(tableId string) *TableCopyCall {
	c := &TableCopyCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// CopyPresentation sets the optional parameter "copyPresentation":
// Whether to also copy tabs, styles, and templates. Default is false.
func (c *TableCopyCall) CopyPresentation(copyPresentation bool) *TableCopyCall {
	c.urlParams_.Set("copyPresentation", fmt.Sprint(copyPresentation))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableCopyCall) Fields(s ...googleapi.Field) *TableCopyCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TableCopyCall) Context(ctx context.Context) *TableCopyCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableCopyCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableCopyCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/copy")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.copy" call.
// Exactly one of *Table or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Table.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TableCopyCall) Do(opts ...googleapi.CallOption) (*Table, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Table{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Copies a table.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.table.copy",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "copyPresentation": {
	//       "description": "Whether to also copy tabs, styles, and templates. Default is false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "tableId": {
	//       "description": "ID of the table that is being copied.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/copy",
	//   "response": {
	//     "$ref": "Table"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// method id "fusiontables.table.delete":

type TableDeleteCall struct {
	s          *Service
	tableId    string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a table.
func (r *TableService) Delete(tableId string) *TableDeleteCall {
	c := &TableDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableDeleteCall) Fields(s ...googleapi.Field) *TableDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TableDeleteCall) Context(ctx context.Context) *TableDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.delete" call.
func (c *TableDeleteCall) Do(opts ...googleapi.CallOption) error {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return err
	}
	return nil
	// {
	//   "description": "Deletes a table.",
	//   "httpMethod": "DELETE",
	//   "id": "fusiontables.table.delete",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "ID of the table that is being deleted.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.table.get":

type TableGetCall struct {
	s            *Service
	tableId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a specific table by its id.
func (r *TableService) Get(tableId string) *TableGetCall {
	c := &TableGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableGetCall) Fields(s ...googleapi.Field) *TableGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TableGetCall) IfNoneMatch(entityTag string) *TableGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TableGetCall) Context(ctx context.Context) *TableGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.get" call.
// Exactly one of *Table or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Table.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TableGetCall) Do(opts ...googleapi.CallOption) (*Table, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Table{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a specific table by its id.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.table.get",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Identifier(ID) for the table being requested.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}",
	//   "response": {
	//     "$ref": "Table"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// method id "fusiontables.table.importRows":

type TableImportRowsCall struct {
	s                *Service
	tableId          string
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// ImportRows: Import more rows into a table.
func (r *TableService) ImportRows(tableId string) *TableImportRowsCall {
	c := &TableImportRowsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// Delimiter sets the optional parameter "delimiter": The delimiter used
// to separate cell values. This can only consist of a single character.
// Default is ','.
func (c *TableImportRowsCall) Delimiter(delimiter string) *TableImportRowsCall {
	c.urlParams_.Set("delimiter", delimiter)
	return c
}

// Encoding sets the optional parameter "encoding": The encoding of the
// content. Default is UTF-8. Use 'auto-detect' if you are unsure of the
// encoding.
func (c *TableImportRowsCall) Encoding(encoding string) *TableImportRowsCall {
	c.urlParams_.Set("encoding", encoding)
	return c
}

// EndLine sets the optional parameter "endLine": The index of the last
// line from which to start importing, exclusive. Thus, the number of
// imported lines is endLine - startLine. If this parameter is not
// provided, the file will be imported until the last line of the file.
// If endLine is negative, then the imported content will exclude the
// last endLine lines. That is, if endline is negative, no line will be
// imported whose index is greater than N + endLine where N is the
// number of lines in the file, and the number of imported lines will be
// N + endLine - startLine.
func (c *TableImportRowsCall) EndLine(endLine int64) *TableImportRowsCall {
	c.urlParams_.Set("endLine", fmt.Sprint(endLine))
	return c
}

// IsStrict sets the optional parameter "isStrict": Whether the CSV must
// have the same number of values for each row. If false, rows with
// fewer values will be padded with empty values. Default is true.
func (c *TableImportRowsCall) IsStrict(isStrict bool) *TableImportRowsCall {
	c.urlParams_.Set("isStrict", fmt.Sprint(isStrict))
	return c
}

// StartLine sets the optional parameter "startLine": The index of the
// first line from which to start importing, inclusive. Default is 0.
func (c *TableImportRowsCall) StartLine(startLine int64) *TableImportRowsCall {
	c.urlParams_.Set("startLine", fmt.Sprint(startLine))
	return c
}

// Media specifies the media to upload in one or more chunks. The chunk
// size may be controlled by supplying a MediaOption generated by
// googleapi.ChunkSize. The chunk size defaults to
// googleapi.DefaultUploadChunkSize.The Content-Type header used in the
// upload request will be determined by sniffing the contents of r,
// unless a MediaOption generated by googleapi.ContentType is
// supplied.
// At most one of Media and ResumableMedia may be set.
func (c *TableImportRowsCall) Media(r io.Reader, options ...googleapi.MediaOption) *TableImportRowsCall {
	opts := googleapi.ProcessMediaOptions(options)
	chunkSize := opts.ChunkSize
	if !opts.ForceEmptyContentType {
		r, c.mediaType_ = gensupport.DetermineContentType(r, opts.ContentType)
	}
	c.media_, c.mediaBuffer_ = gensupport.PrepareUpload(r, chunkSize)
	return c
}

// ResumableMedia specifies the media to upload in chunks and can be
// canceled with ctx.
//
// Deprecated: use Media instead.
//
// At most one of Media and ResumableMedia may be set. mediaType
// identifies the MIME media type of the upload, such as "image/png". If
// mediaType is "", it will be auto-detected. The provided ctx will
// supersede any context previously provided to the Context method.
func (c *TableImportRowsCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *TableImportRowsCall {
	c.ctx_ = ctx
	rdr := gensupport.ReaderAtToReader(r, size)
	rdr, c.mediaType_ = gensupport.DetermineContentType(rdr, mediaType)
	c.mediaBuffer_ = gensupport.NewMediaBuffer(rdr, googleapi.DefaultUploadChunkSize)
	c.media_ = nil
	c.mediaSize_ = size
	return c
}

// ProgressUpdater provides a callback function that will be called
// after every chunk. It should be a low-latency function in order to
// not slow down the upload operation. This should only be called when
// using ResumableMedia (as opposed to Media).
func (c *TableImportRowsCall) ProgressUpdater(pu googleapi.ProgressUpdater) *TableImportRowsCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableImportRowsCall) Fields(s ...googleapi.Field) *TableImportRowsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *TableImportRowsCall) Context(ctx context.Context) *TableImportRowsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableImportRowsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableImportRowsCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/import")
	if c.media_ != nil || c.mediaBuffer_ != nil {
		urls = strings.Replace(urls, "https://www.googleapis.com/", "https://www.googleapis.com/upload/", 1)
		protocol := "multipart"
		if c.mediaBuffer_ != nil {
			protocol = "resumable"
		}
		c.urlParams_.Set("uploadType", protocol)
	}
	if body == nil {
		body = new(bytes.Buffer)
		reqHeaders.Set("Content-Type", "application/json")
	}
	if c.media_ != nil {
		combined, ctype := gensupport.CombineBodyMedia(body, "application/json", c.media_, c.mediaType_)
		defer combined.Close()
		reqHeaders.Set("Content-Type", ctype)
		body = combined
	}
	if c.mediaBuffer_ != nil && c.mediaType_ != "" {
		reqHeaders.Set("X-Upload-Content-Type", c.mediaType_)
	}
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.importRows" call.
// Exactly one of *Import or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Import.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TableImportRowsCall) Do(opts ...googleapi.CallOption) (*Import, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	if c.mediaBuffer_ != nil {
		loc := res.Header.Get("Location")
		rx := &gensupport.ResumableUpload{
			Client:    c.s.client,
			UserAgent: c.s.userAgent(),
			URI:       loc,
			Media:     c.mediaBuffer_,
			MediaType: c.mediaType_,
			Callback: func(curr int64) {
				if c.progressUpdater_ != nil {
					c.progressUpdater_(curr, c.mediaSize_)
				}
			},
		}
		ctx := c.ctx_
		if ctx == nil {
			ctx = context.TODO()
		}
		res, err = rx.Upload(ctx)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if err := googleapi.CheckResponse(res); err != nil {
			return nil, err
		}
	}
	ret := &Import{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Import more rows into a table.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.table.importRows",
	//   "mediaUpload": {
	//     "accept": [
	//       "application/octet-stream"
	//     ],
	//     "maxSize": "250MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/fusiontables/v1/tables/{tableId}/import"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/fusiontables/v1/tables/{tableId}/import"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "delimiter": {
	//       "description": "The delimiter used to separate cell values. This can only consist of a single character. Default is ','.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "encoding": {
	//       "description": "The encoding of the content. Default is UTF-8. Use 'auto-detect' if you are unsure of the encoding.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "endLine": {
	//       "description": "The index of the last line from which to start importing, exclusive. Thus, the number of imported lines is endLine - startLine. If this parameter is not provided, the file will be imported until the last line of the file. If endLine is negative, then the imported content will exclude the last endLine lines. That is, if endline is negative, no line will be imported whose index is greater than N + endLine where N is the number of lines in the file, and the number of imported lines will be N + endLine - startLine.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "isStrict": {
	//       "description": "Whether the CSV must have the same number of values for each row. If false, rows with fewer values will be padded with empty values. Default is true.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "startLine": {
	//       "description": "The index of the first line from which to start importing, inclusive. Default is 0.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "tableId": {
	//       "description": "The table into which new rows are being imported.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/import",
	//   "response": {
	//     "$ref": "Import"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "fusiontables.table.importTable":

type TableImportTableCall struct {
	s                *Service
	urlParams_       gensupport.URLParams
	media_           io.Reader
	mediaBuffer_     *gensupport.MediaBuffer
	mediaType_       string
	mediaSize_       int64 // mediaSize, if known.  Used only for calls to progressUpdater_.
	progressUpdater_ googleapi.ProgressUpdater
	ctx_             context.Context
	header_          http.Header
}

// ImportTable: Import a new table.
func (r *TableService) ImportTable(name string) *TableImportTableCall {
	c := &TableImportTableCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("name", name)
	return c
}

// Delimiter sets the optional parameter "delimiter": The delimiter used
// to separate cell values. This can only consist of a single character.
// Default is ','.
func (c *TableImportTableCall) Delimiter(delimiter string) *TableImportTableCall {
	c.urlParams_.Set("delimiter", delimiter)
	return c
}

// Encoding sets the optional parameter "encoding": The encoding of the
// content. Default is UTF-8. Use 'auto-detect' if you are unsure of the
// encoding.
func (c *TableImportTableCall) Encoding(encoding string) *TableImportTableCall {
	c.urlParams_.Set("encoding", encoding)
	return c
}

// Media specifies the media to upload in one or more chunks. The chunk
// size may be controlled by supplying a MediaOption generated by
// googleapi.ChunkSize. The chunk size defaults to
// googleapi.DefaultUploadChunkSize.The Content-Type header used in the
// upload request will be determined by sniffing the contents of r,
// unless a MediaOption generated by googleapi.ContentType is
// supplied.
// At most one of Media and ResumableMedia may be set.
func (c *TableImportTableCall) Media(r io.Reader, options ...googleapi.MediaOption) *TableImportTableCall {
	opts := googleapi.ProcessMediaOptions(options)
	chunkSize := opts.ChunkSize
	if !opts.ForceEmptyContentType {
		r, c.mediaType_ = gensupport.DetermineContentType(r, opts.ContentType)
	}
	c.media_, c.mediaBuffer_ = gensupport.PrepareUpload(r, chunkSize)
	return c
}

// ResumableMedia specifies the media to upload in chunks and can be
// canceled with ctx.
//
// Deprecated: use Media instead.
//
// At most one of Media and ResumableMedia may be set. mediaType
// identifies the MIME media type of the upload, such as "image/png". If
// mediaType is "", it will be auto-detected. The provided ctx will
// supersede any context previously provided to the Context method.
func (c *TableImportTableCall) ResumableMedia(ctx context.Context, r io.ReaderAt, size int64, mediaType string) *TableImportTableCall {
	c.ctx_ = ctx
	rdr := gensupport.ReaderAtToReader(r, size)
	rdr, c.mediaType_ = gensupport.DetermineContentType(rdr, mediaType)
	c.mediaBuffer_ = gensupport.NewMediaBuffer(rdr, googleapi.DefaultUploadChunkSize)
	c.media_ = nil
	c.mediaSize_ = size
	return c
}

// ProgressUpdater provides a callback function that will be called
// after every chunk. It should be a low-latency function in order to
// not slow down the upload operation. This should only be called when
// using ResumableMedia (as opposed to Media).
func (c *TableImportTableCall) ProgressUpdater(pu googleapi.ProgressUpdater) *TableImportTableCall {
	c.progressUpdater_ = pu
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableImportTableCall) Fields(s ...googleapi.Field) *TableImportTableCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
// This context will supersede any context previously provided to the
// ResumableMedia method.
func (c *TableImportTableCall) Context(ctx context.Context) *TableImportTableCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableImportTableCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableImportTableCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/import")
	if c.media_ != nil || c.mediaBuffer_ != nil {
		urls = strings.Replace(urls, "https://www.googleapis.com/", "https://www.googleapis.com/upload/", 1)
		protocol := "multipart"
		if c.mediaBuffer_ != nil {
			protocol = "resumable"
		}
		c.urlParams_.Set("uploadType", protocol)
	}
	if body == nil {
		body = new(bytes.Buffer)
		reqHeaders.Set("Content-Type", "application/json")
	}
	if c.media_ != nil {
		combined, ctype := gensupport.CombineBodyMedia(body, "application/json", c.media_, c.mediaType_)
		defer combined.Close()
		reqHeaders.Set("Content-Type", ctype)
		body = combined
	}
	if c.mediaBuffer_ != nil && c.mediaType_ != "" {
		reqHeaders.Set("X-Upload-Content-Type", c.mediaType_)
	}
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.importTable" call.
// Exactly one of *Table or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Table.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TableImportTableCall) Do(opts ...googleapi.CallOption) (*Table, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	if c.mediaBuffer_ != nil {
		loc := res.Header.Get("Location")
		rx := &gensupport.ResumableUpload{
			Client:    c.s.client,
			UserAgent: c.s.userAgent(),
			URI:       loc,
			Media:     c.mediaBuffer_,
			MediaType: c.mediaType_,
			Callback: func(curr int64) {
				if c.progressUpdater_ != nil {
					c.progressUpdater_(curr, c.mediaSize_)
				}
			},
		}
		ctx := c.ctx_
		if ctx == nil {
			ctx = context.TODO()
		}
		res, err = rx.Upload(ctx)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if err := googleapi.CheckResponse(res); err != nil {
			return nil, err
		}
	}
	ret := &Table{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Import a new table.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.table.importTable",
	//   "mediaUpload": {
	//     "accept": [
	//       "application/octet-stream"
	//     ],
	//     "maxSize": "250MB",
	//     "protocols": {
	//       "resumable": {
	//         "multipart": true,
	//         "path": "/resumable/upload/fusiontables/v1/tables/import"
	//       },
	//       "simple": {
	//         "multipart": true,
	//         "path": "/upload/fusiontables/v1/tables/import"
	//       }
	//     }
	//   },
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "delimiter": {
	//       "description": "The delimiter used to separate cell values. This can only consist of a single character. Default is ','.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "encoding": {
	//       "description": "The encoding of the content. Default is UTF-8. Use 'auto-detect' if you are unsure of the encoding.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "name": {
	//       "description": "The name to be assigned to the new table.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/import",
	//   "response": {
	//     "$ref": "Table"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ],
	//   "supportsMediaUpload": true
	// }

}

// method id "fusiontables.table.insert":

type TableInsertCall struct {
	s          *Service
	table      *Table
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Creates a new table.
func (r *TableService) Insert(table *Table) *TableInsertCall {
	c := &TableInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.table = table
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableInsertCall) Fields(s ...googleapi.Field) *TableInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TableInsertCall) Context(ctx context.Context) *TableInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.table)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.insert" call.
// Exactly one of *Table or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Table.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TableInsertCall) Do(opts ...googleapi.CallOption) (*Table, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Table{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Creates a new table.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.table.insert",
	//   "path": "tables",
	//   "request": {
	//     "$ref": "Table"
	//   },
	//   "response": {
	//     "$ref": "Table"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.table.list":

type TableListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of tables a user owns.
func (r *TableService) List() *TableListCall {
	c := &TableListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of styles to return.  Default is 5.
func (c *TableListCall) MaxResults(maxResults int64) *TableListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Continuation token
// specifying which result page to return.
func (c *TableListCall) PageToken(pageToken string) *TableListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableListCall) Fields(s ...googleapi.Field) *TableListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TableListCall) IfNoneMatch(entityTag string) *TableListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TableListCall) Context(ctx context.Context) *TableListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.list" call.
// Exactly one of *TableList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *TableList.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TableListCall) Do(opts ...googleapi.CallOption) (*TableList, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &TableList{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a list of tables a user owns.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.table.list",
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of styles to return. Optional. Default is 5.",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Continuation token specifying which result page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables",
	//   "response": {
	//     "$ref": "TableList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *TableListCall) Pages(ctx context.Context, f func(*TableList) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "fusiontables.table.patch":

type TablePatchCall struct {
	s          *Service
	tableId    string
	table      *Table
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates an existing table. Unless explicitly requested, only
// the name, description, and attribution will be updated. This method
// supports patch semantics.
func (r *TableService) Patch(tableId string, table *Table) *TablePatchCall {
	c := &TablePatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.table = table
	return c
}

// ReplaceViewDefinition sets the optional parameter
// "replaceViewDefinition": Should the view definition also be updated?
// The specified view definition replaces the existing one. Only a view
// can be updated with a new definition.
func (c *TablePatchCall) ReplaceViewDefinition(replaceViewDefinition bool) *TablePatchCall {
	c.urlParams_.Set("replaceViewDefinition", fmt.Sprint(replaceViewDefinition))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TablePatchCall) Fields(s ...googleapi.Field) *TablePatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TablePatchCall) Context(ctx context.Context) *TablePatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TablePatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TablePatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.table)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.patch" call.
// Exactly one of *Table or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Table.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TablePatchCall) Do(opts ...googleapi.CallOption) (*Table, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Table{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates an existing table. Unless explicitly requested, only the name, description, and attribution will be updated. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "fusiontables.table.patch",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "replaceViewDefinition": {
	//       "description": "Should the view definition also be updated? The specified view definition replaces the existing one. Only a view can be updated with a new definition.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "tableId": {
	//       "description": "ID of the table that is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}",
	//   "request": {
	//     "$ref": "Table"
	//   },
	//   "response": {
	//     "$ref": "Table"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.table.update":

type TableUpdateCall struct {
	s          *Service
	tableId    string
	table      *Table
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates an existing table. Unless explicitly requested, only
// the name, description, and attribution will be updated.
func (r *TableService) Update(tableId string, table *Table) *TableUpdateCall {
	c := &TableUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.table = table
	return c
}

// ReplaceViewDefinition sets the optional parameter
// "replaceViewDefinition": Should the view definition also be updated?
// The specified view definition replaces the existing one. Only a view
// can be updated with a new definition.
func (c *TableUpdateCall) ReplaceViewDefinition(replaceViewDefinition bool) *TableUpdateCall {
	c.urlParams_.Set("replaceViewDefinition", fmt.Sprint(replaceViewDefinition))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TableUpdateCall) Fields(s ...googleapi.Field) *TableUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TableUpdateCall) Context(ctx context.Context) *TableUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TableUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TableUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.table)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.table.update" call.
// Exactly one of *Table or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Table.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *TableUpdateCall) Do(opts ...googleapi.CallOption) (*Table, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Table{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates an existing table. Unless explicitly requested, only the name, description, and attribution will be updated.",
	//   "httpMethod": "PUT",
	//   "id": "fusiontables.table.update",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "replaceViewDefinition": {
	//       "description": "Should the view definition also be updated? The specified view definition replaces the existing one. Only a view can be updated with a new definition.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "tableId": {
	//       "description": "ID of the table that is being updated.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}",
	//   "request": {
	//     "$ref": "Table"
	//   },
	//   "response": {
	//     "$ref": "Table"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.task.delete":

type TaskDeleteCall struct {
	s          *Service
	tableId    string
	taskId     string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes the task, unless already started.
func (r *TaskService) Delete(tableId string, taskId string) *TaskDeleteCall {
	c := &TaskDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.taskId = taskId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TaskDeleteCall) Fields(s ...googleapi.Field) *TaskDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TaskDeleteCall) Context(ctx context.Context) *TaskDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TaskDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TaskDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/tasks/{taskId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
		"taskId":  c.taskId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.task.delete" call.
func (c *TaskDeleteCall) Do(opts ...googleapi.CallOption) error {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return err
	}
	return nil
	// {
	//   "description": "Deletes the task, unless already started.",
	//   "httpMethod": "DELETE",
	//   "id": "fusiontables.task.delete",
	//   "parameterOrder": [
	//     "tableId",
	//     "taskId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table from which the task is being deleted.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "taskId": {
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/tasks/{taskId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.task.get":

type TaskGetCall struct {
	s            *Service
	tableId      string
	taskId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a specific task by its id.
func (r *TaskService) Get(tableId string, taskId string) *TaskGetCall {
	c := &TaskGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.taskId = taskId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TaskGetCall) Fields(s ...googleapi.Field) *TaskGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TaskGetCall) IfNoneMatch(entityTag string) *TaskGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TaskGetCall) Context(ctx context.Context) *TaskGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TaskGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TaskGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/tasks/{taskId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
		"taskId":  c.taskId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.task.get" call.
// Exactly one of *Task or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Task.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *TaskGetCall) Do(opts ...googleapi.CallOption) (*Task, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Task{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a specific task by its id.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.task.get",
	//   "parameterOrder": [
	//     "tableId",
	//     "taskId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table to which the task belongs.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "taskId": {
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/tasks/{taskId}",
	//   "response": {
	//     "$ref": "Task"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// method id "fusiontables.task.list":

type TaskListCall struct {
	s            *Service
	tableId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of tasks.
func (r *TaskService) List(tableId string) *TaskListCall {
	c := &TaskListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of columns to return.  Default is 5.
func (c *TaskListCall) MaxResults(maxResults int64) *TaskListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken":
func (c *TaskListCall) PageToken(pageToken string) *TaskListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// StartIndex sets the optional parameter "startIndex":
func (c *TaskListCall) StartIndex(startIndex int64) *TaskListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TaskListCall) Fields(s ...googleapi.Field) *TaskListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TaskListCall) IfNoneMatch(entityTag string) *TaskListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TaskListCall) Context(ctx context.Context) *TaskListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TaskListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TaskListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/tasks")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.task.list" call.
// Exactly one of *TaskList or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *TaskList.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TaskListCall) Do(opts ...googleapi.CallOption) (*TaskList, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &TaskList{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a list of tasks.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.task.list",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of columns to return. Optional. Default is 5.",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startIndex": {
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "tableId": {
	//       "description": "Table whose tasks are being listed.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/tasks",
	//   "response": {
	//     "$ref": "TaskList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *TaskListCall) Pages(ctx context.Context, f func(*TaskList) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "fusiontables.template.delete":

type TemplateDeleteCall struct {
	s          *Service
	tableId    string
	templateId int64
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Delete: Deletes a template
func (r *TemplateService) Delete(tableId string, templateId int64) *TemplateDeleteCall {
	c := &TemplateDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.templateId = templateId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TemplateDeleteCall) Fields(s ...googleapi.Field) *TemplateDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TemplateDeleteCall) Context(ctx context.Context) *TemplateDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TemplateDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TemplateDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/templates/{templateId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":    c.tableId,
		"templateId": strconv.FormatInt(c.templateId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.template.delete" call.
func (c *TemplateDeleteCall) Do(opts ...googleapi.CallOption) error {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return err
	}
	return nil
	// {
	//   "description": "Deletes a template",
	//   "httpMethod": "DELETE",
	//   "id": "fusiontables.template.delete",
	//   "parameterOrder": [
	//     "tableId",
	//     "templateId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table from which the template is being deleted",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "templateId": {
	//       "description": "Identifier for the template which is being deleted",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "tables/{tableId}/templates/{templateId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.template.get":

type TemplateGetCall struct {
	s            *Service
	tableId      string
	templateId   int64
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves a specific template by its id
func (r *TemplateService) Get(tableId string, templateId int64) *TemplateGetCall {
	c := &TemplateGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.templateId = templateId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TemplateGetCall) Fields(s ...googleapi.Field) *TemplateGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TemplateGetCall) IfNoneMatch(entityTag string) *TemplateGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TemplateGetCall) Context(ctx context.Context) *TemplateGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TemplateGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TemplateGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/templates/{templateId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":    c.tableId,
		"templateId": strconv.FormatInt(c.templateId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.template.get" call.
// Exactly one of *Template or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Template.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TemplateGetCall) Do(opts ...googleapi.CallOption) (*Template, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Template{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a specific template by its id",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.template.get",
	//   "parameterOrder": [
	//     "tableId",
	//     "templateId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table to which the template belongs",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "templateId": {
	//       "description": "Identifier for the template that is being requested",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "tables/{tableId}/templates/{templateId}",
	//   "response": {
	//     "$ref": "Template"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// method id "fusiontables.template.insert":

type TemplateInsertCall struct {
	s          *Service
	tableId    string
	template   *Template
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Creates a new template for the table.
func (r *TemplateService) Insert(tableId string, template *Template) *TemplateInsertCall {
	c := &TemplateInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.template = template
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TemplateInsertCall) Fields(s ...googleapi.Field) *TemplateInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TemplateInsertCall) Context(ctx context.Context) *TemplateInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TemplateInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TemplateInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.template)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/templates")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.template.insert" call.
// Exactly one of *Template or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Template.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TemplateInsertCall) Do(opts ...googleapi.CallOption) (*Template, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Template{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Creates a new template for the table.",
	//   "httpMethod": "POST",
	//   "id": "fusiontables.template.insert",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table for which a new template is being created",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/templates",
	//   "request": {
	//     "$ref": "Template"
	//   },
	//   "response": {
	//     "$ref": "Template"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.template.list":

type TemplateListCall struct {
	s            *Service
	tableId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of templates.
func (r *TemplateService) List(tableId string) *TemplateListCall {
	c := &TemplateListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of templates to return.  Default is 5.
func (c *TemplateListCall) MaxResults(maxResults int64) *TemplateListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": Continuation token
// specifying which results page to return.
func (c *TemplateListCall) PageToken(pageToken string) *TemplateListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TemplateListCall) Fields(s ...googleapi.Field) *TemplateListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *TemplateListCall) IfNoneMatch(entityTag string) *TemplateListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TemplateListCall) Context(ctx context.Context) *TemplateListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TemplateListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TemplateListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/templates")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId": c.tableId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.template.list" call.
// Exactly one of *TemplateList or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *TemplateList.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TemplateListCall) Do(opts ...googleapi.CallOption) (*TemplateList, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &TemplateList{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Retrieves a list of templates.",
	//   "httpMethod": "GET",
	//   "id": "fusiontables.template.list",
	//   "parameterOrder": [
	//     "tableId"
	//   ],
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of templates to return. Optional. Default is 5.",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "Continuation token specifying which results page to return. Optional.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "tableId": {
	//       "description": "Identifier for the table whose templates are being requested",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "tables/{tableId}/templates",
	//   "response": {
	//     "$ref": "TemplateList"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables",
	//     "https://www.googleapis.com/auth/fusiontables.readonly"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *TemplateListCall) Pages(ctx context.Context, f func(*TemplateList) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "fusiontables.template.patch":

type TemplatePatchCall struct {
	s          *Service
	tableId    string
	templateId int64
	template   *Template
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Patch: Updates an existing template. This method supports patch
// semantics.
func (r *TemplateService) Patch(tableId string, templateId int64, template *Template) *TemplatePatchCall {
	c := &TemplatePatchCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.templateId = templateId
	c.template = template
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TemplatePatchCall) Fields(s ...googleapi.Field) *TemplatePatchCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TemplatePatchCall) Context(ctx context.Context) *TemplatePatchCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TemplatePatchCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TemplatePatchCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.template)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/templates/{templateId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PATCH", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":    c.tableId,
		"templateId": strconv.FormatInt(c.templateId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.template.patch" call.
// Exactly one of *Template or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Template.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TemplatePatchCall) Do(opts ...googleapi.CallOption) (*Template, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Template{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates an existing template. This method supports patch semantics.",
	//   "httpMethod": "PATCH",
	//   "id": "fusiontables.template.patch",
	//   "parameterOrder": [
	//     "tableId",
	//     "templateId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table to which the updated template belongs",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "templateId": {
	//       "description": "Identifier for the template that is being updated",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "tables/{tableId}/templates/{templateId}",
	//   "request": {
	//     "$ref": "Template"
	//   },
	//   "response": {
	//     "$ref": "Template"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}

// method id "fusiontables.template.update":

type TemplateUpdateCall struct {
	s          *Service
	tableId    string
	templateId int64
	template   *Template
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Update: Updates an existing template
func (r *TemplateService) Update(tableId string, templateId int64, template *Template) *TemplateUpdateCall {
	c := &TemplateUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.tableId = tableId
	c.templateId = templateId
	c.template = template
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *TemplateUpdateCall) Fields(s ...googleapi.Field) *TemplateUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *TemplateUpdateCall) Context(ctx context.Context) *TemplateUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *TemplateUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *TemplateUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.template)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "tables/{tableId}/templates/{templateId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"tableId":    c.tableId,
		"templateId": strconv.FormatInt(c.templateId, 10),
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "fusiontables.template.update" call.
// Exactly one of *Template or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Template.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *TemplateUpdateCall) Do(opts ...googleapi.CallOption) (*Template, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Template{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Updates an existing template",
	//   "httpMethod": "PUT",
	//   "id": "fusiontables.template.update",
	//   "parameterOrder": [
	//     "tableId",
	//     "templateId"
	//   ],
	//   "parameters": {
	//     "tableId": {
	//       "description": "Table to which the updated template belongs",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "templateId": {
	//       "description": "Identifier for the template that is being updated",
	//       "format": "int32",
	//       "location": "path",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "tables/{tableId}/templates/{templateId}",
	//   "request": {
	//     "$ref": "Template"
	//   },
	//   "response": {
	//     "$ref": "Template"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/fusiontables"
	//   ]
	// }

}
