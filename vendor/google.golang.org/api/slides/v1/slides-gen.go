// Package slides provides access to the Google Slides API.
//
// See https://developers.google.com/slides/
//
// Usage example:
//
//   import "google.golang.org/api/slides/v1"
//   ...
//   slidesService, err := slides.New(oauthHttpClient)
package slides // import "google.golang.org/api/slides/v1"

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

const apiId = "slides:v1"
const apiName = "slides"
const apiVersion = "v1"
const basePath = "https://slides.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and manage the files in your Google Drive
	DriveScope = "https://www.googleapis.com/auth/drive"

	// View the files in your Google Drive
	DriveReadonlyScope = "https://www.googleapis.com/auth/drive.readonly"

	// View and manage your Google Slides presentations
	PresentationsScope = "https://www.googleapis.com/auth/presentations"

	// View your Google Slides presentations
	PresentationsReadonlyScope = "https://www.googleapis.com/auth/presentations.readonly"

	// View and manage your spreadsheets in Google Drive
	SpreadsheetsScope = "https://www.googleapis.com/auth/spreadsheets"

	// View your Google Spreadsheets
	SpreadsheetsReadonlyScope = "https://www.googleapis.com/auth/spreadsheets.readonly"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Presentations = NewPresentationsService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Presentations *PresentationsService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewPresentationsService(s *Service) *PresentationsService {
	rs := &PresentationsService{s: s}
	rs.Pages = NewPresentationsPagesService(s)
	return rs
}

type PresentationsService struct {
	s *Service

	Pages *PresentationsPagesService
}

func NewPresentationsPagesService(s *Service) *PresentationsPagesService {
	rs := &PresentationsPagesService{s: s}
	return rs
}

type PresentationsPagesService struct {
	s *Service
}

// AffineTransform: AffineTransform uses a 3x3 matrix with an implied
// last row of [ 0 0 1 ]
// to transform source coordinates (x,y) into destination coordinates
// (x', y')
// according to:
//
//       x'  x  =   shear_y  scale_y  translate_y
//       1  [ 1 ]
//
// After transformation,
//
//      x' = scale_x * x + shear_x * y + translate_x;
//      y' = scale_y * y + shear_y * x + translate_y;
//
// This message is therefore composed of these six matrix elements.
type AffineTransform struct {
	// ScaleX: The X coordinate scaling element.
	ScaleX float64 `json:"scaleX,omitempty"`

	// ScaleY: The Y coordinate scaling element.
	ScaleY float64 `json:"scaleY,omitempty"`

	// ShearX: The X coordinate shearing element.
	ShearX float64 `json:"shearX,omitempty"`

	// ShearY: The Y coordinate shearing element.
	ShearY float64 `json:"shearY,omitempty"`

	// TranslateX: The X coordinate translation element.
	TranslateX float64 `json:"translateX,omitempty"`

	// TranslateY: The Y coordinate translation element.
	TranslateY float64 `json:"translateY,omitempty"`

	// Unit: The units for translate elements.
	//
	// Possible values:
	//   "UNIT_UNSPECIFIED" - The units are unknown.
	//   "EMU" - An English Metric Unit (EMU) is defined as 1/360,000 of a
	// centimeter
	// and thus there are 914,400 EMUs per inch, and 12,700 EMUs per point.
	//   "PT" - A point, 1/72 of an inch.
	Unit string `json:"unit,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ScaleX") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ScaleX") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AffineTransform) MarshalJSON() ([]byte, error) {
	type noMethod AffineTransform
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AutoText: A TextElement kind that represents auto text.
type AutoText struct {
	// Content: The rendered content of this auto text, if available.
	Content string `json:"content,omitempty"`

	// Style: The styling applied to this auto text.
	Style *TextStyle `json:"style,omitempty"`

	// Type: The type of this auto text.
	//
	// Possible values:
	//   "TYPE_UNSPECIFIED" - An unspecified autotext type.
	//   "SLIDE_NUMBER" - Type for autotext that represents the current
	// slide number.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Content") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Content") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AutoText) MarshalJSON() ([]byte, error) {
	type noMethod AutoText
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// BatchUpdatePresentationRequest: Request message for
// PresentationsService.BatchUpdatePresentation.
type BatchUpdatePresentationRequest struct {
	// Requests: A list of updates to apply to the presentation.
	Requests []*Request `json:"requests,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Requests") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Requests") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *BatchUpdatePresentationRequest) MarshalJSON() ([]byte, error) {
	type noMethod BatchUpdatePresentationRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// BatchUpdatePresentationResponse: Response message from a batch
// update.
type BatchUpdatePresentationResponse struct {
	// PresentationId: The presentation the updates were applied to.
	PresentationId string `json:"presentationId,omitempty"`

	// Replies: The reply of the updates.  This maps 1:1 with the updates,
	// although
	// replies to some requests may be empty.
	Replies []*Response `json:"replies,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "PresentationId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PresentationId") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *BatchUpdatePresentationResponse) MarshalJSON() ([]byte, error) {
	type noMethod BatchUpdatePresentationResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Bullet: Describes the bullet of a paragraph.
type Bullet struct {
	// BulletStyle: The paragraph specific text style applied to this
	// bullet.
	BulletStyle *TextStyle `json:"bulletStyle,omitempty"`

	// Glyph: The rendered bullet glyph for this paragraph.
	Glyph string `json:"glyph,omitempty"`

	// ListId: The ID of the list this paragraph belongs to.
	ListId string `json:"listId,omitempty"`

	// NestingLevel: The nesting level of this paragraph in the list.
	NestingLevel int64 `json:"nestingLevel,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BulletStyle") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BulletStyle") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Bullet) MarshalJSON() ([]byte, error) {
	type noMethod Bullet
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ColorScheme: The palette of predefined colors for a page.
type ColorScheme struct {
	// Colors: The ThemeColorType and corresponding concrete color pairs.
	Colors []*ThemeColorPair `json:"colors,omitempty"`

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

func (s *ColorScheme) MarshalJSON() ([]byte, error) {
	type noMethod ColorScheme
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ColorStop: A color and position in a gradient band.
type ColorStop struct {
	// Alpha: The alpha value of this color in the gradient band. Defaults
	// to 1.0,
	// fully opaque.
	Alpha float64 `json:"alpha,omitempty"`

	// Color: The color of the gradient stop.
	Color *OpaqueColor `json:"color,omitempty"`

	// Position: The relative position of the color stop in the gradient
	// band measured
	// in percentage. The value should be in the interval [0.0, 1.0].
	Position float64 `json:"position,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Alpha") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alpha") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ColorStop) MarshalJSON() ([]byte, error) {
	type noMethod ColorStop
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateImageRequest: Creates an image.
type CreateImageRequest struct {
	// ElementProperties: The element properties for the image.
	//
	// When the aspect ratio of the provided size does not match the image
	// aspect
	// ratio, the image is scaled and centered with respect to the size in
	// order
	// to maintain aspect ratio. The provided transform is applied after
	// this
	// operation.
	ElementProperties *PageElementProperties `json:"elementProperties,omitempty"`

	// ObjectId: A user-supplied object ID.
	//
	// If you specify an ID, it must be unique among all pages and page
	// elements
	// in the presentation. The ID must start with an alphanumeric character
	// or an
	// underscore (matches regex `[a-zA-Z0-9_]`); remaining characters
	// may include those as well as a hyphen or colon (matches
	// regex
	// `[a-zA-Z0-9_-:]`).
	// The length of the ID must not be less than 5 or greater than 50.
	//
	// If you don't specify an ID, a unique one is generated.
	ObjectId string `json:"objectId,omitempty"`

	// Url: The image URL.
	//
	// The image is fetched once at insertion time and a copy is stored
	// for
	// display inside the presentation. Images must be less than 50MB in
	// size,
	// cannot exceed 25 megapixels, and must be in either in PNG, JPEG, or
	// GIF
	// format.
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ElementProperties")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ElementProperties") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CreateImageRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateImageRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateImageResponse: The result of creating an image.
type CreateImageResponse struct {
	// ObjectId: The object ID of the created image.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateImageResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateImageResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateLineRequest: Creates a line.
type CreateLineRequest struct {
	// ElementProperties: The element properties for the line.
	ElementProperties *PageElementProperties `json:"elementProperties,omitempty"`

	// LineCategory: The category of line to be created.
	//
	// Possible values:
	//   "STRAIGHT" - Straight connectors, including straight connector 1.
	// The is the default
	// category when one is not specified.
	//   "BENT" - Bent connectors, including bent connector 2 to 5.
	//   "CURVED" - Curved connectors, including curved connector 2 to 5.
	LineCategory string `json:"lineCategory,omitempty"`

	// ObjectId: A user-supplied object ID.
	//
	// If you specify an ID, it must be unique among all pages and page
	// elements
	// in the presentation. The ID must start with an alphanumeric character
	// or an
	// underscore (matches regex `[a-zA-Z0-9_]`); remaining characters
	// may include those as well as a hyphen or colon (matches
	// regex
	// `[a-zA-Z0-9_-:]`).
	// The length of the ID must not be less than 5 or greater than 50.
	//
	// If you don't specify an ID, a unique one is generated.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ElementProperties")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ElementProperties") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CreateLineRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateLineRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateLineResponse: The result of creating a line.
type CreateLineResponse struct {
	// ObjectId: The object ID of the created line.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateLineResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateLineResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateParagraphBulletsRequest: Creates bullets for all of the
// paragraphs that overlap with the given
// text index range.
//
// The nesting level of each paragraph will be determined by counting
// leading
// tabs in front of each paragraph. To avoid excess space between the
// bullet and
// the corresponding paragraph, these leading tabs are removed by this
// request.
// This may change the indices of parts of the text.
//
// If the paragraph immediately before paragraphs being updated is in a
// list
// with a matching preset, the paragraphs being updated are added to
// that
// preceding list.
type CreateParagraphBulletsRequest struct {
	// BulletPreset: The kinds of bullet glyphs to be used. Defaults to
	// the
	// `BULLET_DISC_CIRCLE_SQUARE` preset.
	//
	// Possible values:
	//   "BULLET_DISC_CIRCLE_SQUARE" - A bulleted list with a `DISC`,
	// `CIRCLE` and `SQUARE` bullet glyph for the
	// first 3 list nesting levels.
	//   "BULLET_DIAMONDX_ARROW3D_SQUARE" - A bulleted list with a
	// `DIAMONDX`, `ARROW3D` and `SQUARE` bullet glyph for
	// the first 3 list nesting levels.
	//   "BULLET_CHECKBOX" - A bulleted list with `CHECKBOX` bullet glyphs
	// for all list nesting levels.
	//   "BULLET_ARROW_DIAMOND_DISC" - A bulleted list with a `ARROW`,
	// `DIAMOND` and `DISC` bullet glyph for
	// the first 3 list nesting levels.
	//   "BULLET_STAR_CIRCLE_SQUARE" - A bulleted list with a `STAR`,
	// `CIRCLE` and `DISC` bullet glyph for
	// the first 3 list nesting levels.
	//   "BULLET_ARROW3D_CIRCLE_SQUARE" - A bulleted list with a `ARROW3D`,
	// `CIRCLE` and `SQUARE` bullet glyph for
	// the first 3 list nesting levels.
	//   "BULLET_LEFTTRIANGLE_DIAMOND_DISC" - A bulleted list with a
	// `LEFTTRIANGLE`, `DIAMOND` and `DISC` bullet glyph
	// for the first 3 list nesting levels.
	//   "BULLET_DIAMONDX_HOLLOWDIAMOND_SQUARE" - A bulleted list with a
	// `DIAMONDX`, `HOLLOWDIAMOND` and `SQUARE` bullet
	// glyph for the first 3 list nesting levels.
	//   "BULLET_DIAMOND_CIRCLE_SQUARE" - A bulleted list with a `DIAMOND`,
	// `CIRCLE` and `SQUARE` bullet glyph
	// for the first 3 list nesting levels.
	//   "NUMBERED_DIGIT_ALPHA_ROMAN" - A numbered list with `DIGIT`,
	// `ALPHA` and `ROMAN` numeric glyphs for
	// the first 3 list nesting levels, followed by periods.
	//   "NUMBERED_DIGIT_ALPHA_ROMAN_PARENS" - A numbered list with `DIGIT`,
	// `ALPHA` and `ROMAN` numeric glyphs for
	// the first 3 list nesting levels, followed by parenthesis.
	//   "NUMBERED_DIGIT_NESTED" - A numbered list with `DIGIT` numeric
	// glyphs separated by periods, where
	// each nesting level uses the previous nesting level's glyph as a
	// prefix.
	// For example: '1.', '1.1.', '2.', '2.2.'.
	//   "NUMBERED_UPPERALPHA_ALPHA_ROMAN" - A numbered list with
	// `UPPERALPHA`, `ALPHA` and `ROMAN` numeric glyphs for
	// the first 3 list nesting levels, followed by periods.
	//   "NUMBERED_UPPERROMAN_UPPERALPHA_DIGIT" - A numbered list with
	// `UPPERROMAN`, `UPPERALPHA` and `DIGIT` numeric glyphs
	// for the first 3 list nesting levels, followed by periods.
	//   "NUMBERED_ZERODIGIT_ALPHA_ROMAN" - A numbered list with
	// `ZERODIGIT`, `ALPHA` and `ROMAN` numeric glyphs for
	// the first 3 list nesting levels, followed by periods.
	BulletPreset string `json:"bulletPreset,omitempty"`

	// CellLocation: The optional table cell location if the text to be
	// modified is in a table
	// cell. If present the object_id must refer to a table.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// ObjectId: The object ID of the shape or table containing the text to
	// add bullets to.
	ObjectId string `json:"objectId,omitempty"`

	// TextRange: The range of text to apply the bullet presets to, based on
	// TextElement indexes.
	TextRange *Range `json:"textRange,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BulletPreset") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BulletPreset") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateParagraphBulletsRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateParagraphBulletsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateShapeRequest: Creates a new shape.
type CreateShapeRequest struct {
	// ElementProperties: The element properties for the shape.
	ElementProperties *PageElementProperties `json:"elementProperties,omitempty"`

	// ObjectId: A user-supplied object ID.
	//
	// If you specify an ID, it must be unique among all pages and page
	// elements
	// in the presentation. The ID must start with an alphanumeric character
	// or an
	// underscore (matches regex `[a-zA-Z0-9_]`); remaining characters
	// may include those as well as a hyphen or colon (matches
	// regex
	// `[a-zA-Z0-9_-:]`).
	// The length of the ID must not be less than 5 or greater than 50.
	// If empty, a unique identifier will be generated.
	ObjectId string `json:"objectId,omitempty"`

	// ShapeType: The shape type.
	//
	// Possible values:
	//   "TYPE_UNSPECIFIED" - The shape type that is not predefined.
	//   "TEXT_BOX" - Text box shape.
	//   "RECTANGLE" - Rectangle shape. Corresponds to ECMA-376 ST_ShapeType
	// 'rect'.
	//   "ROUND_RECTANGLE" - Round corner rectangle shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'roundRect'
	//   "ELLIPSE" - Ellipse shape. Corresponds to ECMA-376 ST_ShapeType
	// 'ellipse'
	//   "ARC" - Curved arc shape. Corresponds to ECMA-376 ST_ShapeType
	// 'arc'
	//   "BENT_ARROW" - Bent arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'bentArrow'
	//   "BENT_UP_ARROW" - Bent up arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'bentUpArrow'
	//   "BEVEL" - Bevel shape. Corresponds to ECMA-376 ST_ShapeType 'bevel'
	//   "BLOCK_ARC" - Block arc shape. Corresponds to ECMA-376 ST_ShapeType
	// 'blockArc'
	//   "BRACE_PAIR" - Brace pair shape. Corresponds to ECMA-376
	// ST_ShapeType 'bracePair'
	//   "BRACKET_PAIR" - Bracket pair shape. Corresponds to ECMA-376
	// ST_ShapeType 'bracketPair'
	//   "CAN" - Can shape. Corresponds to ECMA-376 ST_ShapeType 'can'
	//   "CHEVRON" - Chevron shape. Corresponds to ECMA-376 ST_ShapeType
	// 'chevron'
	//   "CHORD" - Chord shape. Corresponds to ECMA-376 ST_ShapeType 'chord'
	//   "CLOUD" - Cloud shape. Corresponds to ECMA-376 ST_ShapeType 'cloud'
	//   "CORNER" - Corner shape. Corresponds to ECMA-376 ST_ShapeType
	// 'corner'
	//   "CUBE" - Cube shape. Corresponds to ECMA-376 ST_ShapeType 'cube'
	//   "CURVED_DOWN_ARROW" - Curved down arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedDownArrow'
	//   "CURVED_LEFT_ARROW" - Curved left arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedLeftArrow'
	//   "CURVED_RIGHT_ARROW" - Curved right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedRightArrow'
	//   "CURVED_UP_ARROW" - Curved up arrow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'curvedUpArrow'
	//   "DECAGON" - Decagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'decagon'
	//   "DIAGONAL_STRIPE" - Diagonal stripe shape. Corresponds to ECMA-376
	// ST_ShapeType 'diagStripe'
	//   "DIAMOND" - Diamond shape. Corresponds to ECMA-376 ST_ShapeType
	// 'diamond'
	//   "DODECAGON" - Dodecagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'dodecagon'
	//   "DONUT" - Donut shape. Corresponds to ECMA-376 ST_ShapeType 'donut'
	//   "DOUBLE_WAVE" - Double wave shape. Corresponds to ECMA-376
	// ST_ShapeType 'doubleWave'
	//   "DOWN_ARROW" - Down arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'downArrow'
	//   "DOWN_ARROW_CALLOUT" - Callout down arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'downArrowCallout'
	//   "FOLDED_CORNER" - Folded corner shape. Corresponds to ECMA-376
	// ST_ShapeType 'foldedCorner'
	//   "FRAME" - Frame shape. Corresponds to ECMA-376 ST_ShapeType 'frame'
	//   "HALF_FRAME" - Half frame shape. Corresponds to ECMA-376
	// ST_ShapeType 'halfFrame'
	//   "HEART" - Heart shape. Corresponds to ECMA-376 ST_ShapeType 'heart'
	//   "HEPTAGON" - Heptagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'heptagon'
	//   "HEXAGON" - Hexagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'hexagon'
	//   "HOME_PLATE" - Home plate shape. Corresponds to ECMA-376
	// ST_ShapeType 'homePlate'
	//   "HORIZONTAL_SCROLL" - Horizontal scroll shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'horizontalScroll'
	//   "IRREGULAR_SEAL_1" - Irregular seal 1 shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'irregularSeal1'
	//   "IRREGULAR_SEAL_2" - Irregular seal 2 shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'irregularSeal2'
	//   "LEFT_ARROW" - Left arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftArrow'
	//   "LEFT_ARROW_CALLOUT" - Callout left arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'leftArrowCallout'
	//   "LEFT_BRACE" - Left brace shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftBrace'
	//   "LEFT_BRACKET" - Left bracket shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftBracket'
	//   "LEFT_RIGHT_ARROW" - Left right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'leftRightArrow'
	//   "LEFT_RIGHT_ARROW_CALLOUT" - Callout left right arrow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'leftRightArrowCallout'
	//   "LEFT_RIGHT_UP_ARROW" - Left right up arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'leftRightUpArrow'
	//   "LEFT_UP_ARROW" - Left up arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftUpArrow'
	//   "LIGHTNING_BOLT" - Lightning bolt shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'lightningBolt'
	//   "MATH_DIVIDE" - Divide math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathDivide'
	//   "MATH_EQUAL" - Equal math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathEqual'
	//   "MATH_MINUS" - Minus math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathMinus'
	//   "MATH_MULTIPLY" - Multiply math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathMultiply'
	//   "MATH_NOT_EQUAL" - Not equal math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathNotEqual'
	//   "MATH_PLUS" - Plus math shape. Corresponds to ECMA-376 ST_ShapeType
	// 'mathPlus'
	//   "MOON" - Moon shape. Corresponds to ECMA-376 ST_ShapeType 'moon'
	//   "NO_SMOKING" - No smoking shape. Corresponds to ECMA-376
	// ST_ShapeType 'noSmoking'
	//   "NOTCHED_RIGHT_ARROW" - Notched right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'notchedRightArrow'
	//   "OCTAGON" - Octagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'octagon'
	//   "PARALLELOGRAM" - Parallelogram shape. Corresponds to ECMA-376
	// ST_ShapeType 'parallelogram'
	//   "PENTAGON" - Pentagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'pentagon'
	//   "PIE" - Pie shape. Corresponds to ECMA-376 ST_ShapeType 'pie'
	//   "PLAQUE" - Plaque shape. Corresponds to ECMA-376 ST_ShapeType
	// 'plaque'
	//   "PLUS" - Plus shape. Corresponds to ECMA-376 ST_ShapeType 'plus'
	//   "QUAD_ARROW" - Quad-arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'quadArrow'
	//   "QUAD_ARROW_CALLOUT" - Callout quad-arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'quadArrowCallout'
	//   "RIBBON" - Ribbon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'ribbon'
	//   "RIBBON_2" - Ribbon 2 shape. Corresponds to ECMA-376 ST_ShapeType
	// 'ribbon2'
	//   "RIGHT_ARROW" - Right arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'rightArrow'
	//   "RIGHT_ARROW_CALLOUT" - Callout right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'rightArrowCallout'
	//   "RIGHT_BRACE" - Right brace shape. Corresponds to ECMA-376
	// ST_ShapeType 'rightBrace'
	//   "RIGHT_BRACKET" - Right bracket shape. Corresponds to ECMA-376
	// ST_ShapeType 'rightBracket'
	//   "ROUND_1_RECTANGLE" - One round corner rectangle shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'round1Rect'
	//   "ROUND_2_DIAGONAL_RECTANGLE" - Two diagonal round corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'round2DiagRect'
	//   "ROUND_2_SAME_RECTANGLE" - Two same-side round corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'round2SameRect'
	//   "RIGHT_TRIANGLE" - Right triangle shape. Corresponds to ECMA-376
	// ST_ShapeType 'rtTriangle'
	//   "SMILEY_FACE" - Smiley face shape. Corresponds to ECMA-376
	// ST_ShapeType 'smileyFace'
	//   "SNIP_1_RECTANGLE" - One snip corner rectangle shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'snip1Rect'
	//   "SNIP_2_DIAGONAL_RECTANGLE" - Two diagonal snip corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'snip2DiagRect'
	//   "SNIP_2_SAME_RECTANGLE" - Two same-side snip corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'snip2SameRect'
	//   "SNIP_ROUND_RECTANGLE" - One snip one round corner rectangle shape.
	// Corresponds to ECMA-376
	// ST_ShapeType 'snipRoundRect'
	//   "STAR_10" - Ten pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star10'
	//   "STAR_12" - Twelve pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star12'
	//   "STAR_16" - Sixteen pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star16'
	//   "STAR_24" - Twenty four pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'star24'
	//   "STAR_32" - Thirty two pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'star32'
	//   "STAR_4" - Four pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star4'
	//   "STAR_5" - Five pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star5'
	//   "STAR_6" - Six pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star6'
	//   "STAR_7" - Seven pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star7'
	//   "STAR_8" - Eight pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star8'
	//   "STRIPED_RIGHT_ARROW" - Striped right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'stripedRightArrow'
	//   "SUN" - Sun shape. Corresponds to ECMA-376 ST_ShapeType 'sun'
	//   "TRAPEZOID" - Trapezoid shape. Corresponds to ECMA-376 ST_ShapeType
	// 'trapezoid'
	//   "TRIANGLE" - Triangle shape. Corresponds to ECMA-376 ST_ShapeType
	// 'triangle'
	//   "UP_ARROW" - Up arrow shape. Corresponds to ECMA-376 ST_ShapeType
	// 'upArrow'
	//   "UP_ARROW_CALLOUT" - Callout up arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'upArrowCallout'
	//   "UP_DOWN_ARROW" - Up down arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'upDownArrow'
	//   "UTURN_ARROW" - U-turn arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'uturnArrow'
	//   "VERTICAL_SCROLL" - Vertical scroll shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'verticalScroll'
	//   "WAVE" - Wave shape. Corresponds to ECMA-376 ST_ShapeType 'wave'
	//   "WEDGE_ELLIPSE_CALLOUT" - Callout wedge ellipse shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'wedgeEllipseCallout'
	//   "WEDGE_RECTANGLE_CALLOUT" - Callout wedge rectangle shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'wedgeRectCallout'
	//   "WEDGE_ROUND_RECTANGLE_CALLOUT" - Callout wedge round rectangle
	// shape. Corresponds to ECMA-376 ST_ShapeType
	// 'wedgeRoundRectCallout'
	//   "FLOW_CHART_ALTERNATE_PROCESS" - Alternate process flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartAlternateProcess'
	//   "FLOW_CHART_COLLATE" - Collate flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartCollate'
	//   "FLOW_CHART_CONNECTOR" - Connector flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartConnector'
	//   "FLOW_CHART_DECISION" - Decision flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartDecision'
	//   "FLOW_CHART_DELAY" - Delay flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartDelay'
	//   "FLOW_CHART_DISPLAY" - Display flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartDisplay'
	//   "FLOW_CHART_DOCUMENT" - Document flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartDocument'
	//   "FLOW_CHART_EXTRACT" - Extract flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartExtract'
	//   "FLOW_CHART_INPUT_OUTPUT" - Input output flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartInputOutput'
	//   "FLOW_CHART_INTERNAL_STORAGE" - Internal storage flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartInternalStorage'
	//   "FLOW_CHART_MAGNETIC_DISK" - Magnetic disk flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMagneticDisk'
	//   "FLOW_CHART_MAGNETIC_DRUM" - Magnetic drum flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMagneticDrum'
	//   "FLOW_CHART_MAGNETIC_TAPE" - Magnetic tape flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMagneticTape'
	//   "FLOW_CHART_MANUAL_INPUT" - Manual input flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartManualInput'
	//   "FLOW_CHART_MANUAL_OPERATION" - Manual operation flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartManualOperation'
	//   "FLOW_CHART_MERGE" - Merge flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartMerge'
	//   "FLOW_CHART_MULTIDOCUMENT" - Multi-document flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMultidocument'
	//   "FLOW_CHART_OFFLINE_STORAGE" - Offline storage flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartOfflineStorage'
	//   "FLOW_CHART_OFFPAGE_CONNECTOR" - Off-page connector flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartOffpageConnector'
	//   "FLOW_CHART_ONLINE_STORAGE" - Online storage flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartOnlineStorage'
	//   "FLOW_CHART_OR" - Or flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartOr'
	//   "FLOW_CHART_PREDEFINED_PROCESS" - Predefined process flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartPredefinedProcess'
	//   "FLOW_CHART_PREPARATION" - Preparation flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartPreparation'
	//   "FLOW_CHART_PROCESS" - Process flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartProcess'
	//   "FLOW_CHART_PUNCHED_CARD" - Punched card flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartPunchedCard'
	//   "FLOW_CHART_PUNCHED_TAPE" - Punched tape flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartPunchedTape'
	//   "FLOW_CHART_SORT" - Sort flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartSort'
	//   "FLOW_CHART_SUMMING_JUNCTION" - Summing junction flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartSummingJunction'
	//   "FLOW_CHART_TERMINATOR" - Terminator flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartTerminator'
	//   "ARROW_EAST" - East arrow shape.
	//   "ARROW_NORTH_EAST" - Northeast arrow shape.
	//   "ARROW_NORTH" - North arrow shape.
	//   "SPEECH" - Speech shape.
	//   "STARBURST" - Star burst shape.
	//   "TEARDROP" - Teardrop shape. Corresponds to ECMA-376 ST_ShapeType
	// 'teardrop'
	//   "ELLIPSE_RIBBON" - Ellipse ribbon shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'ellipseRibbon'
	//   "ELLIPSE_RIBBON_2" - Ellipse ribbon 2 shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'ellipseRibbon2'
	//   "CLOUD_CALLOUT" - Callout cloud shape. Corresponds to ECMA-376
	// ST_ShapeType 'cloudCallout'
	//   "CUSTOM" - Custom shape.
	ShapeType string `json:"shapeType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ElementProperties")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ElementProperties") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CreateShapeRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateShapeRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateShapeResponse: The result of creating a shape.
type CreateShapeResponse struct {
	// ObjectId: The object ID of the created shape.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateShapeResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateShapeResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateSheetsChartRequest: Creates an embedded Google Sheets
// chart.
//
// NOTE: Chart creation requires  at least one of the
// spreadsheets.readonly,
// spreadsheets, drive.readonly, or drive OAuth scopes.
type CreateSheetsChartRequest struct {
	// ChartId: The ID of the specific chart in the Google Sheets
	// spreadsheet.
	ChartId int64 `json:"chartId,omitempty"`

	// ElementProperties: The element properties for the chart.
	//
	// When the aspect ratio of the provided size does not match the chart
	// aspect
	// ratio, the chart is scaled and centered with respect to the size in
	// order
	// to maintain aspect ratio. The provided transform is applied after
	// this
	// operation.
	ElementProperties *PageElementProperties `json:"elementProperties,omitempty"`

	// LinkingMode: The mode with which the chart is linked to the source
	// spreadsheet. When
	// not specified, the chart will be an image that is not linked.
	//
	// Possible values:
	//   "NOT_LINKED_IMAGE" - The chart is not associated with the source
	// spreadsheet and cannot be
	// updated. A chart that is not linked will be inserted as an image.
	//   "LINKED" - Linking the chart allows it to be updated, and other
	// collaborators will
	// see a link to the spreadsheet.
	LinkingMode string `json:"linkingMode,omitempty"`

	// ObjectId: A user-supplied object ID.
	//
	// If specified, the ID must be unique among all pages and page elements
	// in
	// the presentation. The ID should start with a word character
	// [a-zA-Z0-9_]
	// and then followed by any number of the following characters
	// [a-zA-Z0-9_-:].
	// The length of the ID should not be less than 5 or greater than 50.
	// If empty, a unique identifier will be generated.
	ObjectId string `json:"objectId,omitempty"`

	// SpreadsheetId: The ID of the Google Sheets spreadsheet that contains
	// the chart.
	SpreadsheetId string `json:"spreadsheetId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ChartId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ChartId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateSheetsChartRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateSheetsChartRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateSheetsChartResponse: The result of creating an embedded Google
// Sheets chart.
type CreateSheetsChartResponse struct {
	// ObjectId: The object ID of the created chart.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateSheetsChartResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateSheetsChartResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateSlideRequest: Creates a new slide.
type CreateSlideRequest struct {
	// InsertionIndex: The optional zero-based index indicating where to
	// insert the slides.
	//
	// If you don't specify an index, the new slide is created at the end.
	InsertionIndex int64 `json:"insertionIndex,omitempty"`

	// ObjectId: A user-supplied object ID.
	//
	// If you specify an ID, it must be unique among all pages and page
	// elements
	// in the presentation. The ID must start with an alphanumeric character
	// or an
	// underscore (matches regex `[a-zA-Z0-9_]`); remaining characters
	// may include those as well as a hyphen or colon (matches
	// regex
	// `[a-zA-Z0-9_-:]`).
	// The length of the ID must not be less than 5 or greater than 50.
	//
	// If you don't specify an ID, a unique one is generated.
	ObjectId string `json:"objectId,omitempty"`

	// SlideLayoutReference: Layout reference of the slide to be inserted,
	// based on the *current
	// master*, which is one of the following:
	//
	// - The master of the previous slide index.
	// - The master of the first slide, if the insertion_index is zero.
	// - The first master in the presentation, if there are no slides.
	//
	// If the LayoutReference is not found in the current master, a 400
	// bad
	// request error is returned.
	//
	// If you don't specify a layout reference, then the new slide will use
	// the
	// predefined layout `BLANK`.
	SlideLayoutReference *LayoutReference `json:"slideLayoutReference,omitempty"`

	// ForceSendFields is a list of field names (e.g. "InsertionIndex") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "InsertionIndex") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CreateSlideRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateSlideRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateSlideResponse: The result of creating a slide.
type CreateSlideResponse struct {
	// ObjectId: The object ID of the created slide.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateSlideResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateSlideResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateTableRequest: Creates a new table.
type CreateTableRequest struct {
	// Columns: Number of columns in the table.
	Columns int64 `json:"columns,omitempty"`

	// ElementProperties: The element properties for the table.
	//
	// The table will be created at the provided size, subject to a minimum
	// size.
	// If no size is provided, the table will be automatically sized.
	//
	// Table transforms must have a scale of 1 and no shear components. If
	// no
	// transform is provided, the table will be centered on the page.
	ElementProperties *PageElementProperties `json:"elementProperties,omitempty"`

	// ObjectId: A user-supplied object ID.
	//
	// If you specify an ID, it must be unique among all pages and page
	// elements
	// in the presentation. The ID must start with an alphanumeric character
	// or an
	// underscore (matches regex `[a-zA-Z0-9_]`); remaining characters
	// may include those as well as a hyphen or colon (matches
	// regex
	// `[a-zA-Z0-9_-:]`).
	// The length of the ID must not be less than 5 or greater than 50.
	//
	// If you don't specify an ID, a unique one is generated.
	ObjectId string `json:"objectId,omitempty"`

	// Rows: Number of rows in the table.
	Rows int64 `json:"rows,omitempty"`

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

func (s *CreateTableRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateTableRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateTableResponse: The result of creating a table.
type CreateTableResponse struct {
	// ObjectId: The object ID of the created table.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateTableResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateTableResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateVideoRequest: Creates a video.
type CreateVideoRequest struct {
	// ElementProperties: The element properties for the video.
	ElementProperties *PageElementProperties `json:"elementProperties,omitempty"`

	// Id: The video source's unique identifier for this video.
	//
	// e.g. For YouTube video
	// https://www.youtube.com/watch?v=7U3axjORYZ0,
	// the ID is 7U3axjORYZ0.
	Id string `json:"id,omitempty"`

	// ObjectId: A user-supplied object ID.
	//
	// If you specify an ID, it must be unique among all pages and page
	// elements
	// in the presentation. The ID must start with an alphanumeric character
	// or an
	// underscore (matches regex `[a-zA-Z0-9_]`); remaining characters
	// may include those as well as a hyphen or colon (matches
	// regex
	// `[a-zA-Z0-9_-:]`).
	// The length of the ID must not be less than 5 or greater than 50.
	//
	// If you don't specify an ID, a unique one is generated.
	ObjectId string `json:"objectId,omitempty"`

	// Source: The video source.
	//
	// Possible values:
	//   "SOURCE_UNSPECIFIED" - The video source is unspecified.
	//   "YOUTUBE" - The video source is YouTube.
	Source string `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ElementProperties")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ElementProperties") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CreateVideoRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateVideoRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateVideoResponse: The result of creating a video.
type CreateVideoResponse struct {
	// ObjectId: The object ID of the created video.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateVideoResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateVideoResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CropProperties: The crop properties of an object enclosed in a
// container. For example, an
// Image.
//
// The crop properties is represented by the offsets of four edges which
// define
// a crop rectangle. The offsets are measured in percentage from
// the
// corresponding edges of the object's original bounding rectangle
// towards
// inside, relative to the object's original dimensions.
//
// - If the offset is in the interval (0, 1), the corresponding edge of
// crop
// rectangle is positioned inside of the object's original bounding
// rectangle.
// - If the offset is negative or greater than 1, the corresponding edge
// of crop
// rectangle is positioned outside of the object's original bounding
// rectangle.
// - If the left edge of the crop rectangle is on the right side of its
// right
// edge, the object will be flipped horizontally.
// - If the top edge of the crop rectangle is below its bottom edge, the
// object
// will be flipped vertically.
// - If all offsets and rotation angle is 0, the object is not
// cropped.
//
// After cropping, the content in the crop rectangle will be stretched
// to fit
// its container.
type CropProperties struct {
	// Angle: The rotation angle of the crop window around its center, in
	// radians.
	// Rotation angle is applied after the offset.
	Angle float64 `json:"angle,omitempty"`

	// BottomOffset: The offset specifies the bottom edge of the crop
	// rectangle that is located
	// above the original bounding rectangle bottom edge, relative to the
	// object's
	// original height.
	BottomOffset float64 `json:"bottomOffset,omitempty"`

	// LeftOffset: The offset specifies the left edge of the crop rectangle
	// that is located to
	// the right of the original bounding rectangle left edge, relative to
	// the
	// object's original width.
	LeftOffset float64 `json:"leftOffset,omitempty"`

	// RightOffset: The offset specifies the right edge of the crop
	// rectangle that is located
	// to the left of the original bounding rectangle right edge, relative
	// to the
	// object's original width.
	RightOffset float64 `json:"rightOffset,omitempty"`

	// TopOffset: The offset specifies the top edge of the crop rectangle
	// that is located
	// below the original bounding rectangle top edge, relative to the
	// object's
	// original height.
	TopOffset float64 `json:"topOffset,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Angle") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Angle") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CropProperties) MarshalJSON() ([]byte, error) {
	type noMethod CropProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeleteObjectRequest: Deletes an object, either pages or
// page elements, from the
// presentation.
type DeleteObjectRequest struct {
	// ObjectId: The object ID of the page or page element to delete.
	//
	// If after a delete operation a group contains
	// only 1 or no page elements, the group is also deleted.
	//
	// If a placeholder is deleted on a layout, any empty inheriting shapes
	// are
	// also deleted.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeleteObjectRequest) MarshalJSON() ([]byte, error) {
	type noMethod DeleteObjectRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeleteTableColumnRequest: Deletes a column from a table.
type DeleteTableColumnRequest struct {
	// CellLocation: The reference table cell location from which a column
	// will be deleted.
	//
	// The column this cell spans will be deleted. If this is a merged
	// cell,
	// multiple columns will be deleted. If no columns remain in the table
	// after
	// this deletion, the whole table is deleted.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// TableObjectId: The table to delete columns from.
	TableObjectId string `json:"tableObjectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CellLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CellLocation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeleteTableColumnRequest) MarshalJSON() ([]byte, error) {
	type noMethod DeleteTableColumnRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeleteTableRowRequest: Deletes a row from a table.
type DeleteTableRowRequest struct {
	// CellLocation: The reference table cell location from which a row will
	// be deleted.
	//
	// The row this cell spans will be deleted. If this is a merged cell,
	// multiple
	// rows will be deleted. If no rows remain in the table after this
	// deletion,
	// the whole table is deleted.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// TableObjectId: The table to delete rows from.
	TableObjectId string `json:"tableObjectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CellLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CellLocation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeleteTableRowRequest) MarshalJSON() ([]byte, error) {
	type noMethod DeleteTableRowRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeleteTextRequest: Deletes text from a shape or a table cell.
type DeleteTextRequest struct {
	// CellLocation: The optional table cell location if the text is to be
	// deleted from a table
	// cell. If present the object_id must refer to a table.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// ObjectId: The object ID of the shape or table from which the text
	// will be deleted.
	ObjectId string `json:"objectId,omitempty"`

	// TextRange: The range of text to delete, based on TextElement
	// indexes.
	//
	// There is always an implicit newline character at the end of a shape's
	// or
	// table cell's text that cannot be deleted. `Range.Type.ALL` will use
	// the
	// correct bounds, but care must be taken when specifying explicit
	// bounds for
	// range types `FROM_START_INDEX` and `FIXED_RANGE`. For example, if the
	// text
	// is "ABC", followed by an implicit newline, then the maximum value is
	// 2 for
	// `text_range.start_index` and 3 for `text_range.end_index`.
	//
	// Deleting text that crosses a paragraph boundary may result in
	// changes
	// to paragraph styles and lists as the two paragraphs are
	// merged.
	//
	// Ranges that include only one code unit of a surrogate pair are
	// expanded to
	// include both code units.
	TextRange *Range `json:"textRange,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CellLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CellLocation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DeleteTextRequest) MarshalJSON() ([]byte, error) {
	type noMethod DeleteTextRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Dimension: A magnitude in a single direction in the specified units.
type Dimension struct {
	// Magnitude: The magnitude.
	Magnitude float64 `json:"magnitude,omitempty"`

	// Unit: The units for magnitude.
	//
	// Possible values:
	//   "UNIT_UNSPECIFIED" - The units are unknown.
	//   "EMU" - An English Metric Unit (EMU) is defined as 1/360,000 of a
	// centimeter
	// and thus there are 914,400 EMUs per inch, and 12,700 EMUs per point.
	//   "PT" - A point, 1/72 of an inch.
	Unit string `json:"unit,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Magnitude") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Magnitude") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Dimension) MarshalJSON() ([]byte, error) {
	type noMethod Dimension
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DuplicateObjectRequest: Duplicates a slide or page element.
//
// When duplicating a slide, the duplicate slide will be created
// immediately
// following the specified slide. When duplicating a page element, the
// duplicate
// will be placed on the same page at the same position as the original.
type DuplicateObjectRequest struct {
	// ObjectId: The ID of the object to duplicate.
	ObjectId string `json:"objectId,omitempty"`

	// ObjectIds: The object being duplicated may contain other objects, for
	// example when
	// duplicating a slide or a group page element. This map defines how the
	// IDs
	// of duplicated objects are generated: the keys are the IDs of the
	// original
	// objects and its values are the IDs that will be assigned to
	// the
	// corresponding duplicate object. The ID of the source object's
	// duplicate
	// may be specified in this map as well, using the same value of
	// the
	// `object_id` field as a key and the newly desired ID as the
	// value.
	//
	// All keys must correspond to existing IDs in the presentation. All
	// values
	// must be unique in the presentation and must start with an
	// alphanumeric
	// character or an underscore (matches regex `[a-zA-Z0-9_]`);
	// remaining
	// characters may include those as well as a hyphen or colon (matches
	// regex
	// `[a-zA-Z0-9_-:]`). The length of the new ID must not be less than 5
	// or
	// greater than 50.
	//
	// If any IDs of source objects are omitted from the map, a new random
	// ID will
	// be assigned. If the map is empty or unset, all duplicate objects
	// will
	// receive a new random ID.
	ObjectIds map[string]string `json:"objectIds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DuplicateObjectRequest) MarshalJSON() ([]byte, error) {
	type noMethod DuplicateObjectRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DuplicateObjectResponse: The response of duplicating an object.
type DuplicateObjectResponse struct {
	// ObjectId: The ID of the new duplicate object.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DuplicateObjectResponse) MarshalJSON() ([]byte, error) {
	type noMethod DuplicateObjectResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Group: A PageElement kind representing a
// joined collection of PageElements.
type Group struct {
	// Children: The collection of elements in the group. The minimum size
	// of a group is 2.
	Children []*PageElement `json:"children,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Children") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Children") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Group) MarshalJSON() ([]byte, error) {
	type noMethod Group
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Image: A PageElement kind representing an
// image.
type Image struct {
	// ContentUrl: An URL to an image with a default lifetime of 30
	// minutes.
	// This URL is tagged with the account of the requester. Anyone with the
	// URL
	// effectively accesses the image as the original requester. Access to
	// the
	// image may be lost if the presentation's sharing settings change.
	ContentUrl string `json:"contentUrl,omitempty"`

	// ImageProperties: The properties of the image.
	ImageProperties *ImageProperties `json:"imageProperties,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContentUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContentUrl") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Image) MarshalJSON() ([]byte, error) {
	type noMethod Image
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ImageProperties: The properties of the Image.
type ImageProperties struct {
	// Brightness: The brightness effect of the image. The value should be
	// in the interval
	// [-1.0, 1.0], where 0 means no effect. This property is read-only.
	Brightness float64 `json:"brightness,omitempty"`

	// Contrast: The contrast effect of the image. The value should be in
	// the interval
	// [-1.0, 1.0], where 0 means no effect. This property is read-only.
	Contrast float64 `json:"contrast,omitempty"`

	// CropProperties: The crop properties of the image. If not set, the
	// image is not cropped.
	// This property is read-only.
	CropProperties *CropProperties `json:"cropProperties,omitempty"`

	// Outline: The outline of the image. If not set, the the image has no
	// outline.
	Outline *Outline `json:"outline,omitempty"`

	// Recolor: The recolor effect of the image. If not set, the image is
	// not recolored.
	// This property is read-only.
	Recolor *Recolor `json:"recolor,omitempty"`

	// Shadow: The shadow of the image. If not set, the image has no shadow.
	// This property
	// is read-only.
	Shadow *Shadow `json:"shadow,omitempty"`

	// Transparency: The transparency effect of the image. The value should
	// be in the interval
	// [0.0, 1.0], where 0 means no effect and 1 means completely
	// transparent.
	// This property is read-only.
	Transparency float64 `json:"transparency,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Brightness") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Brightness") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ImageProperties) MarshalJSON() ([]byte, error) {
	type noMethod ImageProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// InsertTableColumnsRequest: Inserts columns into a table.
//
// Other columns in the table will be resized to fit the new column.
type InsertTableColumnsRequest struct {
	// CellLocation: The reference table cell location from which columns
	// will be inserted.
	//
	// A new column will be inserted to the left (or right) of the column
	// where
	// the reference cell is. If the reference cell is a merged cell, a
	// new
	// column will be inserted to the left (or right) of the merged cell.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// InsertRight: Whether to insert new columns to the right of the
	// reference cell location.
	//
	// - `True`: insert to the right.
	// - `False`: insert to the left.
	InsertRight bool `json:"insertRight,omitempty"`

	// Number: The number of columns to be inserted. Maximum 20 per request.
	Number int64 `json:"number,omitempty"`

	// TableObjectId: The table to insert columns into.
	TableObjectId string `json:"tableObjectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CellLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CellLocation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *InsertTableColumnsRequest) MarshalJSON() ([]byte, error) {
	type noMethod InsertTableColumnsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// InsertTableRowsRequest: Inserts rows into a table.
type InsertTableRowsRequest struct {
	// CellLocation: The reference table cell location from which rows will
	// be inserted.
	//
	// A new row will be inserted above (or below) the row where the
	// reference
	// cell is. If the reference cell is a merged cell, a new row will
	// be
	// inserted above (or below) the merged cell.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// InsertBelow: Whether to insert new rows below the reference cell
	// location.
	//
	// - `True`: insert below the cell.
	// - `False`: insert above the cell.
	InsertBelow bool `json:"insertBelow,omitempty"`

	// Number: The number of rows to be inserted. Maximum 20 per request.
	Number int64 `json:"number,omitempty"`

	// TableObjectId: The table to insert rows into.
	TableObjectId string `json:"tableObjectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CellLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CellLocation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *InsertTableRowsRequest) MarshalJSON() ([]byte, error) {
	type noMethod InsertTableRowsRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// InsertTextRequest: Inserts text into a shape or a table cell.
type InsertTextRequest struct {
	// CellLocation: The optional table cell location if the text is to be
	// inserted into a table
	// cell. If present the object_id must refer to a table.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// InsertionIndex: The index where the text will be inserted, in Unicode
	// code units, based
	// on TextElement indexes.
	//
	// The index is zero-based and is computed from the start of the
	// string.
	// The index may be adjusted to prevent insertions inside Unicode
	// grapheme
	// clusters. In these cases, the text will be inserted immediately after
	// the
	// grapheme cluster.
	InsertionIndex int64 `json:"insertionIndex,omitempty"`

	// ObjectId: The object ID of the shape or table where the text will be
	// inserted.
	ObjectId string `json:"objectId,omitempty"`

	// Text: The text to be inserted.
	//
	// Inserting a newline character will implicitly create a
	// new
	// ParagraphMarker at that index.
	// The paragraph style of the new paragraph will be copied from the
	// paragraph
	// at the current insertion index, including lists and bullets.
	//
	// Text styles for inserted text will be determined automatically,
	// generally
	// preserving the styling of neighboring text. In most cases, the text
	// will be
	// added to the TextRun that exists at the
	// insertion index.
	//
	// Some control characters (U+0000-U+0008, U+000C-U+001F) and
	// characters
	// from the Unicode Basic Multilingual Plane Private Use Area
	// (U+E000-U+F8FF)
	// will be stripped out of the inserted text.
	Text string `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CellLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CellLocation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *InsertTextRequest) MarshalJSON() ([]byte, error) {
	type noMethod InsertTextRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LayoutProperties: The properties of Page are only
// relevant for pages with page_type LAYOUT.
type LayoutProperties struct {
	// DisplayName: The human readable name of the layout in the
	// presentation's locale.
	DisplayName string `json:"displayName,omitempty"`

	// MasterObjectId: The object ID of the master that this layout is based
	// on.
	MasterObjectId string `json:"masterObjectId,omitempty"`

	// Name: The name of the layout.
	Name string `json:"name,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DisplayName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DisplayName") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LayoutProperties) MarshalJSON() ([]byte, error) {
	type noMethod LayoutProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LayoutReference: Slide layout reference. This may reference
// either:
//
// - A predefined layout, or
// - One of the layouts in the presentation.
type LayoutReference struct {
	// LayoutId: Layout ID: the object ID of one of the layouts in the
	// presentation.
	LayoutId string `json:"layoutId,omitempty"`

	// PredefinedLayout: Predefined layout.
	//
	// Possible values:
	//   "PREDEFINED_LAYOUT_UNSPECIFIED" - Unspecified layout.
	//   "BLANK" - Blank layout.
	//   "CAPTION_ONLY" - Layout with a caption.
	//   "TITLE" - Layout with a title placeholder.
	//   "TITLE_AND_BODY" - Layout with a title and body placeholder.
	//   "TITLE_AND_TWO_COLUMNS" - Layout with a title and two columns.
	//   "TITLE_ONLY" - Layout with only a title.
	PredefinedLayout string `json:"predefinedLayout,omitempty"`

	// ForceSendFields is a list of field names (e.g. "LayoutId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LayoutId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LayoutReference) MarshalJSON() ([]byte, error) {
	type noMethod LayoutReference
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Line: A PageElement kind representing a
// line, curved connector, or bent connector.
type Line struct {
	// LineProperties: The properties of the line.
	LineProperties *LineProperties `json:"lineProperties,omitempty"`

	// LineType: The type of the line.
	//
	// Possible values:
	//   "TYPE_UNSPECIFIED" - An unspecified line type.
	//   "STRAIGHT_CONNECTOR_1" - Straight connector 1 form. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'straightConnector1'.
	//   "BENT_CONNECTOR_2" - Bent connector 2 form. Corresponds to ECMA-376
	// ST_ShapeType
	// 'bentConnector2'.
	//   "BENT_CONNECTOR_3" - Bent connector 3 form. Corresponds to ECMA-376
	// ST_ShapeType
	// 'bentConnector3'.
	//   "BENT_CONNECTOR_4" - Bent connector 4 form. Corresponds to ECMA-376
	// ST_ShapeType
	// 'bentConnector4'.
	//   "BENT_CONNECTOR_5" - Bent connector 5 form. Corresponds to ECMA-376
	// ST_ShapeType
	// 'bentConnector5'.
	//   "CURVED_CONNECTOR_2" - Curved connector 2 form. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedConnector2'.
	//   "CURVED_CONNECTOR_3" - Curved connector 3 form. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedConnector3'.
	//   "CURVED_CONNECTOR_4" - Curved connector 4 form. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedConnector4'.
	//   "CURVED_CONNECTOR_5" - Curved connector 5 form. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedConnector5'.
	LineType string `json:"lineType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "LineProperties") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LineProperties") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Line) MarshalJSON() ([]byte, error) {
	type noMethod Line
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LineFill: The fill of the line.
type LineFill struct {
	// SolidFill: Solid color fill.
	SolidFill *SolidFill `json:"solidFill,omitempty"`

	// ForceSendFields is a list of field names (e.g. "SolidFill") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "SolidFill") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LineFill) MarshalJSON() ([]byte, error) {
	type noMethod LineFill
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LineProperties: The properties of the Line.
//
// When unset, these fields default to values that match the appearance
// of
// new lines created in the Slides editor.
type LineProperties struct {
	// DashStyle: The dash style of the line.
	//
	// Possible values:
	//   "DASH_STYLE_UNSPECIFIED" - Unspecified dash style.
	//   "SOLID" - Solid line. Corresponds to ECMA-376 ST_PresetLineDashVal
	// value 'solid'.
	// This is the default dash style.
	//   "DOT" - Dotted line. Corresponds to ECMA-376 ST_PresetLineDashVal
	// value 'dot'.
	//   "DASH" - Dashed line. Corresponds to ECMA-376 ST_PresetLineDashVal
	// value 'dash'.
	//   "DASH_DOT" - Alternating dashes and dots. Corresponds to ECMA-376
	// ST_PresetLineDashVal
	// value 'dashDot'.
	//   "LONG_DASH" - Line with large dashes. Corresponds to ECMA-376
	// ST_PresetLineDashVal
	// value 'lgDash'.
	//   "LONG_DASH_DOT" - Alternating large dashes and dots. Corresponds to
	// ECMA-376
	// ST_PresetLineDashVal value 'lgDashDot'.
	DashStyle string `json:"dashStyle,omitempty"`

	// EndArrow: The style of the arrow at the end of the line.
	//
	// Possible values:
	//   "ARROW_STYLE_UNSPECIFIED" - An unspecified arrow style.
	//   "NONE" - No arrow.
	//   "STEALTH_ARROW" - Arrow with notched back. Corresponds to ECMA-376
	// ST_LineEndType value
	// 'stealth'.
	//   "FILL_ARROW" - Filled arrow. Corresponds to ECMA-376 ST_LineEndType
	// value 'triangle'.
	//   "FILL_CIRCLE" - Filled circle. Corresponds to ECMA-376
	// ST_LineEndType value 'oval'.
	//   "FILL_SQUARE" - Filled square.
	//   "FILL_DIAMOND" - Filled diamond. Corresponds to ECMA-376
	// ST_LineEndType value 'diamond'.
	//   "OPEN_ARROW" - Hollow arrow.
	//   "OPEN_CIRCLE" - Hollow circle.
	//   "OPEN_SQUARE" - Hollow square.
	//   "OPEN_DIAMOND" - Hollow diamond.
	EndArrow string `json:"endArrow,omitempty"`

	// LineFill: The fill of the line. The default line fill matches the
	// defaults for new
	// lines created in the Slides editor.
	LineFill *LineFill `json:"lineFill,omitempty"`

	// StartArrow: The style of the arrow at the beginning of the line.
	//
	// Possible values:
	//   "ARROW_STYLE_UNSPECIFIED" - An unspecified arrow style.
	//   "NONE" - No arrow.
	//   "STEALTH_ARROW" - Arrow with notched back. Corresponds to ECMA-376
	// ST_LineEndType value
	// 'stealth'.
	//   "FILL_ARROW" - Filled arrow. Corresponds to ECMA-376 ST_LineEndType
	// value 'triangle'.
	//   "FILL_CIRCLE" - Filled circle. Corresponds to ECMA-376
	// ST_LineEndType value 'oval'.
	//   "FILL_SQUARE" - Filled square.
	//   "FILL_DIAMOND" - Filled diamond. Corresponds to ECMA-376
	// ST_LineEndType value 'diamond'.
	//   "OPEN_ARROW" - Hollow arrow.
	//   "OPEN_CIRCLE" - Hollow circle.
	//   "OPEN_SQUARE" - Hollow square.
	//   "OPEN_DIAMOND" - Hollow diamond.
	StartArrow string `json:"startArrow,omitempty"`

	// Weight: The thickness of the line.
	Weight *Dimension `json:"weight,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DashStyle") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DashStyle") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LineProperties) MarshalJSON() ([]byte, error) {
	type noMethod LineProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Link: A hypertext link.
type Link struct {
	// PageObjectId: If set, indicates this is a link to the specific page
	// in this
	// presentation with this ID. A page with this ID may not exist.
	PageObjectId string `json:"pageObjectId,omitempty"`

	// RelativeLink: If set, indicates this is a link to a slide in this
	// presentation,
	// addressed by its position.
	//
	// Possible values:
	//   "RELATIVE_SLIDE_LINK_UNSPECIFIED" - An unspecified relative slide
	// link.
	//   "NEXT_SLIDE" - A link to the next slide.
	//   "PREVIOUS_SLIDE" - A link to the previous slide.
	//   "FIRST_SLIDE" - A link to the first slide in the presentation.
	//   "LAST_SLIDE" - A link to the last slide in the presentation.
	RelativeLink string `json:"relativeLink,omitempty"`

	// SlideIndex: If set, indicates this is a link to the slide at this
	// zero-based index
	// in the presentation. There may not be a slide at this index.
	SlideIndex int64 `json:"slideIndex,omitempty"`

	// Url: If set, indicates this is a link to the external web page at
	// this URL.
	Url string `json:"url,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PageObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PageObjectId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Link) MarshalJSON() ([]byte, error) {
	type noMethod Link
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// List: A List describes the look and feel of bullets belonging to
// paragraphs
// associated with a list. A paragraph that is part of a list has an
// implicit
// reference to that list's ID.
type List struct {
	// ListId: The ID of the list.
	ListId string `json:"listId,omitempty"`

	// NestingLevel: A map of nesting levels to the properties of bullets at
	// the associated
	// level. A list has at most nine levels of nesting, so the possible
	// values
	// for the keys of this map are 0 through 8, inclusive.
	NestingLevel map[string]NestingLevel `json:"nestingLevel,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ListId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ListId") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *List) MarshalJSON() ([]byte, error) {
	type noMethod List
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// NestingLevel: Contains properties describing the look and feel of a
// list bullet at a given
// level of nesting.
type NestingLevel struct {
	// BulletStyle: The style of a bullet at this level of nesting.
	BulletStyle *TextStyle `json:"bulletStyle,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BulletStyle") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BulletStyle") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *NestingLevel) MarshalJSON() ([]byte, error) {
	type noMethod NestingLevel
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OpaqueColor: A themeable solid color value.
type OpaqueColor struct {
	// RgbColor: An opaque RGB color.
	RgbColor *RgbColor `json:"rgbColor,omitempty"`

	// ThemeColor: An opaque theme color.
	//
	// Possible values:
	//   "THEME_COLOR_TYPE_UNSPECIFIED" - Unspecified theme color. This
	// value should not be used.
	//   "DARK1" - Represents the first dark color.
	//   "LIGHT1" - Represents the first light color.
	//   "DARK2" - Represents the second dark color.
	//   "LIGHT2" - Represents the second light color.
	//   "ACCENT1" - Represents the first accent color.
	//   "ACCENT2" - Represents the second accent color.
	//   "ACCENT3" - Represents the third accent color.
	//   "ACCENT4" - Represents the fourth accent color.
	//   "ACCENT5" - Represents the fifth accent color.
	//   "ACCENT6" - Represents the sixth accent color.
	//   "HYPERLINK" - Represents the color to use for hyperlinks.
	//   "FOLLOWED_HYPERLINK" - Represents the color to use for visited
	// hyperlinks.
	//   "TEXT1" - Represents the first text color.
	//   "BACKGROUND1" - Represents the first background color.
	//   "TEXT2" - Represents the second text color.
	//   "BACKGROUND2" - Represents the second background color.
	ThemeColor string `json:"themeColor,omitempty"`

	// ForceSendFields is a list of field names (e.g. "RgbColor") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "RgbColor") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OpaqueColor) MarshalJSON() ([]byte, error) {
	type noMethod OpaqueColor
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OptionalColor: A color that can either be fully opaque or fully
// transparent.
type OptionalColor struct {
	// OpaqueColor: If set, this will be used as an opaque color. If unset,
	// this represents
	// a transparent color.
	OpaqueColor *OpaqueColor `json:"opaqueColor,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OpaqueColor") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OpaqueColor") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OptionalColor) MarshalJSON() ([]byte, error) {
	type noMethod OptionalColor
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Outline: The outline of a PageElement.
//
// If these fields are unset, they may be inherited from a parent
// placeholder
// if it exists. If there is no parent, the fields will default to the
// value
// used for new page elements created in the Slides editor, which may
// depend on
// the page element kind.
type Outline struct {
	// DashStyle: The dash style of the outline.
	//
	// Possible values:
	//   "DASH_STYLE_UNSPECIFIED" - Unspecified dash style.
	//   "SOLID" - Solid line. Corresponds to ECMA-376 ST_PresetLineDashVal
	// value 'solid'.
	// This is the default dash style.
	//   "DOT" - Dotted line. Corresponds to ECMA-376 ST_PresetLineDashVal
	// value 'dot'.
	//   "DASH" - Dashed line. Corresponds to ECMA-376 ST_PresetLineDashVal
	// value 'dash'.
	//   "DASH_DOT" - Alternating dashes and dots. Corresponds to ECMA-376
	// ST_PresetLineDashVal
	// value 'dashDot'.
	//   "LONG_DASH" - Line with large dashes. Corresponds to ECMA-376
	// ST_PresetLineDashVal
	// value 'lgDash'.
	//   "LONG_DASH_DOT" - Alternating large dashes and dots. Corresponds to
	// ECMA-376
	// ST_PresetLineDashVal value 'lgDashDot'.
	DashStyle string `json:"dashStyle,omitempty"`

	// OutlineFill: The fill of the outline.
	OutlineFill *OutlineFill `json:"outlineFill,omitempty"`

	// PropertyState: The outline property state.
	//
	// Updating the the outline on a page element will implicitly update
	// this
	// field to`RENDERED`, unless another value is specified in the same
	// request.
	// To have no outline on a page element, set this field to
	// `NOT_RENDERED`. In
	// this case, any other outline fields set in the same request will
	// be
	// ignored.
	//
	// Possible values:
	//   "RENDERED" - If a property's state is RENDERED, then the element
	// has the corresponding
	// property when rendered on a page. If the element is a placeholder
	// shape as
	// determined by the placeholder
	// field, and it inherits from a placeholder shape, the corresponding
	// field
	// may be unset, meaning that the property value is inherited from a
	// parent
	// placeholder. If the element does not inherit, then the field will
	// contain
	// the rendered value. This is the default value.
	//   "NOT_RENDERED" - If a property's state is NOT_RENDERED, then the
	// element does not have the
	// corresponding property when rendered on a page. However, the field
	// may
	// still be set so it can be inherited by child shapes. To remove a
	// property
	// from a rendered element, set its property_state to NOT_RENDERED.
	//   "INHERIT" - If a property's state is INHERIT, then the property
	// state uses the value of
	// corresponding `property_state` field on the parent shape. Elements
	// that do
	// not inherit will never have an INHERIT property state.
	PropertyState string `json:"propertyState,omitempty"`

	// Weight: The thickness of the outline.
	Weight *Dimension `json:"weight,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DashStyle") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DashStyle") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Outline) MarshalJSON() ([]byte, error) {
	type noMethod Outline
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// OutlineFill: The fill of the outline.
type OutlineFill struct {
	// SolidFill: Solid color fill.
	SolidFill *SolidFill `json:"solidFill,omitempty"`

	// ForceSendFields is a list of field names (e.g. "SolidFill") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "SolidFill") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OutlineFill) MarshalJSON() ([]byte, error) {
	type noMethod OutlineFill
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Page: A page in a presentation.
type Page struct {
	// LayoutProperties: Layout specific properties. Only set if page_type =
	// LAYOUT.
	LayoutProperties *LayoutProperties `json:"layoutProperties,omitempty"`

	// ObjectId: The object ID for this page. Object IDs used by
	// Page and
	// PageElement share the same namespace.
	ObjectId string `json:"objectId,omitempty"`

	// PageElements: The page elements rendered on the page.
	PageElements []*PageElement `json:"pageElements,omitempty"`

	// PageProperties: The properties of the page.
	PageProperties *PageProperties `json:"pageProperties,omitempty"`

	// PageType: The type of the page.
	//
	// Possible values:
	//   "SLIDE" - A slide page.
	//   "MASTER" - A master slide page.
	//   "LAYOUT" - A layout page.
	PageType string `json:"pageType,omitempty"`

	// SlideProperties: Slide specific properties. Only set if page_type =
	// SLIDE.
	SlideProperties *SlideProperties `json:"slideProperties,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "LayoutProperties") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LayoutProperties") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Page) MarshalJSON() ([]byte, error) {
	type noMethod Page
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PageBackgroundFill: The page background fill.
type PageBackgroundFill struct {
	// PropertyState: The background fill property state.
	//
	// Updating the the fill on a page will implicitly update this field
	// to
	// `RENDERED`, unless another value is specified in the same request.
	// To
	// have no fill on a page, set this field to `NOT_RENDERED`. In this
	// case,
	// any other fill fields set in the same request will be ignored.
	//
	// Possible values:
	//   "RENDERED" - If a property's state is RENDERED, then the element
	// has the corresponding
	// property when rendered on a page. If the element is a placeholder
	// shape as
	// determined by the placeholder
	// field, and it inherits from a placeholder shape, the corresponding
	// field
	// may be unset, meaning that the property value is inherited from a
	// parent
	// placeholder. If the element does not inherit, then the field will
	// contain
	// the rendered value. This is the default value.
	//   "NOT_RENDERED" - If a property's state is NOT_RENDERED, then the
	// element does not have the
	// corresponding property when rendered on a page. However, the field
	// may
	// still be set so it can be inherited by child shapes. To remove a
	// property
	// from a rendered element, set its property_state to NOT_RENDERED.
	//   "INHERIT" - If a property's state is INHERIT, then the property
	// state uses the value of
	// corresponding `property_state` field on the parent shape. Elements
	// that do
	// not inherit will never have an INHERIT property state.
	PropertyState string `json:"propertyState,omitempty"`

	// SolidFill: Solid color fill.
	SolidFill *SolidFill `json:"solidFill,omitempty"`

	// StretchedPictureFill: Stretched picture fill.
	StretchedPictureFill *StretchedPictureFill `json:"stretchedPictureFill,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PropertyState") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PropertyState") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PageBackgroundFill) MarshalJSON() ([]byte, error) {
	type noMethod PageBackgroundFill
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PageElement: A visual element rendered on a page.
type PageElement struct {
	// Description: The description of the page element. Combined with title
	// to display alt
	// text.
	Description string `json:"description,omitempty"`

	// ElementGroup: A collection of page elements joined as a single unit.
	ElementGroup *Group `json:"elementGroup,omitempty"`

	// Image: An image page element.
	Image *Image `json:"image,omitempty"`

	// Line: A line page element.
	Line *Line `json:"line,omitempty"`

	// ObjectId: The object ID for this page element. Object IDs used
	// by
	// google.apps.slides.v1.Page and
	// google.apps.slides.v1.PageElement share the same namespace.
	ObjectId string `json:"objectId,omitempty"`

	// Shape: A generic shape.
	Shape *Shape `json:"shape,omitempty"`

	// SheetsChart: A linked chart embedded from Google Sheets. Unlinked
	// charts are
	// represented as images.
	SheetsChart *SheetsChart `json:"sheetsChart,omitempty"`

	// Size: The size of the page element.
	Size *Size `json:"size,omitempty"`

	// Table: A table page element.
	Table *Table `json:"table,omitempty"`

	// Title: The title of the page element. Combined with description to
	// display alt
	// text.
	Title string `json:"title,omitempty"`

	// Transform: The transform of the page element.
	Transform *AffineTransform `json:"transform,omitempty"`

	// Video: A video page element.
	Video *Video `json:"video,omitempty"`

	// WordArt: A word art page element.
	WordArt *WordArt `json:"wordArt,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Description") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Description") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PageElement) MarshalJSON() ([]byte, error) {
	type noMethod PageElement
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PageElementProperties: Common properties for a page element.
//
// Note: When you initially create a
// PageElement, the API may modify
// the values of both `size` and `transform`, but the
// visual size will be unchanged.
type PageElementProperties struct {
	// PageObjectId: The object ID of the page where the element is located.
	PageObjectId string `json:"pageObjectId,omitempty"`

	// Size: The size of the element.
	Size *Size `json:"size,omitempty"`

	// Transform: The transform for the element.
	Transform *AffineTransform `json:"transform,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PageObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PageObjectId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PageElementProperties) MarshalJSON() ([]byte, error) {
	type noMethod PageElementProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// PageProperties: The properties of the Page.
//
// The page will inherit properties from the parent page. Depending on
// the page
// type the hierarchy is defined in either
// SlideProperties or
// LayoutProperties.
type PageProperties struct {
	// ColorScheme: The color scheme of the page. If unset, the color scheme
	// is inherited from
	// a parent page. If the page has no parent, the color scheme uses a
	// default
	// Slides color scheme. This field is read-only.
	ColorScheme *ColorScheme `json:"colorScheme,omitempty"`

	// PageBackgroundFill: The background fill of the page. If unset, the
	// background fill is inherited
	// from a parent page if it exists. If the page has no parent, then
	// the
	// background fill defaults to the corresponding fill in the Slides
	// editor.
	PageBackgroundFill *PageBackgroundFill `json:"pageBackgroundFill,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ColorScheme") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ColorScheme") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *PageProperties) MarshalJSON() ([]byte, error) {
	type noMethod PageProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ParagraphMarker: A TextElement kind that represents the beginning of
// a new paragraph.
type ParagraphMarker struct {
	// Bullet: The bullet for this paragraph. If not present, the paragraph
	// does not
	// belong to a list.
	Bullet *Bullet `json:"bullet,omitempty"`

	// Style: The paragraph's style
	Style *ParagraphStyle `json:"style,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Bullet") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Bullet") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ParagraphMarker) MarshalJSON() ([]byte, error) {
	type noMethod ParagraphMarker
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ParagraphStyle: Styles that apply to a whole paragraph.
//
// If this text is contained in a shape with a parent placeholder, then
// these paragraph styles may be
// inherited from the parent. Which paragraph styles are inherited
// depend on the
// nesting level of lists:
//
// * A paragraph not in a list will inherit its paragraph style from
// the
//   paragraph at the 0 nesting level of the list inside the parent
// placeholder.
// * A paragraph in a list will inherit its paragraph style from the
// paragraph
//   at its corresponding nesting level of the list inside the parent
//   placeholder.
//
// Inherited paragraph styles are represented as unset fields in this
// message.
type ParagraphStyle struct {
	// Alignment: The text alignment for this paragraph. This property is
	// read-only.
	//
	// Possible values:
	//   "ALIGNMENT_UNSPECIFIED" - The paragraph alignment is inherited from
	// the parent.
	//   "START" - The paragraph is aligned to the start of the line.
	// Left-aligned for
	// LTR text, right-aligned otherwise.
	//   "CENTER" - The paragraph is centered.
	//   "END" - The paragraph is aligned to the end of the line.
	// Right-aligned for
	// LTR text, left-aligned otherwise.
	//   "JUSTIFIED" - The paragraph is justified.
	Alignment string `json:"alignment,omitempty"`

	// Direction: The text direction of this paragraph. This property is
	// read-only.
	//
	// Possible values:
	//   "TEXT_DIRECTION_UNSPECIFIED" - The text direction is inherited from
	// the parent.
	//   "LEFT_TO_RIGHT" - The text goes from left to right.
	//   "RIGHT_TO_LEFT" - The text goes from right to left.
	Direction string `json:"direction,omitempty"`

	// IndentEnd: The amount indentation for the paragraph on the side that
	// corresponds to
	// the end of the text, based on the current text direction. If unset,
	// the
	// value is inherited from the parent. This property is read-only.
	IndentEnd *Dimension `json:"indentEnd,omitempty"`

	// IndentFirstLine: The amount of indentation for the start of the first
	// line of the paragraph.
	// If unset, the value is inherited from the parent. This property
	// is
	// read-only.
	IndentFirstLine *Dimension `json:"indentFirstLine,omitempty"`

	// IndentStart: The amount indentation for the paragraph on the side
	// that corresponds to
	// the start of the text, based on the current text direction. If unset,
	// the
	// value is inherited from the parent. This property is read-only.
	IndentStart *Dimension `json:"indentStart,omitempty"`

	// LineSpacing: The amount of space between lines, as a percentage of
	// normal, where normal
	// is represented as 100.0. If unset, the value is inherited from the
	// parent.
	// This property is read-only.
	LineSpacing float64 `json:"lineSpacing,omitempty"`

	// SpaceAbove: The amount of extra space above the paragraph. If unset,
	// the value is
	// inherited from the parent. This property is read-only.
	SpaceAbove *Dimension `json:"spaceAbove,omitempty"`

	// SpaceBelow: The amount of extra space above the paragraph. If unset,
	// the value is
	// inherited from the parent. This property is read-only.
	SpaceBelow *Dimension `json:"spaceBelow,omitempty"`

	// SpacingMode: The spacing mode for the paragraph. This property is
	// read-only.
	//
	// Possible values:
	//   "SPACING_MODE_UNSPECIFIED" - The spacing mode is inherited from the
	// parent.
	//   "NEVER_COLLAPSE" - Paragraph spacing is always rendered.
	//   "COLLAPSE_LISTS" - Paragraph spacing is skipped between list
	// elements.
	SpacingMode string `json:"spacingMode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Alignment") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alignment") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ParagraphStyle) MarshalJSON() ([]byte, error) {
	type noMethod ParagraphStyle
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Placeholder: The placeholder information that uniquely identifies a
// placeholder shape.
type Placeholder struct {
	// Index: The index of the placeholder. If the same placeholder types
	// are the present
	// in the same page, they would have different index values.
	Index int64 `json:"index,omitempty"`

	// ParentObjectId: The object ID of this shape's parent placeholder.
	// If unset, the parent placeholder shape does not exist, so the shape
	// does
	// not inherit properties from any other shape.
	ParentObjectId string `json:"parentObjectId,omitempty"`

	// Type: The type of the placeholder.
	//
	// Possible values:
	//   "NONE" - Default value, signifies it is not a placeholder.
	//   "BODY" - Body text.
	//   "CHART" - Chart or graph.
	//   "CLIP_ART" - Clip art image.
	//   "CENTERED_TITLE" - Title centered.
	//   "DIAGRAM" - Diagram.
	//   "DATE_AND_TIME" - Date and time.
	//   "FOOTER" - Footer text.
	//   "HEADER" - Header text.
	//   "MEDIA" - Multimedia.
	//   "OBJECT" - Any content type.
	//   "PICTURE" - Picture.
	//   "SLIDE_NUMBER" - Number of a slide.
	//   "SUBTITLE" - Subtitle.
	//   "TABLE" - Table.
	//   "TITLE" - Slide title.
	//   "SLIDE_IMAGE" - Slide image.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Index") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Index") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Placeholder) MarshalJSON() ([]byte, error) {
	type noMethod Placeholder
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Presentation: A Google Slides presentation.
type Presentation struct {
	// Layouts: The layouts in the presentation.  A layout is a template
	// that determines
	// how content is arranged and styled on the slides that inherit from
	// that
	// layout.
	Layouts []*Page `json:"layouts,omitempty"`

	// Locale: The locale of the presentation, as an IETF BCP 47 language
	// tag.
	Locale string `json:"locale,omitempty"`

	// Masters: The slide masters in the presentation. A slide master
	// contains all common
	// page elements and the common properties for a set of layouts. They
	// serve
	// three purposes:
	//
	// - Placeholder shapes on a master contain the default text styles and
	// shape
	//   properties of all placeholder shapes on pages that use that
	// master.
	// - The master page properties define the common page properties
	// inherited by
	//   its layouts.
	// - Any other shapes on the master slide will appear on all slides
	// using that
	//   master, regardless of their layout.
	Masters []*Page `json:"masters,omitempty"`

	// PageSize: The size of pages in the presentation.
	PageSize *Size `json:"pageSize,omitempty"`

	// PresentationId: The ID of the presentation.
	PresentationId string `json:"presentationId,omitempty"`

	// Slides: The slides in the presentation.
	// A slide inherits properties from a slide layout.
	Slides []*Page `json:"slides,omitempty"`

	// Title: The title of the presentation.
	Title string `json:"title,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Layouts") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Layouts") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Presentation) MarshalJSON() ([]byte, error) {
	type noMethod Presentation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Range: Specifies a contiguous range of an indexed collection, such as
// characters in
// text.
type Range struct {
	// EndIndex: The optional zero-based index of the end of the
	// collection.
	// Required for `SPECIFIC_RANGE` delete mode.
	EndIndex int64 `json:"endIndex,omitempty"`

	// StartIndex: The optional zero-based index of the beginning of the
	// collection.
	// Required for `SPECIFIC_RANGE` and `FROM_START_INDEX` ranges.
	StartIndex int64 `json:"startIndex,omitempty"`

	// Type: The type of range.
	//
	// Possible values:
	//   "RANGE_TYPE_UNSPECIFIED" - Unspecified range type. This value must
	// not be used.
	//   "FIXED_RANGE" - A fixed range. Both the start_index and end_index
	// must be specified.
	//   "FROM_START_INDEX" - Starts the range at start_index and continues
	// until the end of the
	// collection. The end_index must not be specified.
	//   "ALL" - Sets the range to be the whole length of the collection.
	// Both the
	// start_index and the end_index must not be specified.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EndIndex") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EndIndex") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Range) MarshalJSON() ([]byte, error) {
	type noMethod Range
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Recolor: A recolor effect applied on an image.
type Recolor struct {
	// RecolorStops: The recolor effect is represented by a gradient, which
	// is a list of color
	// stops. This property is read-only.
	RecolorStops []*ColorStop `json:"recolorStops,omitempty"`

	// ForceSendFields is a list of field names (e.g. "RecolorStops") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "RecolorStops") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Recolor) MarshalJSON() ([]byte, error) {
	type noMethod Recolor
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RefreshSheetsChartRequest: Refreshes an embedded Google Sheets chart
// by replacing it with the latest
// version of the chart from Google Sheets.
//
// NOTE: Refreshing charts requires  at least one of the
// spreadsheets.readonly,
// spreadsheets, drive.readonly, or drive OAuth scopes.
type RefreshSheetsChartRequest struct {
	// ObjectId: The object ID of the chart to refresh.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ObjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RefreshSheetsChartRequest) MarshalJSON() ([]byte, error) {
	type noMethod RefreshSheetsChartRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReplaceAllShapesWithImageRequest: Replaces all shapes that match the
// given criteria with the provided image.
type ReplaceAllShapesWithImageRequest struct {
	// ContainsText: If set, this request will replace all of the shapes
	// that contain the
	// given text.
	ContainsText *SubstringMatchCriteria `json:"containsText,omitempty"`

	// ImageUrl: The image URL.
	//
	// The image is fetched once at insertion time and a copy is stored
	// for
	// display inside the presentation. Images must be less than 50MB in
	// size,
	// cannot exceed 25 megapixels, and must be in either in PNG, JPEG, or
	// GIF
	// format.
	ImageUrl string `json:"imageUrl,omitempty"`

	// ReplaceMethod: The replace method.
	//
	// Possible values:
	//   "CENTER_INSIDE" - Scales and centers the image to fit within the
	// bounds of the original
	// shape and maintains the image's aspect ratio. The rendered size of
	// the
	// image may be smaller than the size of the shape. This is the
	// default
	// method when one is not specified.
	//   "CENTER_CROP" - Scales and centers the image to fill the bounds of
	// the original shape.
	// The image may be cropped in order to fill the shape. The rendered
	// size of
	// the image will be the same as that of the original shape.
	ReplaceMethod string `json:"replaceMethod,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContainsText") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContainsText") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReplaceAllShapesWithImageRequest) MarshalJSON() ([]byte, error) {
	type noMethod ReplaceAllShapesWithImageRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReplaceAllShapesWithImageResponse: The result of replacing shapes
// with an image.
type ReplaceAllShapesWithImageResponse struct {
	// OccurrencesChanged: The number of shapes replaced with images.
	OccurrencesChanged int64 `json:"occurrencesChanged,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OccurrencesChanged")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OccurrencesChanged") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ReplaceAllShapesWithImageResponse) MarshalJSON() ([]byte, error) {
	type noMethod ReplaceAllShapesWithImageResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReplaceAllTextRequest: Replaces all instances of text matching a
// criteria with replace text.
type ReplaceAllTextRequest struct {
	// ContainsText: Finds text in a shape matching this substring.
	ContainsText *SubstringMatchCriteria `json:"containsText,omitempty"`

	// ReplaceText: The text that will replace the matched text.
	ReplaceText string `json:"replaceText,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContainsText") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContainsText") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ReplaceAllTextRequest) MarshalJSON() ([]byte, error) {
	type noMethod ReplaceAllTextRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReplaceAllTextResponse: The result of replacing text.
type ReplaceAllTextResponse struct {
	// OccurrencesChanged: The number of occurrences changed by replacing
	// all text.
	OccurrencesChanged int64 `json:"occurrencesChanged,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OccurrencesChanged")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OccurrencesChanged") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ReplaceAllTextResponse) MarshalJSON() ([]byte, error) {
	type noMethod ReplaceAllTextResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Request: A single kind of update to apply to a presentation.
type Request struct {
	// CreateImage: Creates an image.
	CreateImage *CreateImageRequest `json:"createImage,omitempty"`

	// CreateLine: Creates a line.
	CreateLine *CreateLineRequest `json:"createLine,omitempty"`

	// CreateParagraphBullets: Creates bullets for paragraphs.
	CreateParagraphBullets *CreateParagraphBulletsRequest `json:"createParagraphBullets,omitempty"`

	// CreateShape: Creates a new shape.
	CreateShape *CreateShapeRequest `json:"createShape,omitempty"`

	// CreateSheetsChart: Creates an embedded Google Sheets chart.
	CreateSheetsChart *CreateSheetsChartRequest `json:"createSheetsChart,omitempty"`

	// CreateSlide: Creates a new slide.
	CreateSlide *CreateSlideRequest `json:"createSlide,omitempty"`

	// CreateTable: Creates a new table.
	CreateTable *CreateTableRequest `json:"createTable,omitempty"`

	// CreateVideo: Creates a video.
	CreateVideo *CreateVideoRequest `json:"createVideo,omitempty"`

	// DeleteObject: Deletes a page or page element from the presentation.
	DeleteObject *DeleteObjectRequest `json:"deleteObject,omitempty"`

	// DeleteTableColumn: Deletes a column from a table.
	DeleteTableColumn *DeleteTableColumnRequest `json:"deleteTableColumn,omitempty"`

	// DeleteTableRow: Deletes a row from a table.
	DeleteTableRow *DeleteTableRowRequest `json:"deleteTableRow,omitempty"`

	// DeleteText: Deletes text from a shape or a table cell.
	DeleteText *DeleteTextRequest `json:"deleteText,omitempty"`

	// DuplicateObject: Duplicates a slide or page element.
	DuplicateObject *DuplicateObjectRequest `json:"duplicateObject,omitempty"`

	// InsertTableColumns: Inserts columns into a table.
	InsertTableColumns *InsertTableColumnsRequest `json:"insertTableColumns,omitempty"`

	// InsertTableRows: Inserts rows into a table.
	InsertTableRows *InsertTableRowsRequest `json:"insertTableRows,omitempty"`

	// InsertText: Inserts text into a shape or table cell.
	InsertText *InsertTextRequest `json:"insertText,omitempty"`

	// RefreshSheetsChart: Refreshes a Google Sheets chart.
	RefreshSheetsChart *RefreshSheetsChartRequest `json:"refreshSheetsChart,omitempty"`

	// ReplaceAllShapesWithImage: Replaces all shapes matching some criteria
	// with an image.
	ReplaceAllShapesWithImage *ReplaceAllShapesWithImageRequest `json:"replaceAllShapesWithImage,omitempty"`

	// ReplaceAllText: Replaces all instances of specified text.
	ReplaceAllText *ReplaceAllTextRequest `json:"replaceAllText,omitempty"`

	// UpdateImageProperties: Updates the properties of an Image.
	UpdateImageProperties *UpdateImagePropertiesRequest `json:"updateImageProperties,omitempty"`

	// UpdatePageElementTransform: Updates the transform of a page element.
	UpdatePageElementTransform *UpdatePageElementTransformRequest `json:"updatePageElementTransform,omitempty"`

	// UpdatePageProperties: Updates the properties of a Page.
	UpdatePageProperties *UpdatePagePropertiesRequest `json:"updatePageProperties,omitempty"`

	// UpdateShapeProperties: Updates the properties of a Shape.
	UpdateShapeProperties *UpdateShapePropertiesRequest `json:"updateShapeProperties,omitempty"`

	// UpdateSlidesPosition: Updates the position of a set of slides in the
	// presentation.
	UpdateSlidesPosition *UpdateSlidesPositionRequest `json:"updateSlidesPosition,omitempty"`

	// UpdateTableCellProperties: Updates the properties of a TableCell.
	UpdateTableCellProperties *UpdateTableCellPropertiesRequest `json:"updateTableCellProperties,omitempty"`

	// UpdateTextStyle: Updates the styling of text within a Shape or Table.
	UpdateTextStyle *UpdateTextStyleRequest `json:"updateTextStyle,omitempty"`

	// UpdateVideoProperties: Updates the properties of a Video.
	UpdateVideoProperties *UpdateVideoPropertiesRequest `json:"updateVideoProperties,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CreateImage") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreateImage") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Request) MarshalJSON() ([]byte, error) {
	type noMethod Request
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Response: A single response from an update.
type Response struct {
	// CreateImage: The result of creating an image.
	CreateImage *CreateImageResponse `json:"createImage,omitempty"`

	// CreateLine: The result of creating a line.
	CreateLine *CreateLineResponse `json:"createLine,omitempty"`

	// CreateShape: The result of creating a shape.
	CreateShape *CreateShapeResponse `json:"createShape,omitempty"`

	// CreateSheetsChart: The result of creating a Google Sheets chart.
	CreateSheetsChart *CreateSheetsChartResponse `json:"createSheetsChart,omitempty"`

	// CreateSlide: The result of creating a slide.
	CreateSlide *CreateSlideResponse `json:"createSlide,omitempty"`

	// CreateTable: The result of creating a table.
	CreateTable *CreateTableResponse `json:"createTable,omitempty"`

	// CreateVideo: The result of creating a video.
	CreateVideo *CreateVideoResponse `json:"createVideo,omitempty"`

	// DuplicateObject: The result of duplicating an object.
	DuplicateObject *DuplicateObjectResponse `json:"duplicateObject,omitempty"`

	// ReplaceAllShapesWithImage: The result of replacing all shapes
	// containing the specified text with
	// an image.
	ReplaceAllShapesWithImage *ReplaceAllShapesWithImageResponse `json:"replaceAllShapesWithImage,omitempty"`

	// ReplaceAllText: The result of replacing text.
	ReplaceAllText *ReplaceAllTextResponse `json:"replaceAllText,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CreateImage") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreateImage") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Response) MarshalJSON() ([]byte, error) {
	type noMethod Response
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RgbColor: An RGB color.
type RgbColor struct {
	// Blue: The blue component of the color, from 0.0 to 1.0.
	Blue float64 `json:"blue,omitempty"`

	// Green: The green component of the color, from 0.0 to 1.0.
	Green float64 `json:"green,omitempty"`

	// Red: The red component of the color, from 0.0 to 1.0.
	Red float64 `json:"red,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Blue") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Blue") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RgbColor) MarshalJSON() ([]byte, error) {
	type noMethod RgbColor
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Shadow: The shadow properties of a page element.
//
// If these fields are unset, they may be inherited from a parent
// placeholder
// if it exists. If there is no parent, the fields will default to the
// value
// used for new page elements created in the Slides editor, which may
// depend on
// the page element kind.
type Shadow struct {
	// Alignment: The alignment point of the shadow, that sets the origin
	// for translate,
	// scale and skew of the shadow.
	//
	// Possible values:
	//   "RECTANGLE_POSITION_UNSPECIFIED" - Unspecified.
	//   "TOP_LEFT" - Top left.
	//   "TOP_CENTER" - Top center.
	//   "TOP_RIGHT" - Top right.
	//   "LEFT_CENTER" - Left center.
	//   "CENTER" - Center.
	//   "RIGHT_CENTER" - Right center.
	//   "BOTTOM_LEFT" - Bottom left.
	//   "BOTTOM_CENTER" - Bottom center.
	//   "BOTTOM_RIGHT" - Bottom right.
	Alignment string `json:"alignment,omitempty"`

	// Alpha: The alpha of the shadow's color, from 0.0 to 1.0.
	Alpha float64 `json:"alpha,omitempty"`

	// BlurRadius: The radius of the shadow blur. The larger the radius, the
	// more diffuse the
	// shadow becomes.
	BlurRadius *Dimension `json:"blurRadius,omitempty"`

	// Color: The shadow color value.
	Color *OpaqueColor `json:"color,omitempty"`

	// PropertyState: The shadow property state.
	//
	// Updating the the shadow on a page element will implicitly update this
	// field
	// to `RENDERED`, unless another value is specified in the same request.
	// To
	// have no shadow on a page element, set this field to `NOT_RENDERED`.
	// In this
	// case, any other shadow fields set in the same request will be
	// ignored.
	//
	// Possible values:
	//   "RENDERED" - If a property's state is RENDERED, then the element
	// has the corresponding
	// property when rendered on a page. If the element is a placeholder
	// shape as
	// determined by the placeholder
	// field, and it inherits from a placeholder shape, the corresponding
	// field
	// may be unset, meaning that the property value is inherited from a
	// parent
	// placeholder. If the element does not inherit, then the field will
	// contain
	// the rendered value. This is the default value.
	//   "NOT_RENDERED" - If a property's state is NOT_RENDERED, then the
	// element does not have the
	// corresponding property when rendered on a page. However, the field
	// may
	// still be set so it can be inherited by child shapes. To remove a
	// property
	// from a rendered element, set its property_state to NOT_RENDERED.
	//   "INHERIT" - If a property's state is INHERIT, then the property
	// state uses the value of
	// corresponding `property_state` field on the parent shape. Elements
	// that do
	// not inherit will never have an INHERIT property state.
	PropertyState string `json:"propertyState,omitempty"`

	// RotateWithShape: Whether the shadow should rotate with the shape.
	RotateWithShape bool `json:"rotateWithShape,omitempty"`

	// Transform: Transform that encodes the translate, scale, and skew of
	// the shadow,
	// relative to the alignment position.
	Transform *AffineTransform `json:"transform,omitempty"`

	// Type: The type of the shadow.
	//
	// Possible values:
	//   "SHADOW_TYPE_UNSPECIFIED" - Unspecified shadow type.
	//   "OUTER" - Outer shadow.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Alignment") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alignment") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Shadow) MarshalJSON() ([]byte, error) {
	type noMethod Shadow
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Shape: A PageElement kind representing a
// generic shape that does not have a more specific classification.
type Shape struct {
	// Placeholder: Placeholders are shapes that are inherit from
	// correponding placeholders on
	// layouts and masters.
	//
	// If set, the shape is a placeholder shape and any inherited
	// properties
	// can be resolved by looking at the parent placeholder identified by
	// the
	// Placeholder.parent_object_id field.
	Placeholder *Placeholder `json:"placeholder,omitempty"`

	// ShapeProperties: The properties of the shape.
	ShapeProperties *ShapeProperties `json:"shapeProperties,omitempty"`

	// ShapeType: The type of the shape.
	//
	// Possible values:
	//   "TYPE_UNSPECIFIED" - The shape type that is not predefined.
	//   "TEXT_BOX" - Text box shape.
	//   "RECTANGLE" - Rectangle shape. Corresponds to ECMA-376 ST_ShapeType
	// 'rect'.
	//   "ROUND_RECTANGLE" - Round corner rectangle shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'roundRect'
	//   "ELLIPSE" - Ellipse shape. Corresponds to ECMA-376 ST_ShapeType
	// 'ellipse'
	//   "ARC" - Curved arc shape. Corresponds to ECMA-376 ST_ShapeType
	// 'arc'
	//   "BENT_ARROW" - Bent arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'bentArrow'
	//   "BENT_UP_ARROW" - Bent up arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'bentUpArrow'
	//   "BEVEL" - Bevel shape. Corresponds to ECMA-376 ST_ShapeType 'bevel'
	//   "BLOCK_ARC" - Block arc shape. Corresponds to ECMA-376 ST_ShapeType
	// 'blockArc'
	//   "BRACE_PAIR" - Brace pair shape. Corresponds to ECMA-376
	// ST_ShapeType 'bracePair'
	//   "BRACKET_PAIR" - Bracket pair shape. Corresponds to ECMA-376
	// ST_ShapeType 'bracketPair'
	//   "CAN" - Can shape. Corresponds to ECMA-376 ST_ShapeType 'can'
	//   "CHEVRON" - Chevron shape. Corresponds to ECMA-376 ST_ShapeType
	// 'chevron'
	//   "CHORD" - Chord shape. Corresponds to ECMA-376 ST_ShapeType 'chord'
	//   "CLOUD" - Cloud shape. Corresponds to ECMA-376 ST_ShapeType 'cloud'
	//   "CORNER" - Corner shape. Corresponds to ECMA-376 ST_ShapeType
	// 'corner'
	//   "CUBE" - Cube shape. Corresponds to ECMA-376 ST_ShapeType 'cube'
	//   "CURVED_DOWN_ARROW" - Curved down arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedDownArrow'
	//   "CURVED_LEFT_ARROW" - Curved left arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedLeftArrow'
	//   "CURVED_RIGHT_ARROW" - Curved right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'curvedRightArrow'
	//   "CURVED_UP_ARROW" - Curved up arrow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'curvedUpArrow'
	//   "DECAGON" - Decagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'decagon'
	//   "DIAGONAL_STRIPE" - Diagonal stripe shape. Corresponds to ECMA-376
	// ST_ShapeType 'diagStripe'
	//   "DIAMOND" - Diamond shape. Corresponds to ECMA-376 ST_ShapeType
	// 'diamond'
	//   "DODECAGON" - Dodecagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'dodecagon'
	//   "DONUT" - Donut shape. Corresponds to ECMA-376 ST_ShapeType 'donut'
	//   "DOUBLE_WAVE" - Double wave shape. Corresponds to ECMA-376
	// ST_ShapeType 'doubleWave'
	//   "DOWN_ARROW" - Down arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'downArrow'
	//   "DOWN_ARROW_CALLOUT" - Callout down arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'downArrowCallout'
	//   "FOLDED_CORNER" - Folded corner shape. Corresponds to ECMA-376
	// ST_ShapeType 'foldedCorner'
	//   "FRAME" - Frame shape. Corresponds to ECMA-376 ST_ShapeType 'frame'
	//   "HALF_FRAME" - Half frame shape. Corresponds to ECMA-376
	// ST_ShapeType 'halfFrame'
	//   "HEART" - Heart shape. Corresponds to ECMA-376 ST_ShapeType 'heart'
	//   "HEPTAGON" - Heptagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'heptagon'
	//   "HEXAGON" - Hexagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'hexagon'
	//   "HOME_PLATE" - Home plate shape. Corresponds to ECMA-376
	// ST_ShapeType 'homePlate'
	//   "HORIZONTAL_SCROLL" - Horizontal scroll shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'horizontalScroll'
	//   "IRREGULAR_SEAL_1" - Irregular seal 1 shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'irregularSeal1'
	//   "IRREGULAR_SEAL_2" - Irregular seal 2 shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'irregularSeal2'
	//   "LEFT_ARROW" - Left arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftArrow'
	//   "LEFT_ARROW_CALLOUT" - Callout left arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'leftArrowCallout'
	//   "LEFT_BRACE" - Left brace shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftBrace'
	//   "LEFT_BRACKET" - Left bracket shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftBracket'
	//   "LEFT_RIGHT_ARROW" - Left right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'leftRightArrow'
	//   "LEFT_RIGHT_ARROW_CALLOUT" - Callout left right arrow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'leftRightArrowCallout'
	//   "LEFT_RIGHT_UP_ARROW" - Left right up arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'leftRightUpArrow'
	//   "LEFT_UP_ARROW" - Left up arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'leftUpArrow'
	//   "LIGHTNING_BOLT" - Lightning bolt shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'lightningBolt'
	//   "MATH_DIVIDE" - Divide math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathDivide'
	//   "MATH_EQUAL" - Equal math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathEqual'
	//   "MATH_MINUS" - Minus math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathMinus'
	//   "MATH_MULTIPLY" - Multiply math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathMultiply'
	//   "MATH_NOT_EQUAL" - Not equal math shape. Corresponds to ECMA-376
	// ST_ShapeType 'mathNotEqual'
	//   "MATH_PLUS" - Plus math shape. Corresponds to ECMA-376 ST_ShapeType
	// 'mathPlus'
	//   "MOON" - Moon shape. Corresponds to ECMA-376 ST_ShapeType 'moon'
	//   "NO_SMOKING" - No smoking shape. Corresponds to ECMA-376
	// ST_ShapeType 'noSmoking'
	//   "NOTCHED_RIGHT_ARROW" - Notched right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'notchedRightArrow'
	//   "OCTAGON" - Octagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'octagon'
	//   "PARALLELOGRAM" - Parallelogram shape. Corresponds to ECMA-376
	// ST_ShapeType 'parallelogram'
	//   "PENTAGON" - Pentagon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'pentagon'
	//   "PIE" - Pie shape. Corresponds to ECMA-376 ST_ShapeType 'pie'
	//   "PLAQUE" - Plaque shape. Corresponds to ECMA-376 ST_ShapeType
	// 'plaque'
	//   "PLUS" - Plus shape. Corresponds to ECMA-376 ST_ShapeType 'plus'
	//   "QUAD_ARROW" - Quad-arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'quadArrow'
	//   "QUAD_ARROW_CALLOUT" - Callout quad-arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'quadArrowCallout'
	//   "RIBBON" - Ribbon shape. Corresponds to ECMA-376 ST_ShapeType
	// 'ribbon'
	//   "RIBBON_2" - Ribbon 2 shape. Corresponds to ECMA-376 ST_ShapeType
	// 'ribbon2'
	//   "RIGHT_ARROW" - Right arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'rightArrow'
	//   "RIGHT_ARROW_CALLOUT" - Callout right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'rightArrowCallout'
	//   "RIGHT_BRACE" - Right brace shape. Corresponds to ECMA-376
	// ST_ShapeType 'rightBrace'
	//   "RIGHT_BRACKET" - Right bracket shape. Corresponds to ECMA-376
	// ST_ShapeType 'rightBracket'
	//   "ROUND_1_RECTANGLE" - One round corner rectangle shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'round1Rect'
	//   "ROUND_2_DIAGONAL_RECTANGLE" - Two diagonal round corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'round2DiagRect'
	//   "ROUND_2_SAME_RECTANGLE" - Two same-side round corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'round2SameRect'
	//   "RIGHT_TRIANGLE" - Right triangle shape. Corresponds to ECMA-376
	// ST_ShapeType 'rtTriangle'
	//   "SMILEY_FACE" - Smiley face shape. Corresponds to ECMA-376
	// ST_ShapeType 'smileyFace'
	//   "SNIP_1_RECTANGLE" - One snip corner rectangle shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'snip1Rect'
	//   "SNIP_2_DIAGONAL_RECTANGLE" - Two diagonal snip corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'snip2DiagRect'
	//   "SNIP_2_SAME_RECTANGLE" - Two same-side snip corner rectangle
	// shape. Corresponds to ECMA-376
	// ST_ShapeType 'snip2SameRect'
	//   "SNIP_ROUND_RECTANGLE" - One snip one round corner rectangle shape.
	// Corresponds to ECMA-376
	// ST_ShapeType 'snipRoundRect'
	//   "STAR_10" - Ten pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star10'
	//   "STAR_12" - Twelve pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star12'
	//   "STAR_16" - Sixteen pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star16'
	//   "STAR_24" - Twenty four pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'star24'
	//   "STAR_32" - Thirty two pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'star32'
	//   "STAR_4" - Four pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star4'
	//   "STAR_5" - Five pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star5'
	//   "STAR_6" - Six pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star6'
	//   "STAR_7" - Seven pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star7'
	//   "STAR_8" - Eight pointed star shape. Corresponds to ECMA-376
	// ST_ShapeType 'star8'
	//   "STRIPED_RIGHT_ARROW" - Striped right arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'stripedRightArrow'
	//   "SUN" - Sun shape. Corresponds to ECMA-376 ST_ShapeType 'sun'
	//   "TRAPEZOID" - Trapezoid shape. Corresponds to ECMA-376 ST_ShapeType
	// 'trapezoid'
	//   "TRIANGLE" - Triangle shape. Corresponds to ECMA-376 ST_ShapeType
	// 'triangle'
	//   "UP_ARROW" - Up arrow shape. Corresponds to ECMA-376 ST_ShapeType
	// 'upArrow'
	//   "UP_ARROW_CALLOUT" - Callout up arrow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'upArrowCallout'
	//   "UP_DOWN_ARROW" - Up down arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'upDownArrow'
	//   "UTURN_ARROW" - U-turn arrow shape. Corresponds to ECMA-376
	// ST_ShapeType 'uturnArrow'
	//   "VERTICAL_SCROLL" - Vertical scroll shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'verticalScroll'
	//   "WAVE" - Wave shape. Corresponds to ECMA-376 ST_ShapeType 'wave'
	//   "WEDGE_ELLIPSE_CALLOUT" - Callout wedge ellipse shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'wedgeEllipseCallout'
	//   "WEDGE_RECTANGLE_CALLOUT" - Callout wedge rectangle shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'wedgeRectCallout'
	//   "WEDGE_ROUND_RECTANGLE_CALLOUT" - Callout wedge round rectangle
	// shape. Corresponds to ECMA-376 ST_ShapeType
	// 'wedgeRoundRectCallout'
	//   "FLOW_CHART_ALTERNATE_PROCESS" - Alternate process flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartAlternateProcess'
	//   "FLOW_CHART_COLLATE" - Collate flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartCollate'
	//   "FLOW_CHART_CONNECTOR" - Connector flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartConnector'
	//   "FLOW_CHART_DECISION" - Decision flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartDecision'
	//   "FLOW_CHART_DELAY" - Delay flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartDelay'
	//   "FLOW_CHART_DISPLAY" - Display flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartDisplay'
	//   "FLOW_CHART_DOCUMENT" - Document flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartDocument'
	//   "FLOW_CHART_EXTRACT" - Extract flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartExtract'
	//   "FLOW_CHART_INPUT_OUTPUT" - Input output flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartInputOutput'
	//   "FLOW_CHART_INTERNAL_STORAGE" - Internal storage flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartInternalStorage'
	//   "FLOW_CHART_MAGNETIC_DISK" - Magnetic disk flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMagneticDisk'
	//   "FLOW_CHART_MAGNETIC_DRUM" - Magnetic drum flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMagneticDrum'
	//   "FLOW_CHART_MAGNETIC_TAPE" - Magnetic tape flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMagneticTape'
	//   "FLOW_CHART_MANUAL_INPUT" - Manual input flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartManualInput'
	//   "FLOW_CHART_MANUAL_OPERATION" - Manual operation flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartManualOperation'
	//   "FLOW_CHART_MERGE" - Merge flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartMerge'
	//   "FLOW_CHART_MULTIDOCUMENT" - Multi-document flow shape. Corresponds
	// to ECMA-376 ST_ShapeType
	// 'flowChartMultidocument'
	//   "FLOW_CHART_OFFLINE_STORAGE" - Offline storage flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartOfflineStorage'
	//   "FLOW_CHART_OFFPAGE_CONNECTOR" - Off-page connector flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartOffpageConnector'
	//   "FLOW_CHART_ONLINE_STORAGE" - Online storage flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartOnlineStorage'
	//   "FLOW_CHART_OR" - Or flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartOr'
	//   "FLOW_CHART_PREDEFINED_PROCESS" - Predefined process flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartPredefinedProcess'
	//   "FLOW_CHART_PREPARATION" - Preparation flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartPreparation'
	//   "FLOW_CHART_PROCESS" - Process flow shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'flowChartProcess'
	//   "FLOW_CHART_PUNCHED_CARD" - Punched card flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartPunchedCard'
	//   "FLOW_CHART_PUNCHED_TAPE" - Punched tape flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartPunchedTape'
	//   "FLOW_CHART_SORT" - Sort flow shape. Corresponds to ECMA-376
	// ST_ShapeType 'flowChartSort'
	//   "FLOW_CHART_SUMMING_JUNCTION" - Summing junction flow shape.
	// Corresponds to ECMA-376 ST_ShapeType
	// 'flowChartSummingJunction'
	//   "FLOW_CHART_TERMINATOR" - Terminator flow shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'flowChartTerminator'
	//   "ARROW_EAST" - East arrow shape.
	//   "ARROW_NORTH_EAST" - Northeast arrow shape.
	//   "ARROW_NORTH" - North arrow shape.
	//   "SPEECH" - Speech shape.
	//   "STARBURST" - Star burst shape.
	//   "TEARDROP" - Teardrop shape. Corresponds to ECMA-376 ST_ShapeType
	// 'teardrop'
	//   "ELLIPSE_RIBBON" - Ellipse ribbon shape. Corresponds to ECMA-376
	// ST_ShapeType
	// 'ellipseRibbon'
	//   "ELLIPSE_RIBBON_2" - Ellipse ribbon 2 shape. Corresponds to
	// ECMA-376 ST_ShapeType
	// 'ellipseRibbon2'
	//   "CLOUD_CALLOUT" - Callout cloud shape. Corresponds to ECMA-376
	// ST_ShapeType 'cloudCallout'
	//   "CUSTOM" - Custom shape.
	ShapeType string `json:"shapeType,omitempty"`

	// Text: The text content of the shape.
	Text *TextContent `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Placeholder") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Placeholder") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Shape) MarshalJSON() ([]byte, error) {
	type noMethod Shape
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ShapeBackgroundFill: The shape background fill.
type ShapeBackgroundFill struct {
	// PropertyState: The background fill property state.
	//
	// Updating the the fill on a shape will implicitly update this field
	// to
	// `RENDERED`, unless another value is specified in the same request.
	// To
	// have no fill on a shape, set this field to `NOT_RENDERED`. In this
	// case,
	// any other fill fields set in the same request will be ignored.
	//
	// Possible values:
	//   "RENDERED" - If a property's state is RENDERED, then the element
	// has the corresponding
	// property when rendered on a page. If the element is a placeholder
	// shape as
	// determined by the placeholder
	// field, and it inherits from a placeholder shape, the corresponding
	// field
	// may be unset, meaning that the property value is inherited from a
	// parent
	// placeholder. If the element does not inherit, then the field will
	// contain
	// the rendered value. This is the default value.
	//   "NOT_RENDERED" - If a property's state is NOT_RENDERED, then the
	// element does not have the
	// corresponding property when rendered on a page. However, the field
	// may
	// still be set so it can be inherited by child shapes. To remove a
	// property
	// from a rendered element, set its property_state to NOT_RENDERED.
	//   "INHERIT" - If a property's state is INHERIT, then the property
	// state uses the value of
	// corresponding `property_state` field on the parent shape. Elements
	// that do
	// not inherit will never have an INHERIT property state.
	PropertyState string `json:"propertyState,omitempty"`

	// SolidFill: Solid color fill.
	SolidFill *SolidFill `json:"solidFill,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PropertyState") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PropertyState") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ShapeBackgroundFill) MarshalJSON() ([]byte, error) {
	type noMethod ShapeBackgroundFill
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ShapeProperties: The properties of a Shape.
//
// If the shape is a placeholder shape as determined by the
// placeholder field, then these
// properties may be inherited from a parent placeholder
// shape.
// Determining the rendered value of the property depends on the
// corresponding
// property_state field value.
type ShapeProperties struct {
	// Outline: The outline of the shape. If unset, the outline is inherited
	// from a
	// parent placeholder if it exists. If the shape has no parent, then
	// the
	// default outline depends on the shape type, matching the defaults
	// for
	// new shapes created in the Slides editor.
	Outline *Outline `json:"outline,omitempty"`

	// Shadow: The shadow properties of the shape. If unset, the shadow is
	// inherited from
	// a parent placeholder if it exists. If the shape has no parent, then
	// the
	// default shadow matches the defaults for new shapes created in the
	// Slides
	// editor. This property is read-only.
	Shadow *Shadow `json:"shadow,omitempty"`

	// ShapeBackgroundFill: The background fill of the shape. If unset, the
	// background fill is
	// inherited from a parent placeholder if it exists. If the shape has
	// no
	// parent, then the default background fill depends on the shape
	// type,
	// matching the defaults for new shapes created in the Slides editor.
	ShapeBackgroundFill *ShapeBackgroundFill `json:"shapeBackgroundFill,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Outline") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Outline") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ShapeProperties) MarshalJSON() ([]byte, error) {
	type noMethod ShapeProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SheetsChart: A PageElement kind representing
// a linked chart embedded from Google Sheets.
type SheetsChart struct {
	// ChartId: The ID of the specific chart in the Google Sheets
	// spreadsheet that is
	// embedded.
	ChartId int64 `json:"chartId,omitempty"`

	// ContentUrl: The URL of an image of the embedded chart, with a default
	// lifetime of 30
	// minutes. This URL is tagged with the account of the requester. Anyone
	// with
	// the URL effectively accesses the image as the original requester.
	// Access to
	// the image may be lost if the presentation's sharing settings change.
	ContentUrl string `json:"contentUrl,omitempty"`

	// SheetsChartProperties: The properties of the Sheets chart.
	SheetsChartProperties *SheetsChartProperties `json:"sheetsChartProperties,omitempty"`

	// SpreadsheetId: The ID of the Google Sheets spreadsheet that contains
	// the source chart.
	SpreadsheetId string `json:"spreadsheetId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ChartId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ChartId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SheetsChart) MarshalJSON() ([]byte, error) {
	type noMethod SheetsChart
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SheetsChartProperties: The properties of the SheetsChart.
type SheetsChartProperties struct {
	// ChartImageProperties: The properties of the embedded chart image.
	ChartImageProperties *ImageProperties `json:"chartImageProperties,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "ChartImageProperties") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ChartImageProperties") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SheetsChartProperties) MarshalJSON() ([]byte, error) {
	type noMethod SheetsChartProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Size: A width and height.
type Size struct {
	// Height: The height of the object.
	Height *Dimension `json:"height,omitempty"`

	// Width: The width of the object.
	Width *Dimension `json:"width,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Height") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Height") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Size) MarshalJSON() ([]byte, error) {
	type noMethod Size
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SlideProperties: The properties of Page that are only
// relevant for pages with page_type SLIDE.
type SlideProperties struct {
	// LayoutObjectId: The object ID of the layout that this slide is based
	// on.
	LayoutObjectId string `json:"layoutObjectId,omitempty"`

	// MasterObjectId: The object ID of the master that this slide is based
	// on.
	MasterObjectId string `json:"masterObjectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "LayoutObjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LayoutObjectId") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SlideProperties) MarshalJSON() ([]byte, error) {
	type noMethod SlideProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SolidFill: A solid color fill. The page or page element is filled
// entirely with the
// specified color value.
//
// If any field is unset, its value may be inherited from a parent
// placeholder
// if it exists.
type SolidFill struct {
	// Alpha: The fraction of this `color` that should be applied to the
	// pixel.
	// That is, the final pixel color is defined by the equation:
	//
	//   pixel color = alpha * (color) + (1.0 - alpha) * (background
	// color)
	//
	// This means that a value of 1.0 corresponds to a solid color,
	// whereas
	// a value of 0.0 corresponds to a completely transparent color.
	Alpha float64 `json:"alpha,omitempty"`

	// Color: The color value of the solid fill.
	Color *OpaqueColor `json:"color,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Alpha") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alpha") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SolidFill) MarshalJSON() ([]byte, error) {
	type noMethod SolidFill
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// StretchedPictureFill: The stretched picture fill. The page or page
// element is filled entirely with
// the specified picture. The picture is stretched to fit its container.
type StretchedPictureFill struct {
	// ContentUrl: Reading the content_url:
	//
	// An URL to a picture with a default lifetime of 30 minutes.
	// This URL is tagged with the account of the requester. Anyone with the
	// URL
	// effectively accesses the picture as the original requester. Access to
	// the
	// picture may be lost if the presentation's sharing settings
	// change.
	//
	// Writing the content_url:
	//
	// The picture is fetched once at insertion time and a copy is stored
	// for
	// display inside the presentation. Pictures must be less than 50MB in
	// size,
	// cannot exceed 25 megapixels, and must be in either in PNG, JPEG, or
	// GIF
	// format.
	ContentUrl string `json:"contentUrl,omitempty"`

	// Size: The original size of the picture fill. This field is read-only.
	Size *Size `json:"size,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContentUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContentUrl") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *StretchedPictureFill) MarshalJSON() ([]byte, error) {
	type noMethod StretchedPictureFill
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SubstringMatchCriteria: A criteria that matches a specific string of
// text in a shape or table.
type SubstringMatchCriteria struct {
	// MatchCase: Indicates whether the search should respect case:
	//
	// - `True`: the search is case sensitive.
	// - `False`: the search is case insensitive.
	MatchCase bool `json:"matchCase,omitempty"`

	// Text: The text to search for in the shape or table.
	Text string `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "MatchCase") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MatchCase") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SubstringMatchCriteria) MarshalJSON() ([]byte, error) {
	type noMethod SubstringMatchCriteria
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Table: A PageElement kind representing a
// table.
type Table struct {
	// Columns: Number of columns in the table.
	Columns int64 `json:"columns,omitempty"`

	// Rows: Number of rows in the table.
	Rows int64 `json:"rows,omitempty"`

	// TableColumns: Properties of each column.
	TableColumns []*TableColumnProperties `json:"tableColumns,omitempty"`

	// TableRows: Properties and contents of each row.
	//
	// Cells that span multiple rows are contained in only one of these rows
	// and
	// have a row_span greater
	// than 1.
	TableRows []*TableRow `json:"tableRows,omitempty"`

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

func (s *Table) MarshalJSON() ([]byte, error) {
	type noMethod Table
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableCell: Properties and contents of each table cell.
type TableCell struct {
	// ColumnSpan: Column span of the cell.
	ColumnSpan int64 `json:"columnSpan,omitempty"`

	// Location: The location of the cell within the table.
	Location *TableCellLocation `json:"location,omitempty"`

	// RowSpan: Row span of the cell.
	RowSpan int64 `json:"rowSpan,omitempty"`

	// TableCellProperties: The properties of the table cell.
	TableCellProperties *TableCellProperties `json:"tableCellProperties,omitempty"`

	// Text: The text content of the cell.
	Text *TextContent `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ColumnSpan") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ColumnSpan") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TableCell) MarshalJSON() ([]byte, error) {
	type noMethod TableCell
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableCellBackgroundFill: The table cell background fill.
type TableCellBackgroundFill struct {
	// PropertyState: The background fill property state.
	//
	// Updating the the fill on a table cell will implicitly update this
	// field
	// to `RENDERED`, unless another value is specified in the same request.
	// To
	// have no fill on a table cell, set this field to `NOT_RENDERED`. In
	// this
	// case, any other fill fields set in the same request will be ignored.
	//
	// Possible values:
	//   "RENDERED" - If a property's state is RENDERED, then the element
	// has the corresponding
	// property when rendered on a page. If the element is a placeholder
	// shape as
	// determined by the placeholder
	// field, and it inherits from a placeholder shape, the corresponding
	// field
	// may be unset, meaning that the property value is inherited from a
	// parent
	// placeholder. If the element does not inherit, then the field will
	// contain
	// the rendered value. This is the default value.
	//   "NOT_RENDERED" - If a property's state is NOT_RENDERED, then the
	// element does not have the
	// corresponding property when rendered on a page. However, the field
	// may
	// still be set so it can be inherited by child shapes. To remove a
	// property
	// from a rendered element, set its property_state to NOT_RENDERED.
	//   "INHERIT" - If a property's state is INHERIT, then the property
	// state uses the value of
	// corresponding `property_state` field on the parent shape. Elements
	// that do
	// not inherit will never have an INHERIT property state.
	PropertyState string `json:"propertyState,omitempty"`

	// SolidFill: Solid color fill.
	SolidFill *SolidFill `json:"solidFill,omitempty"`

	// ForceSendFields is a list of field names (e.g. "PropertyState") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PropertyState") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TableCellBackgroundFill) MarshalJSON() ([]byte, error) {
	type noMethod TableCellBackgroundFill
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableCellLocation: A location of a single table cell within a table.
type TableCellLocation struct {
	// ColumnIndex: The 0-based column index.
	ColumnIndex int64 `json:"columnIndex,omitempty"`

	// RowIndex: The 0-based row index.
	RowIndex int64 `json:"rowIndex,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ColumnIndex") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ColumnIndex") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TableCellLocation) MarshalJSON() ([]byte, error) {
	type noMethod TableCellLocation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableCellProperties: The properties of the TableCell.
type TableCellProperties struct {
	// TableCellBackgroundFill: The background fill of the table cell. The
	// default fill matches the fill
	// for newly created table cells in the Slides editor.
	TableCellBackgroundFill *TableCellBackgroundFill `json:"tableCellBackgroundFill,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "TableCellBackgroundFill") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "TableCellBackgroundFill")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *TableCellProperties) MarshalJSON() ([]byte, error) {
	type noMethod TableCellProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableColumnProperties: Properties of each column in a table.
type TableColumnProperties struct {
	// ColumnWidth: Width of a column.
	ColumnWidth *Dimension `json:"columnWidth,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ColumnWidth") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ColumnWidth") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TableColumnProperties) MarshalJSON() ([]byte, error) {
	type noMethod TableColumnProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableRange: A table range represents a reference to a subset of a
// table.
//
// It's important to note that the cells specified by a table range do
// not
// necessarily form a rectangle. For example, let's say we have a 3 x 3
// table
// where all the cells of the last row are merged together. The table
// looks
// like this:
//
//
//   [             ]
//
// A table range with location = (0, 0), row span = 3 and column span =
// 2
// specifies the following cells:
//
//    x     x
//   [      x      ]
type TableRange struct {
	// ColumnSpan: The column span of the table range.
	ColumnSpan int64 `json:"columnSpan,omitempty"`

	// Location: The starting location of the table range.
	Location *TableCellLocation `json:"location,omitempty"`

	// RowSpan: The row span of the table range.
	RowSpan int64 `json:"rowSpan,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ColumnSpan") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ColumnSpan") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TableRange) MarshalJSON() ([]byte, error) {
	type noMethod TableRange
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TableRow: Properties and contents of each row in a table.
type TableRow struct {
	// RowHeight: Height of a row.
	RowHeight *Dimension `json:"rowHeight,omitempty"`

	// TableCells: Properties and contents of each cell.
	//
	// Cells that span multiple columns are represented only once with
	// a
	// column_span greater
	// than 1. As a result, the length of this collection does not always
	// match
	// the number of columns of the entire table.
	TableCells []*TableCell `json:"tableCells,omitempty"`

	// ForceSendFields is a list of field names (e.g. "RowHeight") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "RowHeight") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TableRow) MarshalJSON() ([]byte, error) {
	type noMethod TableRow
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TextContent: The general text content. The text must reside in a
// compatible shape (e.g.
// text box or rectangle) or a table cell in a page.
type TextContent struct {
	// Lists: The bulleted lists contained in this text, keyed by list ID.
	Lists map[string]List `json:"lists,omitempty"`

	// TextElements: The text contents broken down into its component parts,
	// including styling
	// information. This property is read-only.
	TextElements []*TextElement `json:"textElements,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Lists") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Lists") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TextContent) MarshalJSON() ([]byte, error) {
	type noMethod TextContent
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TextElement: A TextElement describes the content of a range of
// indices in the text content
// of a Shape or TableCell.
type TextElement struct {
	// AutoText: A TextElement representing a spot in the text that is
	// dynamically
	// replaced with content that can change over time.
	AutoText *AutoText `json:"autoText,omitempty"`

	// EndIndex: The zero-based end index of this text element, exclusive,
	// in Unicode code
	// units.
	EndIndex int64 `json:"endIndex,omitempty"`

	// ParagraphMarker: A marker representing the beginning of a new
	// paragraph.
	//
	// The `start_index` and `end_index` of this TextElement represent
	// the
	// range of the paragraph. Other TextElements with an index range
	// contained
	// inside this paragraph's range are considered to be part of
	// this
	// paragraph. The range of indices of two separate paragraphs will
	// never
	// overlap.
	ParagraphMarker *ParagraphMarker `json:"paragraphMarker,omitempty"`

	// StartIndex: The zero-based start index of this text element, in
	// Unicode code units.
	StartIndex int64 `json:"startIndex,omitempty"`

	// TextRun: A TextElement representing a run of text where all of the
	// characters
	// in the run have the same TextStyle.
	//
	// The `start_index` and `end_index` of TextRuns will always be
	// fully
	// contained in the index range of a single `paragraph_marker`
	// TextElement.
	// In other words, a TextRun will never span multiple paragraphs.
	TextRun *TextRun `json:"textRun,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AutoText") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AutoText") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TextElement) MarshalJSON() ([]byte, error) {
	type noMethod TextElement
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TextRun: A TextElement kind that represents a run of text that all
// has the same
// styling.
type TextRun struct {
	// Content: The text of this run.
	Content string `json:"content,omitempty"`

	// Style: The styling applied to this run.
	Style *TextStyle `json:"style,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Content") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Content") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *TextRun) MarshalJSON() ([]byte, error) {
	type noMethod TextRun
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// TextStyle: Represents the styling that can be applied to a
// TextRun.
//
// If this text is contained in a shape with a parent placeholder, then
// these text styles may be
// inherited from the parent. Which text styles are inherited depend on
// the
// nesting level of lists:
//
// * A text run in a paragraph that is not in a list will inherit its
// text style
//   from the the newline character in the paragraph at the 0 nesting
// level of
//   the list inside the parent placeholder.
// * A text run in a paragraph that is in a list will inherit its text
// style
//   from the newline character in the paragraph at its corresponding
// nesting
//   level of the list inside the parent placeholder.
//
// Inherited text styles are represented as unset fields in this
// message. If
// text is contained in a shape without a parent placeholder, unsetting
// these
// fields will revert the style to a value matching the defaults in the
// Slides
// editor.
type TextStyle struct {
	// BackgroundColor: The background color of the text. If set, the color
	// is either opaque or
	// transparent, depending on if the `opaque_color` field in it is set.
	BackgroundColor *OptionalColor `json:"backgroundColor,omitempty"`

	// BaselineOffset: The text's vertical offset from its normal
	// position.
	//
	// Text with `SUPERSCRIPT` or `SUBSCRIPT` baseline offsets is
	// automatically
	// rendered in a smaller font size, computed based on the `font_size`
	// field.
	// The `font_size` itself is not affected by changes in this field.
	//
	// Possible values:
	//   "BASELINE_OFFSET_UNSPECIFIED" - The text's baseline offset is
	// inherited from the parent.
	//   "NONE" - The text is not vertically offset.
	//   "SUPERSCRIPT" - The text is vertically offset upwards
	// (superscript).
	//   "SUBSCRIPT" - The text is vertically offset downwards (subscript).
	BaselineOffset string `json:"baselineOffset,omitempty"`

	// Bold: Whether or not the text is bold.
	Bold bool `json:"bold,omitempty"`

	// FontFamily: The font family of the text.
	//
	// The font family can be any font from the Font menu in Slides or
	// from
	// [Google Fonts] (https://fonts.google.com/). If the font name
	// is
	// unrecognized, the text is rendered in `Arial`.
	//
	// Some fonts can affect the weight of the text. If an update
	// request
	// specifies values for both `font_family` and `bold`, the
	// explicitly-set
	// `bold` value is used.
	FontFamily string `json:"fontFamily,omitempty"`

	// FontSize: The size of the text's font. When read, the `font_size`
	// will specified in
	// points.
	FontSize *Dimension `json:"fontSize,omitempty"`

	// ForegroundColor: The color of the text itself. If set, the color is
	// either opaque or
	// transparent, depending on if the `opaque_color` field in it is set.
	ForegroundColor *OptionalColor `json:"foregroundColor,omitempty"`

	// Italic: Whether or not the text is italicized.
	Italic bool `json:"italic,omitempty"`

	// Link: The hyperlink destination of the text. If unset, there is no
	// link. Links
	// are not inherited from parent text.
	//
	// Changing the link in an update request causes some other changes to
	// the
	// text style of the range:
	//
	// * When setting a link, the text foreground color will be set to
	//   ThemeColorType.HYPERLINK and the text will
	//   be underlined. If these fields are modified in the same
	//   request, those values will be used instead of the link defaults.
	// * Setting a link on a text range that overlaps with an existing link
	// will
	//   also update the existing link to point to the new URL.
	// * Links are not settable on newline characters. As a result, setting
	// a link
	//   on a text range that crosses a paragraph boundary, such as
	// "ABC\n123",
	//   will separate the newline character(s) into their own text runs.
	// The
	//   link will be applied separately to the runs before and after the
	// newline.
	// * Removing a link will update the text style of the range to match
	// the
	//   style of the preceding text (or the default text styles if the
	// preceding
	//   text is another link) unless different styles are being set in the
	// same
	//   request.
	Link *Link `json:"link,omitempty"`

	// SmallCaps: Whether or not the text is in small capital letters.
	SmallCaps bool `json:"smallCaps,omitempty"`

	// Strikethrough: Whether or not the text is struck through.
	Strikethrough bool `json:"strikethrough,omitempty"`

	// Underline: Whether or not the text is underlined.
	Underline bool `json:"underline,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BackgroundColor") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BackgroundColor") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *TextStyle) MarshalJSON() ([]byte, error) {
	type noMethod TextStyle
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ThemeColorPair: A pair mapping a theme color type to the concrete
// color it represents.
type ThemeColorPair struct {
	// Color: The concrete color corresponding to the theme color type
	// above.
	Color *RgbColor `json:"color,omitempty"`

	// Type: The type of the theme color.
	//
	// Possible values:
	//   "THEME_COLOR_TYPE_UNSPECIFIED" - Unspecified theme color. This
	// value should not be used.
	//   "DARK1" - Represents the first dark color.
	//   "LIGHT1" - Represents the first light color.
	//   "DARK2" - Represents the second dark color.
	//   "LIGHT2" - Represents the second light color.
	//   "ACCENT1" - Represents the first accent color.
	//   "ACCENT2" - Represents the second accent color.
	//   "ACCENT3" - Represents the third accent color.
	//   "ACCENT4" - Represents the fourth accent color.
	//   "ACCENT5" - Represents the fifth accent color.
	//   "ACCENT6" - Represents the sixth accent color.
	//   "HYPERLINK" - Represents the color to use for hyperlinks.
	//   "FOLLOWED_HYPERLINK" - Represents the color to use for visited
	// hyperlinks.
	//   "TEXT1" - Represents the first text color.
	//   "BACKGROUND1" - Represents the first background color.
	//   "TEXT2" - Represents the second text color.
	//   "BACKGROUND2" - Represents the second background color.
	Type string `json:"type,omitempty"`

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

func (s *ThemeColorPair) MarshalJSON() ([]byte, error) {
	type noMethod ThemeColorPair
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateImagePropertiesRequest: Update the properties of an Image.
type UpdateImagePropertiesRequest struct {
	// Fields: The fields that should be updated.
	//
	// At least one field must be specified. The root `imageProperties`
	// is
	// implied and should not be specified. A single "*" can be used
	// as
	// short-hand for listing every field.
	//
	// For example to update the image outline color, set `fields`
	// to
	// "outline.outlineFill.solidFill.color".
	//
	// To reset a property to its default value, include its field name in
	// the
	// field mask but leave the field itself unset.
	Fields string `json:"fields,omitempty"`

	// ImageProperties: The image properties to update.
	ImageProperties *ImageProperties `json:"imageProperties,omitempty"`

	// ObjectId: The object ID of the image the updates are applied to.
	ObjectId string `json:"objectId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Fields") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Fields") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UpdateImagePropertiesRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdateImagePropertiesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdatePageElementTransformRequest: Updates the transform of a page
// element.
type UpdatePageElementTransformRequest struct {
	// ApplyMode: The apply mode of the transform update.
	//
	// Possible values:
	//   "APPLY_MODE_UNSPECIFIED" - Unspecified mode.
	//   "RELATIVE" - Applies the new AffineTransform matrix to the existing
	// one, and
	// replaces the existing one with the resulting concatenation.
	//   "ABSOLUTE" - Replaces the existing AffineTransform matrix with the
	// new one.
	ApplyMode string `json:"applyMode,omitempty"`

	// ObjectId: The object ID of the page element to update.
	ObjectId string `json:"objectId,omitempty"`

	// Transform: The input transform matrix used to update the page
	// element.
	Transform *AffineTransform `json:"transform,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ApplyMode") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ApplyMode") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UpdatePageElementTransformRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdatePageElementTransformRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdatePagePropertiesRequest: Updates the properties of a Page.
type UpdatePagePropertiesRequest struct {
	// Fields: The fields that should be updated.
	//
	// At least one field must be specified. The root `pageProperties`
	// is
	// implied and should not be specified. A single "*" can be used
	// as
	// short-hand for listing every field.
	//
	// For example to update the page background solid fill color, set
	// `fields`
	// to "pageBackgroundFill.solidFill.color".
	//
	// To reset a property to its default value, include its field name in
	// the
	// field mask but leave the field itself unset.
	Fields string `json:"fields,omitempty"`

	// ObjectId: The object ID of the page the update is applied to.
	ObjectId string `json:"objectId,omitempty"`

	// PageProperties: The page properties to update.
	PageProperties *PageProperties `json:"pageProperties,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Fields") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Fields") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UpdatePagePropertiesRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdatePagePropertiesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateShapePropertiesRequest: Update the properties of a Shape.
type UpdateShapePropertiesRequest struct {
	// Fields: The fields that should be updated.
	//
	// At least one field must be specified. The root `shapeProperties`
	// is
	// implied and should not be specified. A single "*" can be used
	// as
	// short-hand for listing every field.
	//
	// For example to update the shape background solid fill color, set
	// `fields`
	// to "shapeBackgroundFill.solidFill.color".
	//
	// To reset a property to its default value, include its field name in
	// the
	// field mask but leave the field itself unset.
	Fields string `json:"fields,omitempty"`

	// ObjectId: The object ID of the shape the updates are applied to.
	ObjectId string `json:"objectId,omitempty"`

	// ShapeProperties: The shape properties to update.
	ShapeProperties *ShapeProperties `json:"shapeProperties,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Fields") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Fields") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UpdateShapePropertiesRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdateShapePropertiesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateSlidesPositionRequest: Updates the position of slides in the
// presentation.
type UpdateSlidesPositionRequest struct {
	// InsertionIndex: The index where the slides should be inserted, based
	// on the slide
	// arrangement before the move takes place. Must be between zero and
	// the
	// number of slides in the presentation, inclusive.
	InsertionIndex int64 `json:"insertionIndex,omitempty"`

	// SlideObjectIds: The IDs of the slides in the presentation that should
	// be moved.
	// The slides in this list must be in existing presentation order,
	// without
	// duplicates.
	SlideObjectIds []string `json:"slideObjectIds,omitempty"`

	// ForceSendFields is a list of field names (e.g. "InsertionIndex") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "InsertionIndex") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *UpdateSlidesPositionRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdateSlidesPositionRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateTableCellPropertiesRequest: Update the properties of a
// TableCell.
type UpdateTableCellPropertiesRequest struct {
	// Fields: The fields that should be updated.
	//
	// At least one field must be specified. The root `tableCellProperties`
	// is
	// implied and should not be specified. A single "*" can be used
	// as
	// short-hand for listing every field.
	//
	// For example to update the table cell background solid fill color,
	// set
	// `fields` to "tableCellBackgroundFill.solidFill.color".
	//
	// To reset a property to its default value, include its field name in
	// the
	// field mask but leave the field itself unset.
	Fields string `json:"fields,omitempty"`

	// ObjectId: The object ID of the table.
	ObjectId string `json:"objectId,omitempty"`

	// TableCellProperties: The table cell properties to update.
	TableCellProperties *TableCellProperties `json:"tableCellProperties,omitempty"`

	// TableRange: The table range representing the subset of the table to
	// which the updates
	// are applied. If a table range is not specified, the updates will
	// apply to
	// the entire table.
	TableRange *TableRange `json:"tableRange,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Fields") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Fields") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UpdateTableCellPropertiesRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdateTableCellPropertiesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateTextStyleRequest: Update the styling of text in a Shape
// or
// Table.
type UpdateTextStyleRequest struct {
	// CellLocation: The optional table cell location if the text to be
	// styled is in a table
	// cell. If present, the object_id must refer to a table.
	CellLocation *TableCellLocation `json:"cellLocation,omitempty"`

	// Fields: The fields that should be updated.
	//
	// At least one field must be specified. The root `style` is implied
	// and
	// should not be specified. A single "*" can be used as short-hand
	// for
	// listing every field.
	//
	// For example to update the text style to bold, set `fields` to
	// "bold".
	//
	// To reset a property to its default value,
	// include its field name in the field mask but leave the field itself
	// unset.
	Fields string `json:"fields,omitempty"`

	// ObjectId: The object ID of the shape or table with the text to be
	// styled.
	ObjectId string `json:"objectId,omitempty"`

	// Style: The style(s) to set on the text.
	//
	// If the value for a particular style matches that of the parent, that
	// style
	// will be set to inherit.
	//
	// Certain text style changes may cause other changes meant to mirror
	// the
	// behavior of the Slides editor. See the documentation of
	// TextStyle for more information.
	Style *TextStyle `json:"style,omitempty"`

	// TextRange: The range of text to style.
	//
	// The range may be extended to include adjacent newlines.
	//
	// If the range fully contains a paragraph belonging to a list,
	// the
	// paragraph's bullet is also updated with the matching text style.
	TextRange *Range `json:"textRange,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CellLocation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CellLocation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UpdateTextStyleRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdateTextStyleRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UpdateVideoPropertiesRequest: Update the properties of a Video.
type UpdateVideoPropertiesRequest struct {
	// Fields: The fields that should be updated.
	//
	// At least one field must be specified. The root `videoProperties`
	// is
	// implied and should not be specified. A single "*" can be used
	// as
	// short-hand for listing every field.
	//
	// For example to update the video outline color, set `fields`
	// to
	// "outline.outlineFill.solidFill.color".
	//
	// To reset a property to its default value, include its field name in
	// the
	// field mask but leave the field itself unset.
	Fields string `json:"fields,omitempty"`

	// ObjectId: The object ID of the video the updates are applied to.
	ObjectId string `json:"objectId,omitempty"`

	// VideoProperties: The video properties to update.
	VideoProperties *VideoProperties `json:"videoProperties,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Fields") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Fields") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UpdateVideoPropertiesRequest) MarshalJSON() ([]byte, error) {
	type noMethod UpdateVideoPropertiesRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Video: A PageElement kind representing a
// video.
type Video struct {
	// Id: The video source's unique identifier for this video.
	Id string `json:"id,omitempty"`

	// Source: The video source.
	//
	// Possible values:
	//   "SOURCE_UNSPECIFIED" - The video source is unspecified.
	//   "YOUTUBE" - The video source is YouTube.
	Source string `json:"source,omitempty"`

	// Url: An URL to a video. The URL is valid as long as the source
	// video
	// exists and sharing settings do not change.
	Url string `json:"url,omitempty"`

	// VideoProperties: The properties of the video.
	VideoProperties *VideoProperties `json:"videoProperties,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Id") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Id") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Video) MarshalJSON() ([]byte, error) {
	type noMethod Video
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VideoProperties: The properties of the Video.
type VideoProperties struct {
	// Outline: The outline of the video. The default outline matches the
	// defaults for new
	// videos created in the Slides editor.
	Outline *Outline `json:"outline,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Outline") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Outline") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VideoProperties) MarshalJSON() ([]byte, error) {
	type noMethod VideoProperties
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WordArt: A PageElement kind representing
// word art.
type WordArt struct {
	// RenderedText: The text rendered as word art.
	RenderedText string `json:"renderedText,omitempty"`

	// ForceSendFields is a list of field names (e.g. "RenderedText") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "RenderedText") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WordArt) MarshalJSON() ([]byte, error) {
	type noMethod WordArt
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "slides.presentations.batchUpdate":

type PresentationsBatchUpdateCall struct {
	s                              *Service
	presentationId                 string
	batchupdatepresentationrequest *BatchUpdatePresentationRequest
	urlParams_                     gensupport.URLParams
	ctx_                           context.Context
	header_                        http.Header
}

// BatchUpdate: Applies one or more updates to the presentation.
//
// Each request is validated before
// being applied. If any request is not valid then the entire request
// will
// fail and nothing will be applied.
//
// Some requests have replies to
// give you some information about how they are applied. Other requests
// do
// not need to return information; these each return an empty reply.
// The order of replies matches that of the requests.
//
// For example, suppose you call batchUpdate with four updates, and only
// the
// third one returns information. The response would have two empty
// replies,
// the reply to the third request, and another empty reply, in that
// order.
//
// Because other users may be editing the presentation, it is not
// guaranteed
// that the presentation will exactly reflect your changes: your changes
// may
// be altered with respect to collaborator changes. If there are
// no
// collaborators, the presentation should reflect your changes. In any
// case,
// it is guaranteed that the updates in your request will be applied
// together
// atomically.
func (r *PresentationsService) BatchUpdate(presentationId string, batchupdatepresentationrequest *BatchUpdatePresentationRequest) *PresentationsBatchUpdateCall {
	c := &PresentationsBatchUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.presentationId = presentationId
	c.batchupdatepresentationrequest = batchupdatepresentationrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PresentationsBatchUpdateCall) Fields(s ...googleapi.Field) *PresentationsBatchUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PresentationsBatchUpdateCall) Context(ctx context.Context) *PresentationsBatchUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PresentationsBatchUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PresentationsBatchUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.batchupdatepresentationrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/presentations/{presentationId}:batchUpdate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"presentationId": c.presentationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "slides.presentations.batchUpdate" call.
// Exactly one of *BatchUpdatePresentationResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *BatchUpdatePresentationResponse.ServerResponse.Header or (if
// a response was returned at all) in error.(*googleapi.Error).Header.
// Use googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PresentationsBatchUpdateCall) Do(opts ...googleapi.CallOption) (*BatchUpdatePresentationResponse, error) {
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
	ret := &BatchUpdatePresentationResponse{
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
	//   "description": "Applies one or more updates to the presentation.\n\nEach request is validated before\nbeing applied. If any request is not valid then the entire request will\nfail and nothing will be applied.\n\nSome requests have replies to\ngive you some information about how they are applied. Other requests do\nnot need to return information; these each return an empty reply.\nThe order of replies matches that of the requests.\n\nFor example, suppose you call batchUpdate with four updates, and only the\nthird one returns information. The response would have two empty replies,\nthe reply to the third request, and another empty reply, in that order.\n\nBecause other users may be editing the presentation, it is not guaranteed\nthat the presentation will exactly reflect your changes: your changes may\nbe altered with respect to collaborator changes. If there are no\ncollaborators, the presentation should reflect your changes. In any case,\nit is guaranteed that the updates in your request will be applied together\natomically.",
	//   "flatPath": "v1/presentations/{presentationId}:batchUpdate",
	//   "httpMethod": "POST",
	//   "id": "slides.presentations.batchUpdate",
	//   "parameterOrder": [
	//     "presentationId"
	//   ],
	//   "parameters": {
	//     "presentationId": {
	//       "description": "The presentation to apply the updates to.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/presentations/{presentationId}:batchUpdate",
	//   "request": {
	//     "$ref": "BatchUpdatePresentationRequest"
	//   },
	//   "response": {
	//     "$ref": "BatchUpdatePresentationResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/drive",
	//     "https://www.googleapis.com/auth/drive.readonly",
	//     "https://www.googleapis.com/auth/presentations",
	//     "https://www.googleapis.com/auth/spreadsheets",
	//     "https://www.googleapis.com/auth/spreadsheets.readonly"
	//   ]
	// }

}

// method id "slides.presentations.create":

type PresentationsCreateCall struct {
	s            *Service
	presentation *Presentation
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Create: Creates a new presentation using the title given in the
// request. Other
// fields in the request are ignored.
// Returns the created presentation.
func (r *PresentationsService) Create(presentation *Presentation) *PresentationsCreateCall {
	c := &PresentationsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.presentation = presentation
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PresentationsCreateCall) Fields(s ...googleapi.Field) *PresentationsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PresentationsCreateCall) Context(ctx context.Context) *PresentationsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PresentationsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PresentationsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.presentation)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/presentations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "slides.presentations.create" call.
// Exactly one of *Presentation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Presentation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *PresentationsCreateCall) Do(opts ...googleapi.CallOption) (*Presentation, error) {
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
	ret := &Presentation{
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
	//   "description": "Creates a new presentation using the title given in the request. Other\nfields in the request are ignored.\nReturns the created presentation.",
	//   "flatPath": "v1/presentations",
	//   "httpMethod": "POST",
	//   "id": "slides.presentations.create",
	//   "parameterOrder": [],
	//   "parameters": {},
	//   "path": "v1/presentations",
	//   "request": {
	//     "$ref": "Presentation"
	//   },
	//   "response": {
	//     "$ref": "Presentation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/drive",
	//     "https://www.googleapis.com/auth/presentations"
	//   ]
	// }

}

// method id "slides.presentations.get":

type PresentationsGetCall struct {
	s              *Service
	presentationId string
	urlParams_     gensupport.URLParams
	ifNoneMatch_   string
	ctx_           context.Context
	header_        http.Header
}

// Get: Gets the latest version of the specified presentation.
func (r *PresentationsService) Get(presentationId string) *PresentationsGetCall {
	c := &PresentationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.presentationId = presentationId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PresentationsGetCall) Fields(s ...googleapi.Field) *PresentationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PresentationsGetCall) IfNoneMatch(entityTag string) *PresentationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PresentationsGetCall) Context(ctx context.Context) *PresentationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PresentationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PresentationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/presentations/{+presentationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"presentationId": c.presentationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "slides.presentations.get" call.
// Exactly one of *Presentation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Presentation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *PresentationsGetCall) Do(opts ...googleapi.CallOption) (*Presentation, error) {
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
	ret := &Presentation{
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
	//   "description": "Gets the latest version of the specified presentation.",
	//   "flatPath": "v1/presentations/{presentationsId}",
	//   "httpMethod": "GET",
	//   "id": "slides.presentations.get",
	//   "parameterOrder": [
	//     "presentationId"
	//   ],
	//   "parameters": {
	//     "presentationId": {
	//       "description": "The ID of the presentation to retrieve.",
	//       "location": "path",
	//       "pattern": "^[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/presentations/{+presentationId}",
	//   "response": {
	//     "$ref": "Presentation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/drive",
	//     "https://www.googleapis.com/auth/drive.readonly",
	//     "https://www.googleapis.com/auth/presentations",
	//     "https://www.googleapis.com/auth/presentations.readonly"
	//   ]
	// }

}

// method id "slides.presentations.pages.get":

type PresentationsPagesGetCall struct {
	s              *Service
	presentationId string
	pageObjectId   string
	urlParams_     gensupport.URLParams
	ifNoneMatch_   string
	ctx_           context.Context
	header_        http.Header
}

// Get: Gets the latest version of the specified page in the
// presentation.
func (r *PresentationsPagesService) Get(presentationId string, pageObjectId string) *PresentationsPagesGetCall {
	c := &PresentationsPagesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.presentationId = presentationId
	c.pageObjectId = pageObjectId
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PresentationsPagesGetCall) Fields(s ...googleapi.Field) *PresentationsPagesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PresentationsPagesGetCall) IfNoneMatch(entityTag string) *PresentationsPagesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PresentationsPagesGetCall) Context(ctx context.Context) *PresentationsPagesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PresentationsPagesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PresentationsPagesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/presentations/{presentationId}/pages/{pageObjectId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"presentationId": c.presentationId,
		"pageObjectId":   c.pageObjectId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "slides.presentations.pages.get" call.
// Exactly one of *Page or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Page.ServerResponse.Header or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *PresentationsPagesGetCall) Do(opts ...googleapi.CallOption) (*Page, error) {
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
	ret := &Page{
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
	//   "description": "Gets the latest version of the specified page in the presentation.",
	//   "flatPath": "v1/presentations/{presentationId}/pages/{pageObjectId}",
	//   "httpMethod": "GET",
	//   "id": "slides.presentations.pages.get",
	//   "parameterOrder": [
	//     "presentationId",
	//     "pageObjectId"
	//   ],
	//   "parameters": {
	//     "pageObjectId": {
	//       "description": "The object ID of the page to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "presentationId": {
	//       "description": "The ID of the presentation to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1/presentations/{presentationId}/pages/{pageObjectId}",
	//   "response": {
	//     "$ref": "Page"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/drive",
	//     "https://www.googleapis.com/auth/drive.readonly",
	//     "https://www.googleapis.com/auth/presentations",
	//     "https://www.googleapis.com/auth/presentations.readonly"
	//   ]
	// }

}
