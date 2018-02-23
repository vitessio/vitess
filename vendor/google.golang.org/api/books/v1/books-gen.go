// Package books provides access to the Books API.
//
// See https://developers.google.com/books/docs/v1/getting_started
//
// Usage example:
//
//   import "google.golang.org/api/books/v1"
//   ...
//   booksService, err := books.New(oauthHttpClient)
package books // import "google.golang.org/api/books/v1"

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

const apiId = "books:v1"
const apiName = "books"
const apiVersion = "v1"
const basePath = "https://www.googleapis.com/books/v1/"

// OAuth2 scopes used by this API.
const (
	// Manage your books
	BooksScope = "https://www.googleapis.com/auth/books"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Bookshelves = NewBookshelvesService(s)
	s.Cloudloading = NewCloudloadingService(s)
	s.Dictionary = NewDictionaryService(s)
	s.Layers = NewLayersService(s)
	s.Myconfig = NewMyconfigService(s)
	s.Mylibrary = NewMylibraryService(s)
	s.Notification = NewNotificationService(s)
	s.Onboarding = NewOnboardingService(s)
	s.Personalizedstream = NewPersonalizedstreamService(s)
	s.Promooffer = NewPromoofferService(s)
	s.Series = NewSeriesService(s)
	s.Volumes = NewVolumesService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Bookshelves *BookshelvesService

	Cloudloading *CloudloadingService

	Dictionary *DictionaryService

	Layers *LayersService

	Myconfig *MyconfigService

	Mylibrary *MylibraryService

	Notification *NotificationService

	Onboarding *OnboardingService

	Personalizedstream *PersonalizedstreamService

	Promooffer *PromoofferService

	Series *SeriesService

	Volumes *VolumesService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewBookshelvesService(s *Service) *BookshelvesService {
	rs := &BookshelvesService{s: s}
	rs.Volumes = NewBookshelvesVolumesService(s)
	return rs
}

type BookshelvesService struct {
	s *Service

	Volumes *BookshelvesVolumesService
}

func NewBookshelvesVolumesService(s *Service) *BookshelvesVolumesService {
	rs := &BookshelvesVolumesService{s: s}
	return rs
}

type BookshelvesVolumesService struct {
	s *Service
}

func NewCloudloadingService(s *Service) *CloudloadingService {
	rs := &CloudloadingService{s: s}
	return rs
}

type CloudloadingService struct {
	s *Service
}

func NewDictionaryService(s *Service) *DictionaryService {
	rs := &DictionaryService{s: s}
	return rs
}

type DictionaryService struct {
	s *Service
}

func NewLayersService(s *Service) *LayersService {
	rs := &LayersService{s: s}
	rs.AnnotationData = NewLayersAnnotationDataService(s)
	rs.VolumeAnnotations = NewLayersVolumeAnnotationsService(s)
	return rs
}

type LayersService struct {
	s *Service

	AnnotationData *LayersAnnotationDataService

	VolumeAnnotations *LayersVolumeAnnotationsService
}

func NewLayersAnnotationDataService(s *Service) *LayersAnnotationDataService {
	rs := &LayersAnnotationDataService{s: s}
	return rs
}

type LayersAnnotationDataService struct {
	s *Service
}

func NewLayersVolumeAnnotationsService(s *Service) *LayersVolumeAnnotationsService {
	rs := &LayersVolumeAnnotationsService{s: s}
	return rs
}

type LayersVolumeAnnotationsService struct {
	s *Service
}

func NewMyconfigService(s *Service) *MyconfigService {
	rs := &MyconfigService{s: s}
	return rs
}

type MyconfigService struct {
	s *Service
}

func NewMylibraryService(s *Service) *MylibraryService {
	rs := &MylibraryService{s: s}
	rs.Annotations = NewMylibraryAnnotationsService(s)
	rs.Bookshelves = NewMylibraryBookshelvesService(s)
	rs.Readingpositions = NewMylibraryReadingpositionsService(s)
	return rs
}

type MylibraryService struct {
	s *Service

	Annotations *MylibraryAnnotationsService

	Bookshelves *MylibraryBookshelvesService

	Readingpositions *MylibraryReadingpositionsService
}

func NewMylibraryAnnotationsService(s *Service) *MylibraryAnnotationsService {
	rs := &MylibraryAnnotationsService{s: s}
	return rs
}

type MylibraryAnnotationsService struct {
	s *Service
}

func NewMylibraryBookshelvesService(s *Service) *MylibraryBookshelvesService {
	rs := &MylibraryBookshelvesService{s: s}
	rs.Volumes = NewMylibraryBookshelvesVolumesService(s)
	return rs
}

type MylibraryBookshelvesService struct {
	s *Service

	Volumes *MylibraryBookshelvesVolumesService
}

func NewMylibraryBookshelvesVolumesService(s *Service) *MylibraryBookshelvesVolumesService {
	rs := &MylibraryBookshelvesVolumesService{s: s}
	return rs
}

type MylibraryBookshelvesVolumesService struct {
	s *Service
}

func NewMylibraryReadingpositionsService(s *Service) *MylibraryReadingpositionsService {
	rs := &MylibraryReadingpositionsService{s: s}
	return rs
}

type MylibraryReadingpositionsService struct {
	s *Service
}

func NewNotificationService(s *Service) *NotificationService {
	rs := &NotificationService{s: s}
	return rs
}

type NotificationService struct {
	s *Service
}

func NewOnboardingService(s *Service) *OnboardingService {
	rs := &OnboardingService{s: s}
	return rs
}

type OnboardingService struct {
	s *Service
}

func NewPersonalizedstreamService(s *Service) *PersonalizedstreamService {
	rs := &PersonalizedstreamService{s: s}
	return rs
}

type PersonalizedstreamService struct {
	s *Service
}

func NewPromoofferService(s *Service) *PromoofferService {
	rs := &PromoofferService{s: s}
	return rs
}

type PromoofferService struct {
	s *Service
}

func NewSeriesService(s *Service) *SeriesService {
	rs := &SeriesService{s: s}
	rs.Membership = NewSeriesMembershipService(s)
	return rs
}

type SeriesService struct {
	s *Service

	Membership *SeriesMembershipService
}

func NewSeriesMembershipService(s *Service) *SeriesMembershipService {
	rs := &SeriesMembershipService{s: s}
	return rs
}

type SeriesMembershipService struct {
	s *Service
}

func NewVolumesService(s *Service) *VolumesService {
	rs := &VolumesService{s: s}
	rs.Associated = NewVolumesAssociatedService(s)
	rs.Mybooks = NewVolumesMybooksService(s)
	rs.Recommended = NewVolumesRecommendedService(s)
	rs.Useruploaded = NewVolumesUseruploadedService(s)
	return rs
}

type VolumesService struct {
	s *Service

	Associated *VolumesAssociatedService

	Mybooks *VolumesMybooksService

	Recommended *VolumesRecommendedService

	Useruploaded *VolumesUseruploadedService
}

func NewVolumesAssociatedService(s *Service) *VolumesAssociatedService {
	rs := &VolumesAssociatedService{s: s}
	return rs
}

type VolumesAssociatedService struct {
	s *Service
}

func NewVolumesMybooksService(s *Service) *VolumesMybooksService {
	rs := &VolumesMybooksService{s: s}
	return rs
}

type VolumesMybooksService struct {
	s *Service
}

func NewVolumesRecommendedService(s *Service) *VolumesRecommendedService {
	rs := &VolumesRecommendedService{s: s}
	return rs
}

type VolumesRecommendedService struct {
	s *Service
}

func NewVolumesUseruploadedService(s *Service) *VolumesUseruploadedService {
	rs := &VolumesUseruploadedService{s: s}
	return rs
}

type VolumesUseruploadedService struct {
	s *Service
}

type Annotation struct {
	// AfterSelectedText: Anchor text after excerpt. For requests, if the
	// user bookmarked a screen that has no flowing text on it, then this
	// field should be empty.
	AfterSelectedText string `json:"afterSelectedText,omitempty"`

	// BeforeSelectedText: Anchor text before excerpt. For requests, if the
	// user bookmarked a screen that has no flowing text on it, then this
	// field should be empty.
	BeforeSelectedText string `json:"beforeSelectedText,omitempty"`

	// ClientVersionRanges: Selection ranges sent from the client.
	ClientVersionRanges *AnnotationClientVersionRanges `json:"clientVersionRanges,omitempty"`

	// Created: Timestamp for the created time of this annotation.
	Created string `json:"created,omitempty"`

	// CurrentVersionRanges: Selection ranges for the most recent content
	// version.
	CurrentVersionRanges *AnnotationCurrentVersionRanges `json:"currentVersionRanges,omitempty"`

	// Data: User-created data for this annotation.
	Data string `json:"data,omitempty"`

	// Deleted: Indicates that this annotation is deleted.
	Deleted bool `json:"deleted,omitempty"`

	// HighlightStyle: The highlight style for this annotation.
	HighlightStyle string `json:"highlightStyle,omitempty"`

	// Id: Id of this annotation, in the form of a GUID.
	Id string `json:"id,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// LayerId: The layer this annotation is for.
	LayerId string `json:"layerId,omitempty"`

	LayerSummary *AnnotationLayerSummary `json:"layerSummary,omitempty"`

	// PageIds: Pages that this annotation spans.
	PageIds []string `json:"pageIds,omitempty"`

	// SelectedText: Excerpt from the volume.
	SelectedText string `json:"selectedText,omitempty"`

	// SelfLink: URL to this resource.
	SelfLink string `json:"selfLink,omitempty"`

	// Updated: Timestamp for the last time this annotation was modified.
	Updated string `json:"updated,omitempty"`

	// VolumeId: The volume that this annotation belongs to.
	VolumeId string `json:"volumeId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AfterSelectedText")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AfterSelectedText") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Annotation) MarshalJSON() ([]byte, error) {
	type noMethod Annotation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AnnotationClientVersionRanges: Selection ranges sent from the client.
type AnnotationClientVersionRanges struct {
	// CfiRange: Range in CFI format for this annotation sent by client.
	CfiRange *BooksAnnotationsRange `json:"cfiRange,omitempty"`

	// ContentVersion: Content version the client sent in.
	ContentVersion string `json:"contentVersion,omitempty"`

	// GbImageRange: Range in GB image format for this annotation sent by
	// client.
	GbImageRange *BooksAnnotationsRange `json:"gbImageRange,omitempty"`

	// GbTextRange: Range in GB text format for this annotation sent by
	// client.
	GbTextRange *BooksAnnotationsRange `json:"gbTextRange,omitempty"`

	// ImageCfiRange: Range in image CFI format for this annotation sent by
	// client.
	ImageCfiRange *BooksAnnotationsRange `json:"imageCfiRange,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CfiRange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CfiRange") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AnnotationClientVersionRanges) MarshalJSON() ([]byte, error) {
	type noMethod AnnotationClientVersionRanges
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AnnotationCurrentVersionRanges: Selection ranges for the most recent
// content version.
type AnnotationCurrentVersionRanges struct {
	// CfiRange: Range in CFI format for this annotation for version above.
	CfiRange *BooksAnnotationsRange `json:"cfiRange,omitempty"`

	// ContentVersion: Content version applicable to ranges below.
	ContentVersion string `json:"contentVersion,omitempty"`

	// GbImageRange: Range in GB image format for this annotation for
	// version above.
	GbImageRange *BooksAnnotationsRange `json:"gbImageRange,omitempty"`

	// GbTextRange: Range in GB text format for this annotation for version
	// above.
	GbTextRange *BooksAnnotationsRange `json:"gbTextRange,omitempty"`

	// ImageCfiRange: Range in image CFI format for this annotation for
	// version above.
	ImageCfiRange *BooksAnnotationsRange `json:"imageCfiRange,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CfiRange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CfiRange") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *AnnotationCurrentVersionRanges) MarshalJSON() ([]byte, error) {
	type noMethod AnnotationCurrentVersionRanges
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AnnotationLayerSummary struct {
	// AllowedCharacterCount: Maximum allowed characters on this layer,
	// especially for the "copy" layer.
	AllowedCharacterCount int64 `json:"allowedCharacterCount,omitempty"`

	// LimitType: Type of limitation on this layer. "limited" or "unlimited"
	// for the "copy" layer.
	LimitType string `json:"limitType,omitempty"`

	// RemainingCharacterCount: Remaining allowed characters on this layer,
	// especially for the "copy" layer.
	RemainingCharacterCount int64 `json:"remainingCharacterCount,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "AllowedCharacterCount") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllowedCharacterCount") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *AnnotationLayerSummary) MarshalJSON() ([]byte, error) {
	type noMethod AnnotationLayerSummary
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Annotationdata struct {
	// AnnotationType: The type of annotation this data is for.
	AnnotationType string `json:"annotationType,omitempty"`

	Data interface{} `json:"data,omitempty"`

	// EncodedData: Base64 encoded data for this annotation data.
	EncodedData string `json:"encoded_data,omitempty"`

	// Id: Unique id for this annotation data.
	Id string `json:"id,omitempty"`

	// Kind: Resource Type
	Kind string `json:"kind,omitempty"`

	// LayerId: The Layer id for this data. *
	LayerId string `json:"layerId,omitempty"`

	// SelfLink: URL for this resource. *
	SelfLink string `json:"selfLink,omitempty"`

	// Updated: Timestamp for the last time this data was updated. (RFC 3339
	// UTC date-time format).
	Updated string `json:"updated,omitempty"`

	// VolumeId: The volume id for this data. *
	VolumeId string `json:"volumeId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AnnotationType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AnnotationType") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Annotationdata) MarshalJSON() ([]byte, error) {
	type noMethod Annotationdata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Annotations struct {
	// Items: A list of annotations.
	Items []*Annotation `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token to pass in for pagination for the next page.
	// This will not be present if this request does not have more results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalItems: Total number of annotations found. This may be greater
	// than the number of notes returned in this response if results have
	// been paginated.
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

func (s *Annotations) MarshalJSON() ([]byte, error) {
	type noMethod Annotations
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AnnotationsSummary struct {
	Kind string `json:"kind,omitempty"`

	Layers []*AnnotationsSummaryLayers `json:"layers,omitempty"`

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

func (s *AnnotationsSummary) MarshalJSON() ([]byte, error) {
	type noMethod AnnotationsSummary
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type AnnotationsSummaryLayers struct {
	AllowedCharacterCount int64 `json:"allowedCharacterCount,omitempty"`

	LayerId string `json:"layerId,omitempty"`

	LimitType string `json:"limitType,omitempty"`

	RemainingCharacterCount int64 `json:"remainingCharacterCount,omitempty"`

	Updated string `json:"updated,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "AllowedCharacterCount") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllowedCharacterCount") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *AnnotationsSummaryLayers) MarshalJSON() ([]byte, error) {
	type noMethod AnnotationsSummaryLayers
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Annotationsdata struct {
	// Items: A list of Annotation Data.
	Items []*Annotationdata `json:"items,omitempty"`

	// Kind: Resource type
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token to pass in for pagination for the next page.
	// This will not be present if this request does not have more results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalItems: The total number of volume annotations found.
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

func (s *Annotationsdata) MarshalJSON() ([]byte, error) {
	type noMethod Annotationsdata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type BooksAnnotationsRange struct {
	// EndOffset: The offset from the ending position.
	EndOffset string `json:"endOffset,omitempty"`

	// EndPosition: The ending position for the range.
	EndPosition string `json:"endPosition,omitempty"`

	// StartOffset: The offset from the starting position.
	StartOffset string `json:"startOffset,omitempty"`

	// StartPosition: The starting position for the range.
	StartPosition string `json:"startPosition,omitempty"`

	// ForceSendFields is a list of field names (e.g. "EndOffset") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EndOffset") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *BooksAnnotationsRange) MarshalJSON() ([]byte, error) {
	type noMethod BooksAnnotationsRange
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type BooksCloudloadingResource struct {
	Author string `json:"author,omitempty"`

	ProcessingState string `json:"processingState,omitempty"`

	Title string `json:"title,omitempty"`

	VolumeId string `json:"volumeId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Author") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Author") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *BooksCloudloadingResource) MarshalJSON() ([]byte, error) {
	type noMethod BooksCloudloadingResource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type BooksVolumesRecommendedRateResponse struct {
	ConsistencyToken string `json:"consistency_token,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ConsistencyToken") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ConsistencyToken") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *BooksVolumesRecommendedRateResponse) MarshalJSON() ([]byte, error) {
	type noMethod BooksVolumesRecommendedRateResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Bookshelf struct {
	// Access: Whether this bookshelf is PUBLIC or PRIVATE.
	Access string `json:"access,omitempty"`

	// Created: Created time for this bookshelf (formatted UTC timestamp
	// with millisecond resolution).
	Created string `json:"created,omitempty"`

	// Description: Description of this bookshelf.
	Description string `json:"description,omitempty"`

	// Id: Id of this bookshelf, only unique by user.
	Id int64 `json:"id,omitempty"`

	// Kind: Resource type for bookshelf metadata.
	Kind string `json:"kind,omitempty"`

	// SelfLink: URL to this resource.
	SelfLink string `json:"selfLink,omitempty"`

	// Title: Title of this bookshelf.
	Title string `json:"title,omitempty"`

	// Updated: Last modified time of this bookshelf (formatted UTC
	// timestamp with millisecond resolution).
	Updated string `json:"updated,omitempty"`

	// VolumeCount: Number of volumes in this bookshelf.
	VolumeCount int64 `json:"volumeCount,omitempty"`

	// VolumesLastUpdated: Last time a volume was added or removed from this
	// bookshelf (formatted UTC timestamp with millisecond resolution).
	VolumesLastUpdated string `json:"volumesLastUpdated,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Access") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Access") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Bookshelf) MarshalJSON() ([]byte, error) {
	type noMethod Bookshelf
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Bookshelves struct {
	// Items: A list of bookshelves.
	Items []*Bookshelf `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

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

func (s *Bookshelves) MarshalJSON() ([]byte, error) {
	type noMethod Bookshelves
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Category struct {
	// Items: A list of onboarding categories.
	Items []*CategoryItems `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

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

func (s *Category) MarshalJSON() ([]byte, error) {
	type noMethod Category
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type CategoryItems struct {
	BadgeUrl string `json:"badgeUrl,omitempty"`

	CategoryId string `json:"categoryId,omitempty"`

	Name string `json:"name,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BadgeUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BadgeUrl") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CategoryItems) MarshalJSON() ([]byte, error) {
	type noMethod CategoryItems
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ConcurrentAccessRestriction struct {
	// DeviceAllowed: Whether access is granted for this (user, device,
	// volume).
	DeviceAllowed bool `json:"deviceAllowed,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// MaxConcurrentDevices: The maximum number of concurrent access
	// licenses for this volume.
	MaxConcurrentDevices int64 `json:"maxConcurrentDevices,omitempty"`

	// Message: Error/warning message.
	Message string `json:"message,omitempty"`

	// Nonce: Client nonce for verification. Download access and
	// client-validation only.
	Nonce string `json:"nonce,omitempty"`

	// ReasonCode: Error/warning reason code.
	ReasonCode string `json:"reasonCode,omitempty"`

	// Restricted: Whether this volume has any concurrent access
	// restrictions.
	Restricted bool `json:"restricted,omitempty"`

	// Signature: Response signature.
	Signature string `json:"signature,omitempty"`

	// Source: Client app identifier for verification. Download access and
	// client-validation only.
	Source string `json:"source,omitempty"`

	// TimeWindowSeconds: Time in seconds for license auto-expiration.
	TimeWindowSeconds int64 `json:"timeWindowSeconds,omitempty"`

	// VolumeId: Identifies the volume for which this entry applies.
	VolumeId string `json:"volumeId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeviceAllowed") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeviceAllowed") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ConcurrentAccessRestriction) MarshalJSON() ([]byte, error) {
	type noMethod ConcurrentAccessRestriction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Dictlayerdata struct {
	Common *DictlayerdataCommon `json:"common,omitempty"`

	Dict *DictlayerdataDict `json:"dict,omitempty"`

	Kind string `json:"kind,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Common") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Common") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Dictlayerdata) MarshalJSON() ([]byte, error) {
	type noMethod Dictlayerdata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataCommon struct {
	// Title: The display title and localized canonical name to use when
	// searching for this entity on Google search.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Title") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Title") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataCommon) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataCommon
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDict struct {
	// Source: The source, url and attribution for this dictionary data.
	Source *DictlayerdataDictSource `json:"source,omitempty"`

	Words []*DictlayerdataDictWords `json:"words,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Source") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Source") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDict) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDict
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DictlayerdataDictSource: The source, url and attribution for this
// dictionary data.
type DictlayerdataDictSource struct {
	Attribution string `json:"attribution,omitempty"`

	Url string `json:"url,omitempty"`

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

func (s *DictlayerdataDictSource) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWords struct {
	Derivatives []*DictlayerdataDictWordsDerivatives `json:"derivatives,omitempty"`

	Examples []*DictlayerdataDictWordsExamples `json:"examples,omitempty"`

	Senses []*DictlayerdataDictWordsSenses `json:"senses,omitempty"`

	// Source: The words with different meanings but not related words, e.g.
	// "go" (game) and "go" (verb).
	Source *DictlayerdataDictWordsSource `json:"source,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Derivatives") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Derivatives") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWords) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWords
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsDerivatives struct {
	Source *DictlayerdataDictWordsDerivativesSource `json:"source,omitempty"`

	Text string `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Source") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Source") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWordsDerivatives) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsDerivatives
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsDerivativesSource struct {
	Attribution string `json:"attribution,omitempty"`

	Url string `json:"url,omitempty"`

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

func (s *DictlayerdataDictWordsDerivativesSource) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsDerivativesSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsExamples struct {
	Source *DictlayerdataDictWordsExamplesSource `json:"source,omitempty"`

	Text string `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Source") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Source") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWordsExamples) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsExamples
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsExamplesSource struct {
	Attribution string `json:"attribution,omitempty"`

	Url string `json:"url,omitempty"`

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

func (s *DictlayerdataDictWordsExamplesSource) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsExamplesSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSenses struct {
	Conjugations []*DictlayerdataDictWordsSensesConjugations `json:"conjugations,omitempty"`

	Definitions []*DictlayerdataDictWordsSensesDefinitions `json:"definitions,omitempty"`

	PartOfSpeech string `json:"partOfSpeech,omitempty"`

	Pronunciation string `json:"pronunciation,omitempty"`

	PronunciationUrl string `json:"pronunciationUrl,omitempty"`

	Source *DictlayerdataDictWordsSensesSource `json:"source,omitempty"`

	Syllabification string `json:"syllabification,omitempty"`

	Synonyms []*DictlayerdataDictWordsSensesSynonyms `json:"synonyms,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Conjugations") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Conjugations") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWordsSenses) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSenses
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSensesConjugations struct {
	Type string `json:"type,omitempty"`

	Value string `json:"value,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Type") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Type") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWordsSensesConjugations) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSensesConjugations
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSensesDefinitions struct {
	Definition string `json:"definition,omitempty"`

	Examples []*DictlayerdataDictWordsSensesDefinitionsExamples `json:"examples,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Definition") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Definition") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWordsSensesDefinitions) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSensesDefinitions
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSensesDefinitionsExamples struct {
	Source *DictlayerdataDictWordsSensesDefinitionsExamplesSource `json:"source,omitempty"`

	Text string `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Source") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Source") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWordsSensesDefinitionsExamples) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSensesDefinitionsExamples
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSensesDefinitionsExamplesSource struct {
	Attribution string `json:"attribution,omitempty"`

	Url string `json:"url,omitempty"`

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

func (s *DictlayerdataDictWordsSensesDefinitionsExamplesSource) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSensesDefinitionsExamplesSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSensesSource struct {
	Attribution string `json:"attribution,omitempty"`

	Url string `json:"url,omitempty"`

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

func (s *DictlayerdataDictWordsSensesSource) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSensesSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSensesSynonyms struct {
	Source *DictlayerdataDictWordsSensesSynonymsSource `json:"source,omitempty"`

	Text string `json:"text,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Source") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Source") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DictlayerdataDictWordsSensesSynonyms) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSensesSynonyms
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DictlayerdataDictWordsSensesSynonymsSource struct {
	Attribution string `json:"attribution,omitempty"`

	Url string `json:"url,omitempty"`

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

func (s *DictlayerdataDictWordsSensesSynonymsSource) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSensesSynonymsSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DictlayerdataDictWordsSource: The words with different meanings but
// not related words, e.g. "go" (game) and "go" (verb).
type DictlayerdataDictWordsSource struct {
	Attribution string `json:"attribution,omitempty"`

	Url string `json:"url,omitempty"`

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

func (s *DictlayerdataDictWordsSource) MarshalJSON() ([]byte, error) {
	type noMethod DictlayerdataDictWordsSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Discoveryclusters struct {
	Clusters []*DiscoveryclustersClusters `json:"clusters,omitempty"`

	// Kind: Resorce type.
	Kind string `json:"kind,omitempty"`

	TotalClusters int64 `json:"totalClusters,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Clusters") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Clusters") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Discoveryclusters) MarshalJSON() ([]byte, error) {
	type noMethod Discoveryclusters
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DiscoveryclustersClusters struct {
	BannerWithContentContainer *DiscoveryclustersClustersBannerWithContentContainer `json:"banner_with_content_container,omitempty"`

	SubTitle string `json:"subTitle,omitempty"`

	Title string `json:"title,omitempty"`

	TotalVolumes int64 `json:"totalVolumes,omitempty"`

	Uid string `json:"uid,omitempty"`

	Volumes []*Volume `json:"volumes,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "BannerWithContentContainer") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g.
	// "BannerWithContentContainer") to include in API requests with the
	// JSON null value. By default, fields with empty values are omitted
	// from API requests. However, any field with an empty value appearing
	// in NullFields will be sent to the server as null. It is an error if a
	// field in this list has a non-empty value. This may be used to include
	// null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DiscoveryclustersClusters) MarshalJSON() ([]byte, error) {
	type noMethod DiscoveryclustersClusters
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DiscoveryclustersClustersBannerWithContentContainer struct {
	FillColorArgb string `json:"fillColorArgb,omitempty"`

	ImageUrl string `json:"imageUrl,omitempty"`

	MaskColorArgb string `json:"maskColorArgb,omitempty"`

	MoreButtonText string `json:"moreButtonText,omitempty"`

	MoreButtonUrl string `json:"moreButtonUrl,omitempty"`

	TextColorArgb string `json:"textColorArgb,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FillColorArgb") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FillColorArgb") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DiscoveryclustersClustersBannerWithContentContainer) MarshalJSON() ([]byte, error) {
	type noMethod DiscoveryclustersClustersBannerWithContentContainer
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DownloadAccessRestriction struct {
	// DeviceAllowed: If restricted, whether access is granted for this
	// (user, device, volume).
	DeviceAllowed bool `json:"deviceAllowed,omitempty"`

	// DownloadsAcquired: If restricted, the number of content download
	// licenses already acquired (including the requesting client, if
	// licensed).
	DownloadsAcquired int64 `json:"downloadsAcquired,omitempty"`

	// JustAcquired: If deviceAllowed, whether access was just acquired with
	// this request.
	JustAcquired bool `json:"justAcquired,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// MaxDownloadDevices: If restricted, the maximum number of content
	// download licenses for this volume.
	MaxDownloadDevices int64 `json:"maxDownloadDevices,omitempty"`

	// Message: Error/warning message.
	Message string `json:"message,omitempty"`

	// Nonce: Client nonce for verification. Download access and
	// client-validation only.
	Nonce string `json:"nonce,omitempty"`

	// ReasonCode: Error/warning reason code. Additional codes may be added
	// in the future. 0 OK 100 ACCESS_DENIED_PUBLISHER_LIMIT 101
	// ACCESS_DENIED_LIMIT 200 WARNING_USED_LAST_ACCESS
	ReasonCode string `json:"reasonCode,omitempty"`

	// Restricted: Whether this volume has any download access restrictions.
	Restricted bool `json:"restricted,omitempty"`

	// Signature: Response signature.
	Signature string `json:"signature,omitempty"`

	// Source: Client app identifier for verification. Download access and
	// client-validation only.
	Source string `json:"source,omitempty"`

	// VolumeId: Identifies the volume for which this entry applies.
	VolumeId string `json:"volumeId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DeviceAllowed") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DeviceAllowed") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DownloadAccessRestriction) MarshalJSON() ([]byte, error) {
	type noMethod DownloadAccessRestriction
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type DownloadAccesses struct {
	// DownloadAccessList: A list of download access responses.
	DownloadAccessList []*DownloadAccessRestriction `json:"downloadAccessList,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "DownloadAccessList")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DownloadAccessList") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *DownloadAccesses) MarshalJSON() ([]byte, error) {
	type noMethod DownloadAccesses
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Geolayerdata struct {
	Common *GeolayerdataCommon `json:"common,omitempty"`

	Geo *GeolayerdataGeo `json:"geo,omitempty"`

	Kind string `json:"kind,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Common") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Common") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Geolayerdata) MarshalJSON() ([]byte, error) {
	type noMethod Geolayerdata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GeolayerdataCommon struct {
	// Lang: The language of the information url and description.
	Lang string `json:"lang,omitempty"`

	// PreviewImageUrl: The URL for the preview image information.
	PreviewImageUrl string `json:"previewImageUrl,omitempty"`

	// Snippet: The description for this location.
	Snippet string `json:"snippet,omitempty"`

	// SnippetUrl: The URL for information for this location. Ex: wikipedia
	// link.
	SnippetUrl string `json:"snippetUrl,omitempty"`

	// Title: The display title and localized canonical name to use when
	// searching for this entity on Google search.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Lang") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Lang") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeolayerdataCommon) MarshalJSON() ([]byte, error) {
	type noMethod GeolayerdataCommon
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GeolayerdataGeo struct {
	// Boundary: The boundary of the location as a set of loops containing
	// pairs of latitude, longitude coordinates.
	Boundary [][]*GeolayerdataGeoBoundaryItem `json:"boundary,omitempty"`

	// CachePolicy: The cache policy active for this data. EX: UNRESTRICTED,
	// RESTRICTED, NEVER
	CachePolicy string `json:"cachePolicy,omitempty"`

	// CountryCode: The country code of the location.
	CountryCode string `json:"countryCode,omitempty"`

	// Latitude: The latitude of the location.
	Latitude float64 `json:"latitude,omitempty"`

	// Longitude: The longitude of the location.
	Longitude float64 `json:"longitude,omitempty"`

	// MapType: The type of map that should be used for this location. EX:
	// HYBRID, ROADMAP, SATELLITE, TERRAIN
	MapType string `json:"mapType,omitempty"`

	// Viewport: The viewport for showing this location. This is a latitude,
	// longitude rectangle.
	Viewport *GeolayerdataGeoViewport `json:"viewport,omitempty"`

	// Zoom: The Zoom level to use for the map. Zoom levels between 0 (the
	// lowest zoom level, in which the entire world can be seen on one map)
	// to 21+ (down to individual buildings). See:
	// https://developers.google.com/maps/documentation/staticmaps/#Zoomlevels
	Zoom int64 `json:"zoom,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Boundary") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Boundary") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeolayerdataGeo) MarshalJSON() ([]byte, error) {
	type noMethod GeolayerdataGeo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GeolayerdataGeoBoundaryItem struct {
	Latitude int64 `json:"latitude,omitempty"`

	Longitude int64 `json:"longitude,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Latitude") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Latitude") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeolayerdataGeoBoundaryItem) MarshalJSON() ([]byte, error) {
	type noMethod GeolayerdataGeoBoundaryItem
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GeolayerdataGeoViewport: The viewport for showing this location. This
// is a latitude, longitude rectangle.
type GeolayerdataGeoViewport struct {
	Hi *GeolayerdataGeoViewportHi `json:"hi,omitempty"`

	Lo *GeolayerdataGeoViewportLo `json:"lo,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Hi") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Hi") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeolayerdataGeoViewport) MarshalJSON() ([]byte, error) {
	type noMethod GeolayerdataGeoViewport
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GeolayerdataGeoViewportHi struct {
	Latitude float64 `json:"latitude,omitempty"`

	Longitude float64 `json:"longitude,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Latitude") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Latitude") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeolayerdataGeoViewportHi) MarshalJSON() ([]byte, error) {
	type noMethod GeolayerdataGeoViewportHi
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type GeolayerdataGeoViewportLo struct {
	Latitude float64 `json:"latitude,omitempty"`

	Longitude float64 `json:"longitude,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Latitude") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Latitude") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GeolayerdataGeoViewportLo) MarshalJSON() ([]byte, error) {
	type noMethod GeolayerdataGeoViewportLo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Layersummaries struct {
	// Items: A list of layer summary items.
	Items []*Layersummary `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// TotalItems: The total number of layer summaries found.
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

func (s *Layersummaries) MarshalJSON() ([]byte, error) {
	type noMethod Layersummaries
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Layersummary struct {
	// AnnotationCount: The number of annotations for this layer.
	AnnotationCount int64 `json:"annotationCount,omitempty"`

	// AnnotationTypes: The list of annotation types contained for this
	// layer.
	AnnotationTypes []string `json:"annotationTypes,omitempty"`

	// AnnotationsDataLink: Link to get data for this annotation.
	AnnotationsDataLink string `json:"annotationsDataLink,omitempty"`

	// AnnotationsLink: The link to get the annotations for this layer.
	AnnotationsLink string `json:"annotationsLink,omitempty"`

	// ContentVersion: The content version this resource is for.
	ContentVersion string `json:"contentVersion,omitempty"`

	// DataCount: The number of data items for this layer.
	DataCount int64 `json:"dataCount,omitempty"`

	// Id: Unique id of this layer summary.
	Id string `json:"id,omitempty"`

	// Kind: Resource Type
	Kind string `json:"kind,omitempty"`

	// LayerId: The layer id for this summary.
	LayerId string `json:"layerId,omitempty"`

	// SelfLink: URL to this resource.
	SelfLink string `json:"selfLink,omitempty"`

	// Updated: Timestamp for the last time an item in this layer was
	// updated. (RFC 3339 UTC date-time format).
	Updated string `json:"updated,omitempty"`

	// VolumeAnnotationsVersion: The current version of this layer's volume
	// annotations. Note that this version applies only to the data in the
	// books.layers.volumeAnnotations.* responses. The actual annotation
	// data is versioned separately.
	VolumeAnnotationsVersion string `json:"volumeAnnotationsVersion,omitempty"`

	// VolumeId: The volume id this resource is for.
	VolumeId string `json:"volumeId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AnnotationCount") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AnnotationCount") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Layersummary) MarshalJSON() ([]byte, error) {
	type noMethod Layersummary
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Metadata struct {
	// Items: A list of offline dictionary metadata.
	Items []*MetadataItems `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

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

func (s *Metadata) MarshalJSON() ([]byte, error) {
	type noMethod Metadata
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type MetadataItems struct {
	DownloadUrl string `json:"download_url,omitempty"`

	EncryptedKey string `json:"encrypted_key,omitempty"`

	Language string `json:"language,omitempty"`

	Size int64 `json:"size,omitempty,string"`

	Version int64 `json:"version,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "DownloadUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DownloadUrl") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *MetadataItems) MarshalJSON() ([]byte, error) {
	type noMethod MetadataItems
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Notification struct {
	Body string `json:"body,omitempty"`

	// CrmExperimentIds: The list of crm experiment ids.
	CrmExperimentIds googleapi.Int64s `json:"crmExperimentIds,omitempty"`

	DocId string `json:"doc_id,omitempty"`

	DocType string `json:"doc_type,omitempty"`

	DontShowNotification bool `json:"dont_show_notification,omitempty"`

	IconUrl string `json:"iconUrl,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	NotificationType string `json:"notification_type,omitempty"`

	PcampaignId string `json:"pcampaign_id,omitempty"`

	Reason string `json:"reason,omitempty"`

	ShowNotificationSettingsAction bool `json:"show_notification_settings_action,omitempty"`

	TargetUrl string `json:"targetUrl,omitempty"`

	Title string `json:"title,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Body") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Body") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Notification) MarshalJSON() ([]byte, error) {
	type noMethod Notification
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Offers struct {
	// Items: A list of offers.
	Items []*OffersItems `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

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

func (s *Offers) MarshalJSON() ([]byte, error) {
	type noMethod Offers
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type OffersItems struct {
	ArtUrl string `json:"artUrl,omitempty"`

	GservicesKey string `json:"gservicesKey,omitempty"`

	Id string `json:"id,omitempty"`

	Items []*OffersItemsItems `json:"items,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ArtUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ArtUrl") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OffersItems) MarshalJSON() ([]byte, error) {
	type noMethod OffersItems
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type OffersItemsItems struct {
	Author string `json:"author,omitempty"`

	CanonicalVolumeLink string `json:"canonicalVolumeLink,omitempty"`

	CoverUrl string `json:"coverUrl,omitempty"`

	Description string `json:"description,omitempty"`

	Title string `json:"title,omitempty"`

	VolumeId string `json:"volumeId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Author") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Author") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *OffersItemsItems) MarshalJSON() ([]byte, error) {
	type noMethod OffersItemsItems
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ReadingPosition struct {
	// EpubCfiPosition: Position in an EPUB as a CFI.
	EpubCfiPosition string `json:"epubCfiPosition,omitempty"`

	// GbImagePosition: Position in a volume for image-based content.
	GbImagePosition string `json:"gbImagePosition,omitempty"`

	// GbTextPosition: Position in a volume for text-based content.
	GbTextPosition string `json:"gbTextPosition,omitempty"`

	// Kind: Resource type for a reading position.
	Kind string `json:"kind,omitempty"`

	// PdfPosition: Position in a PDF file.
	PdfPosition string `json:"pdfPosition,omitempty"`

	// Updated: Timestamp when this reading position was last updated
	// (formatted UTC timestamp with millisecond resolution).
	Updated string `json:"updated,omitempty"`

	// VolumeId: Volume id associated with this reading position.
	VolumeId string `json:"volumeId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "EpubCfiPosition") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EpubCfiPosition") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ReadingPosition) MarshalJSON() ([]byte, error) {
	type noMethod ReadingPosition
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type RequestAccess struct {
	// ConcurrentAccess: A concurrent access response.
	ConcurrentAccess *ConcurrentAccessRestriction `json:"concurrentAccess,omitempty"`

	// DownloadAccess: A download access response.
	DownloadAccess *DownloadAccessRestriction `json:"downloadAccess,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ConcurrentAccess") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ConcurrentAccess") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *RequestAccess) MarshalJSON() ([]byte, error) {
	type noMethod RequestAccess
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Review struct {
	// Author: Author of this review.
	Author *ReviewAuthor `json:"author,omitempty"`

	// Content: Review text.
	Content string `json:"content,omitempty"`

	// Date: Date of this review.
	Date string `json:"date,omitempty"`

	// FullTextUrl: URL for the full review text, for reviews gathered from
	// the web.
	FullTextUrl string `json:"fullTextUrl,omitempty"`

	// Kind: Resource type for a review.
	Kind string `json:"kind,omitempty"`

	// Rating: Star rating for this review. Possible values are ONE, TWO,
	// THREE, FOUR, FIVE or NOT_RATED.
	Rating string `json:"rating,omitempty"`

	// Source: Information regarding the source of this review, when the
	// review is not from a Google Books user.
	Source *ReviewSource `json:"source,omitempty"`

	// Title: Title for this review.
	Title string `json:"title,omitempty"`

	// Type: Source type for this review. Possible values are EDITORIAL,
	// WEB_USER or GOOGLE_USER.
	Type string `json:"type,omitempty"`

	// VolumeId: Volume that this review is for.
	VolumeId string `json:"volumeId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Author") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Author") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Review) MarshalJSON() ([]byte, error) {
	type noMethod Review
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReviewAuthor: Author of this review.
type ReviewAuthor struct {
	// DisplayName: Name of this person.
	DisplayName string `json:"displayName,omitempty"`

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

func (s *ReviewAuthor) MarshalJSON() ([]byte, error) {
	type noMethod ReviewAuthor
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ReviewSource: Information regarding the source of this review, when
// the review is not from a Google Books user.
type ReviewSource struct {
	// Description: Name of the source.
	Description string `json:"description,omitempty"`

	// ExtraDescription: Extra text about the source of the review.
	ExtraDescription string `json:"extraDescription,omitempty"`

	// Url: URL of the source of the review.
	Url string `json:"url,omitempty"`

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

func (s *ReviewSource) MarshalJSON() ([]byte, error) {
	type noMethod ReviewSource
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Series struct {
	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// Series: Series info list. The client always expects this element in
	// the JSON output, hence declared here as OutputAlways.
	Series []*SeriesSeries `json:"series,omitempty"`

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

func (s *Series) MarshalJSON() ([]byte, error) {
	type noMethod Series
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SeriesSeries struct {
	BannerImageUrl string `json:"bannerImageUrl,omitempty"`

	ImageUrl string `json:"imageUrl,omitempty"`

	SeriesId string `json:"seriesId,omitempty"`

	SeriesType string `json:"seriesType,omitempty"`

	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BannerImageUrl") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BannerImageUrl") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SeriesSeries) MarshalJSON() ([]byte, error) {
	type noMethod SeriesSeries
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Seriesmembership struct {
	// Kind: Resorce type.
	Kind string `json:"kind,omitempty"`

	Member []*Volume `json:"member,omitempty"`

	NextPageToken string `json:"nextPageToken,omitempty"`

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

func (s *Seriesmembership) MarshalJSON() ([]byte, error) {
	type noMethod Seriesmembership
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Usersettings struct {
	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// NotesExport: User settings in sub-objects, each for different
	// purposes.
	NotesExport *UsersettingsNotesExport `json:"notesExport,omitempty"`

	Notification *UsersettingsNotification `json:"notification,omitempty"`

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

func (s *Usersettings) MarshalJSON() ([]byte, error) {
	type noMethod Usersettings
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UsersettingsNotesExport: User settings in sub-objects, each for
// different purposes.
type UsersettingsNotesExport struct {
	FolderName string `json:"folderName,omitempty"`

	IsEnabled bool `json:"isEnabled,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FolderName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FolderName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UsersettingsNotesExport) MarshalJSON() ([]byte, error) {
	type noMethod UsersettingsNotesExport
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type UsersettingsNotification struct {
	MoreFromAuthors *UsersettingsNotificationMoreFromAuthors `json:"moreFromAuthors,omitempty"`

	MoreFromSeries *UsersettingsNotificationMoreFromSeries `json:"moreFromSeries,omitempty"`

	// ForceSendFields is a list of field names (e.g. "MoreFromAuthors") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "MoreFromAuthors") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *UsersettingsNotification) MarshalJSON() ([]byte, error) {
	type noMethod UsersettingsNotification
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type UsersettingsNotificationMoreFromAuthors struct {
	OptedState string `json:"opted_state,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OptedState") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OptedState") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UsersettingsNotificationMoreFromAuthors) MarshalJSON() ([]byte, error) {
	type noMethod UsersettingsNotificationMoreFromAuthors
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type UsersettingsNotificationMoreFromSeries struct {
	OptedState string `json:"opted_state,omitempty"`

	// ForceSendFields is a list of field names (e.g. "OptedState") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "OptedState") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UsersettingsNotificationMoreFromSeries) MarshalJSON() ([]byte, error) {
	type noMethod UsersettingsNotificationMoreFromSeries
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Volume struct {
	// AccessInfo: Any information about a volume related to reading or
	// obtaining that volume text. This information can depend on country
	// (books may be public domain in one country but not in another, e.g.).
	AccessInfo *VolumeAccessInfo `json:"accessInfo,omitempty"`

	// Etag: Opaque identifier for a specific version of a volume resource.
	// (In LITE projection)
	Etag string `json:"etag,omitempty"`

	// Id: Unique identifier for a volume. (In LITE projection.)
	Id string `json:"id,omitempty"`

	// Kind: Resource type for a volume. (In LITE projection.)
	Kind string `json:"kind,omitempty"`

	// LayerInfo: What layers exist in this volume and high level
	// information about them.
	LayerInfo *VolumeLayerInfo `json:"layerInfo,omitempty"`

	// RecommendedInfo: Recommendation related information for this volume.
	RecommendedInfo *VolumeRecommendedInfo `json:"recommendedInfo,omitempty"`

	// SaleInfo: Any information about a volume related to the eBookstore
	// and/or purchaseability. This information can depend on the country
	// where the request originates from (i.e. books may not be for sale in
	// certain countries).
	SaleInfo *VolumeSaleInfo `json:"saleInfo,omitempty"`

	// SearchInfo: Search result information related to this volume.
	SearchInfo *VolumeSearchInfo `json:"searchInfo,omitempty"`

	// SelfLink: URL to this resource. (In LITE projection.)
	SelfLink string `json:"selfLink,omitempty"`

	// UserInfo: User specific information related to this volume. (e.g.
	// page this user last read or whether they purchased this book)
	UserInfo *VolumeUserInfo `json:"userInfo,omitempty"`

	// VolumeInfo: General volume information.
	VolumeInfo *VolumeVolumeInfo `json:"volumeInfo,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AccessInfo") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccessInfo") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Volume) MarshalJSON() ([]byte, error) {
	type noMethod Volume
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeAccessInfo: Any information about a volume related to reading
// or obtaining that volume text. This information can depend on country
// (books may be public domain in one country but not in another, e.g.).
type VolumeAccessInfo struct {
	// AccessViewStatus: Combines the access and viewability of this volume
	// into a single status field for this user. Values can be
	// FULL_PURCHASED, FULL_PUBLIC_DOMAIN, SAMPLE or NONE. (In LITE
	// projection.)
	AccessViewStatus string `json:"accessViewStatus,omitempty"`

	// Country: The two-letter ISO_3166-1 country code for which this access
	// information is valid. (In LITE projection.)
	Country string `json:"country,omitempty"`

	// DownloadAccess: Information about a volume's download license access
	// restrictions.
	DownloadAccess *DownloadAccessRestriction `json:"downloadAccess,omitempty"`

	// DriveImportedContentLink: URL to the Google Drive viewer if this
	// volume is uploaded by the user by selecting the file from Google
	// Drive.
	DriveImportedContentLink string `json:"driveImportedContentLink,omitempty"`

	// Embeddable: Whether this volume can be embedded in a viewport using
	// the Embedded Viewer API.
	Embeddable bool `json:"embeddable,omitempty"`

	// Epub: Information about epub content. (In LITE projection.)
	Epub *VolumeAccessInfoEpub `json:"epub,omitempty"`

	// ExplicitOfflineLicenseManagement: Whether this volume requires that
	// the client explicitly request offline download license rather than
	// have it done automatically when loading the content, if the client
	// supports it.
	ExplicitOfflineLicenseManagement bool `json:"explicitOfflineLicenseManagement,omitempty"`

	// Pdf: Information about pdf content. (In LITE projection.)
	Pdf *VolumeAccessInfoPdf `json:"pdf,omitempty"`

	// PublicDomain: Whether or not this book is public domain in the
	// country listed above.
	PublicDomain bool `json:"publicDomain,omitempty"`

	// QuoteSharingAllowed: Whether quote sharing is allowed for this
	// volume.
	QuoteSharingAllowed bool `json:"quoteSharingAllowed,omitempty"`

	// TextToSpeechPermission: Whether text-to-speech is permitted for this
	// volume. Values can be ALLOWED, ALLOWED_FOR_ACCESSIBILITY, or
	// NOT_ALLOWED.
	TextToSpeechPermission string `json:"textToSpeechPermission,omitempty"`

	// ViewOrderUrl: For ordered but not yet processed orders, we give a URL
	// that can be used to go to the appropriate Google Wallet page.
	ViewOrderUrl string `json:"viewOrderUrl,omitempty"`

	// Viewability: The read access of a volume. Possible values are
	// PARTIAL, ALL_PAGES, NO_PAGES or UNKNOWN. This value depends on the
	// country listed above. A value of PARTIAL means that the publisher has
	// allowed some portion of the volume to be viewed publicly, without
	// purchase. This can apply to eBooks as well as non-eBooks. Public
	// domain books will always have a value of ALL_PAGES.
	Viewability string `json:"viewability,omitempty"`

	// WebReaderLink: URL to read this volume on the Google Books site. Link
	// will not allow users to read non-viewable volumes.
	WebReaderLink string `json:"webReaderLink,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AccessViewStatus") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AccessViewStatus") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeAccessInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeAccessInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeAccessInfoEpub: Information about epub content. (In LITE
// projection.)
type VolumeAccessInfoEpub struct {
	// AcsTokenLink: URL to retrieve ACS token for epub download. (In LITE
	// projection.)
	AcsTokenLink string `json:"acsTokenLink,omitempty"`

	// DownloadLink: URL to download epub. (In LITE projection.)
	DownloadLink string `json:"downloadLink,omitempty"`

	// IsAvailable: Is a flowing text epub available either as public domain
	// or for purchase. (In LITE projection.)
	IsAvailable bool `json:"isAvailable,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AcsTokenLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AcsTokenLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeAccessInfoEpub) MarshalJSON() ([]byte, error) {
	type noMethod VolumeAccessInfoEpub
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeAccessInfoPdf: Information about pdf content. (In LITE
// projection.)
type VolumeAccessInfoPdf struct {
	// AcsTokenLink: URL to retrieve ACS token for pdf download. (In LITE
	// projection.)
	AcsTokenLink string `json:"acsTokenLink,omitempty"`

	// DownloadLink: URL to download pdf. (In LITE projection.)
	DownloadLink string `json:"downloadLink,omitempty"`

	// IsAvailable: Is a scanned image pdf available either as public domain
	// or for purchase. (In LITE projection.)
	IsAvailable bool `json:"isAvailable,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AcsTokenLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AcsTokenLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeAccessInfoPdf) MarshalJSON() ([]byte, error) {
	type noMethod VolumeAccessInfoPdf
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeLayerInfo: What layers exist in this volume and high level
// information about them.
type VolumeLayerInfo struct {
	// Layers: A layer should appear here if and only if the layer exists
	// for this book.
	Layers []*VolumeLayerInfoLayers `json:"layers,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Layers") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Layers") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeLayerInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeLayerInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type VolumeLayerInfoLayers struct {
	// LayerId: The layer id of this layer (e.g. "geo").
	LayerId string `json:"layerId,omitempty"`

	// VolumeAnnotationsVersion: The current version of this layer's volume
	// annotations. Note that this version applies only to the data in the
	// books.layers.volumeAnnotations.* responses. The actual annotation
	// data is versioned separately.
	VolumeAnnotationsVersion string `json:"volumeAnnotationsVersion,omitempty"`

	// ForceSendFields is a list of field names (e.g. "LayerId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LayerId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeLayerInfoLayers) MarshalJSON() ([]byte, error) {
	type noMethod VolumeLayerInfoLayers
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeRecommendedInfo: Recommendation related information for this
// volume.
type VolumeRecommendedInfo struct {
	// Explanation: A text explaining why this volume is recommended.
	Explanation string `json:"explanation,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Explanation") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Explanation") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeRecommendedInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeRecommendedInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeSaleInfo: Any information about a volume related to the
// eBookstore and/or purchaseability. This information can depend on the
// country where the request originates from (i.e. books may not be for
// sale in certain countries).
type VolumeSaleInfo struct {
	// BuyLink: URL to purchase this volume on the Google Books site. (In
	// LITE projection)
	BuyLink string `json:"buyLink,omitempty"`

	// Country: The two-letter ISO_3166-1 country code for which this sale
	// information is valid. (In LITE projection.)
	Country string `json:"country,omitempty"`

	// IsEbook: Whether or not this volume is an eBook (can be added to the
	// My eBooks shelf).
	IsEbook bool `json:"isEbook,omitempty"`

	// ListPrice: Suggested retail price. (In LITE projection.)
	ListPrice *VolumeSaleInfoListPrice `json:"listPrice,omitempty"`

	// Offers: Offers available for this volume (sales and rentals).
	Offers []*VolumeSaleInfoOffers `json:"offers,omitempty"`

	// OnSaleDate: The date on which this book is available for sale.
	OnSaleDate string `json:"onSaleDate,omitempty"`

	// RetailPrice: The actual selling price of the book. This is the same
	// as the suggested retail or list price unless there are offers or
	// discounts on this volume. (In LITE projection.)
	RetailPrice *VolumeSaleInfoRetailPrice `json:"retailPrice,omitempty"`

	// Saleability: Whether or not this book is available for sale or
	// offered for free in the Google eBookstore for the country listed
	// above. Possible values are FOR_SALE, FOR_RENTAL_ONLY,
	// FOR_SALE_AND_RENTAL, FREE, NOT_FOR_SALE, or FOR_PREORDER.
	Saleability string `json:"saleability,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BuyLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BuyLink") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSaleInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSaleInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeSaleInfoListPrice: Suggested retail price. (In LITE
// projection.)
type VolumeSaleInfoListPrice struct {
	// Amount: Amount in the currency listed below. (In LITE projection.)
	Amount float64 `json:"amount,omitempty"`

	// CurrencyCode: An ISO 4217, three-letter currency code. (In LITE
	// projection.)
	CurrencyCode string `json:"currencyCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Amount") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Amount") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSaleInfoListPrice) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSaleInfoListPrice
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type VolumeSaleInfoOffers struct {
	// FinskyOfferType: The finsky offer type (e.g., PURCHASE=0 RENTAL=3)
	FinskyOfferType int64 `json:"finskyOfferType,omitempty"`

	// Giftable: Indicates whether the offer is giftable.
	Giftable bool `json:"giftable,omitempty"`

	// ListPrice: Offer list (=undiscounted) price in Micros.
	ListPrice *VolumeSaleInfoOffersListPrice `json:"listPrice,omitempty"`

	// RentalDuration: The rental duration (for rental offers only).
	RentalDuration *VolumeSaleInfoOffersRentalDuration `json:"rentalDuration,omitempty"`

	// RetailPrice: Offer retail (=discounted) price in Micros
	RetailPrice *VolumeSaleInfoOffersRetailPrice `json:"retailPrice,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FinskyOfferType") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FinskyOfferType") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSaleInfoOffers) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSaleInfoOffers
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeSaleInfoOffersListPrice: Offer list (=undiscounted) price in
// Micros.
type VolumeSaleInfoOffersListPrice struct {
	AmountInMicros float64 `json:"amountInMicros,omitempty"`

	CurrencyCode string `json:"currencyCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AmountInMicros") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AmountInMicros") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSaleInfoOffersListPrice) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSaleInfoOffersListPrice
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeSaleInfoOffersRentalDuration: The rental duration (for rental
// offers only).
type VolumeSaleInfoOffersRentalDuration struct {
	Count float64 `json:"count,omitempty"`

	Unit string `json:"unit,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Count") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Count") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSaleInfoOffersRentalDuration) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSaleInfoOffersRentalDuration
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeSaleInfoOffersRetailPrice: Offer retail (=discounted) price in
// Micros
type VolumeSaleInfoOffersRetailPrice struct {
	AmountInMicros float64 `json:"amountInMicros,omitempty"`

	CurrencyCode string `json:"currencyCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AmountInMicros") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AmountInMicros") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSaleInfoOffersRetailPrice) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSaleInfoOffersRetailPrice
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeSaleInfoRetailPrice: The actual selling price of the book. This
// is the same as the suggested retail or list price unless there are
// offers or discounts on this volume. (In LITE projection.)
type VolumeSaleInfoRetailPrice struct {
	// Amount: Amount in the currency listed below. (In LITE projection.)
	Amount float64 `json:"amount,omitempty"`

	// CurrencyCode: An ISO 4217, three-letter currency code. (In LITE
	// projection.)
	CurrencyCode string `json:"currencyCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Amount") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Amount") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSaleInfoRetailPrice) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSaleInfoRetailPrice
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeSearchInfo: Search result information related to this volume.
type VolumeSearchInfo struct {
	// TextSnippet: A text snippet containing the search query.
	TextSnippet string `json:"textSnippet,omitempty"`

	// ForceSendFields is a list of field names (e.g. "TextSnippet") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "TextSnippet") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeSearchInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeSearchInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeUserInfo: User specific information related to this volume.
// (e.g. page this user last read or whether they purchased this book)
type VolumeUserInfo struct {
	// AcquiredTime: Timestamp when this volume was acquired by the user.
	// (RFC 3339 UTC date-time format) Acquiring includes purchase, user
	// upload, receiving family sharing, etc.
	AcquiredTime string `json:"acquiredTime,omitempty"`

	// AcquisitionType: How this volume was acquired.
	AcquisitionType int64 `json:"acquisitionType,omitempty"`

	// Copy: Copy/Paste accounting information.
	Copy *VolumeUserInfoCopy `json:"copy,omitempty"`

	// EntitlementType: Whether this volume is purchased, sample, pd
	// download etc.
	EntitlementType int64 `json:"entitlementType,omitempty"`

	// FamilySharing: Information on the ability to share with the family.
	FamilySharing *VolumeUserInfoFamilySharing `json:"familySharing,omitempty"`

	// IsFamilySharedFromUser: Whether or not the user shared this volume
	// with the family.
	IsFamilySharedFromUser bool `json:"isFamilySharedFromUser,omitempty"`

	// IsFamilySharedToUser: Whether or not the user received this volume
	// through family sharing.
	IsFamilySharedToUser bool `json:"isFamilySharedToUser,omitempty"`

	// IsFamilySharingAllowed: Deprecated: Replaced by familySharing.
	IsFamilySharingAllowed bool `json:"isFamilySharingAllowed,omitempty"`

	// IsFamilySharingDisabledByFop: Deprecated: Replaced by familySharing.
	IsFamilySharingDisabledByFop bool `json:"isFamilySharingDisabledByFop,omitempty"`

	// IsInMyBooks: Whether or not this volume is currently in "my books."
	IsInMyBooks bool `json:"isInMyBooks,omitempty"`

	// IsPreordered: Whether or not this volume was pre-ordered by the
	// authenticated user making the request. (In LITE projection.)
	IsPreordered bool `json:"isPreordered,omitempty"`

	// IsPurchased: Whether or not this volume was purchased by the
	// authenticated user making the request. (In LITE projection.)
	IsPurchased bool `json:"isPurchased,omitempty"`

	// IsUploaded: Whether or not this volume was user uploaded.
	IsUploaded bool `json:"isUploaded,omitempty"`

	// ReadingPosition: The user's current reading position in the volume,
	// if one is available. (In LITE projection.)
	ReadingPosition *ReadingPosition `json:"readingPosition,omitempty"`

	// RentalPeriod: Period during this book is/was a valid rental.
	RentalPeriod *VolumeUserInfoRentalPeriod `json:"rentalPeriod,omitempty"`

	// RentalState: Whether this book is an active or an expired rental.
	RentalState string `json:"rentalState,omitempty"`

	// Review: This user's review of this volume, if one exists.
	Review *Review `json:"review,omitempty"`

	// Updated: Timestamp when this volume was last modified by a user
	// action, such as a reading position update, volume purchase or writing
	// a review. (RFC 3339 UTC date-time format).
	Updated string `json:"updated,omitempty"`

	UserUploadedVolumeInfo *VolumeUserInfoUserUploadedVolumeInfo `json:"userUploadedVolumeInfo,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AcquiredTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AcquiredTime") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeUserInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeUserInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeUserInfoCopy: Copy/Paste accounting information.
type VolumeUserInfoCopy struct {
	AllowedCharacterCount int64 `json:"allowedCharacterCount,omitempty"`

	LimitType string `json:"limitType,omitempty"`

	RemainingCharacterCount int64 `json:"remainingCharacterCount,omitempty"`

	Updated string `json:"updated,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "AllowedCharacterCount") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllowedCharacterCount") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeUserInfoCopy) MarshalJSON() ([]byte, error) {
	type noMethod VolumeUserInfoCopy
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeUserInfoFamilySharing: Information on the ability to share with
// the family.
type VolumeUserInfoFamilySharing struct {
	// FamilyRole: The role of the user in the family.
	FamilyRole string `json:"familyRole,omitempty"`

	// IsSharingAllowed: Whether or not this volume can be shared with the
	// family by the user. This includes sharing eligibility of both the
	// volume and the user. If the value is true, the user can initiate a
	// family sharing action.
	IsSharingAllowed bool `json:"isSharingAllowed,omitempty"`

	// IsSharingDisabledByFop: Whether or not sharing this volume is
	// temporarily disabled due to issues with the Family Wallet.
	IsSharingDisabledByFop bool `json:"isSharingDisabledByFop,omitempty"`

	// ForceSendFields is a list of field names (e.g. "FamilyRole") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "FamilyRole") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeUserInfoFamilySharing) MarshalJSON() ([]byte, error) {
	type noMethod VolumeUserInfoFamilySharing
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeUserInfoRentalPeriod: Period during this book is/was a valid
// rental.
type VolumeUserInfoRentalPeriod struct {
	EndUtcSec int64 `json:"endUtcSec,omitempty,string"`

	StartUtcSec int64 `json:"startUtcSec,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "EndUtcSec") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "EndUtcSec") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeUserInfoRentalPeriod) MarshalJSON() ([]byte, error) {
	type noMethod VolumeUserInfoRentalPeriod
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type VolumeUserInfoUserUploadedVolumeInfo struct {
	ProcessingState string `json:"processingState,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ProcessingState") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ProcessingState") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeUserInfoUserUploadedVolumeInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeUserInfoUserUploadedVolumeInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeVolumeInfo: General volume information.
type VolumeVolumeInfo struct {
	// AllowAnonLogging: Whether anonymous logging should be allowed.
	AllowAnonLogging bool `json:"allowAnonLogging,omitempty"`

	// Authors: The names of the authors and/or editors for this volume. (In
	// LITE projection)
	Authors []string `json:"authors,omitempty"`

	// AverageRating: The mean review rating for this volume. (min = 1.0,
	// max = 5.0)
	AverageRating float64 `json:"averageRating,omitempty"`

	// CanonicalVolumeLink: Canonical URL for a volume. (In LITE
	// projection.)
	CanonicalVolumeLink string `json:"canonicalVolumeLink,omitempty"`

	// Categories: A list of subject categories, such as "Fiction",
	// "Suspense", etc.
	Categories []string `json:"categories,omitempty"`

	// ContentVersion: An identifier for the version of the volume content
	// (text & images). (In LITE projection)
	ContentVersion string `json:"contentVersion,omitempty"`

	// Description: A synopsis of the volume. The text of the description is
	// formatted in HTML and includes simple formatting elements, such as b,
	// i, and br tags. (In LITE projection.)
	Description string `json:"description,omitempty"`

	// Dimensions: Physical dimensions of this volume.
	Dimensions *VolumeVolumeInfoDimensions `json:"dimensions,omitempty"`

	// ImageLinks: A list of image links for all the sizes that are
	// available. (In LITE projection.)
	ImageLinks *VolumeVolumeInfoImageLinks `json:"imageLinks,omitempty"`

	// IndustryIdentifiers: Industry standard identifiers for this volume.
	IndustryIdentifiers []*VolumeVolumeInfoIndustryIdentifiers `json:"industryIdentifiers,omitempty"`

	// InfoLink: URL to view information about this volume on the Google
	// Books site. (In LITE projection)
	InfoLink string `json:"infoLink,omitempty"`

	// Language: Best language for this volume (based on content). It is the
	// two-letter ISO 639-1 code such as 'fr', 'en', etc.
	Language string `json:"language,omitempty"`

	// MainCategory: The main category to which this volume belongs. It will
	// be the category from the categories list returned below that has the
	// highest weight.
	MainCategory string `json:"mainCategory,omitempty"`

	MaturityRating string `json:"maturityRating,omitempty"`

	// PageCount: Total number of pages as per publisher metadata.
	PageCount int64 `json:"pageCount,omitempty"`

	// PanelizationSummary: A top-level summary of the panelization info in
	// this volume.
	PanelizationSummary *VolumeVolumeInfoPanelizationSummary `json:"panelizationSummary,omitempty"`

	// PreviewLink: URL to preview this volume on the Google Books site.
	PreviewLink string `json:"previewLink,omitempty"`

	// PrintType: Type of publication of this volume. Possible values are
	// BOOK or MAGAZINE.
	PrintType string `json:"printType,omitempty"`

	// PrintedPageCount: Total number of printed pages in generated pdf
	// representation.
	PrintedPageCount int64 `json:"printedPageCount,omitempty"`

	// PublishedDate: Date of publication. (In LITE projection.)
	PublishedDate string `json:"publishedDate,omitempty"`

	// Publisher: Publisher of this volume. (In LITE projection.)
	Publisher string `json:"publisher,omitempty"`

	// RatingsCount: The number of review ratings for this volume.
	RatingsCount int64 `json:"ratingsCount,omitempty"`

	// ReadingModes: The reading modes available for this volume.
	ReadingModes interface{} `json:"readingModes,omitempty"`

	// SamplePageCount: Total number of sample pages as per publisher
	// metadata.
	SamplePageCount int64 `json:"samplePageCount,omitempty"`

	SeriesInfo *Volumeseriesinfo `json:"seriesInfo,omitempty"`

	// Subtitle: Volume subtitle. (In LITE projection.)
	Subtitle string `json:"subtitle,omitempty"`

	// Title: Volume title. (In LITE projection.)
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AllowAnonLogging") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllowAnonLogging") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeVolumeInfo) MarshalJSON() ([]byte, error) {
	type noMethod VolumeVolumeInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeVolumeInfoDimensions: Physical dimensions of this volume.
type VolumeVolumeInfoDimensions struct {
	// Height: Height or length of this volume (in cm).
	Height string `json:"height,omitempty"`

	// Thickness: Thickness of this volume (in cm).
	Thickness string `json:"thickness,omitempty"`

	// Width: Width of this volume (in cm).
	Width string `json:"width,omitempty"`

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

func (s *VolumeVolumeInfoDimensions) MarshalJSON() ([]byte, error) {
	type noMethod VolumeVolumeInfoDimensions
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeVolumeInfoImageLinks: A list of image links for all the sizes
// that are available. (In LITE projection.)
type VolumeVolumeInfoImageLinks struct {
	// ExtraLarge: Image link for extra large size (width of ~1280 pixels).
	// (In LITE projection)
	ExtraLarge string `json:"extraLarge,omitempty"`

	// Large: Image link for large size (width of ~800 pixels). (In LITE
	// projection)
	Large string `json:"large,omitempty"`

	// Medium: Image link for medium size (width of ~575 pixels). (In LITE
	// projection)
	Medium string `json:"medium,omitempty"`

	// Small: Image link for small size (width of ~300 pixels). (In LITE
	// projection)
	Small string `json:"small,omitempty"`

	// SmallThumbnail: Image link for small thumbnail size (width of ~80
	// pixels). (In LITE projection)
	SmallThumbnail string `json:"smallThumbnail,omitempty"`

	// Thumbnail: Image link for thumbnail size (width of ~128 pixels). (In
	// LITE projection)
	Thumbnail string `json:"thumbnail,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ExtraLarge") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ExtraLarge") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeVolumeInfoImageLinks) MarshalJSON() ([]byte, error) {
	type noMethod VolumeVolumeInfoImageLinks
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type VolumeVolumeInfoIndustryIdentifiers struct {
	// Identifier: Industry specific volume identifier.
	Identifier string `json:"identifier,omitempty"`

	// Type: Identifier type. Possible values are ISBN_10, ISBN_13, ISSN and
	// OTHER.
	Type string `json:"type,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Identifier") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Identifier") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeVolumeInfoIndustryIdentifiers) MarshalJSON() ([]byte, error) {
	type noMethod VolumeVolumeInfoIndustryIdentifiers
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeVolumeInfoPanelizationSummary: A top-level summary of the
// panelization info in this volume.
type VolumeVolumeInfoPanelizationSummary struct {
	ContainsEpubBubbles bool `json:"containsEpubBubbles,omitempty"`

	ContainsImageBubbles bool `json:"containsImageBubbles,omitempty"`

	EpubBubbleVersion string `json:"epubBubbleVersion,omitempty"`

	ImageBubbleVersion string `json:"imageBubbleVersion,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ContainsEpubBubbles")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ContainsEpubBubbles") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeVolumeInfoPanelizationSummary) MarshalJSON() ([]byte, error) {
	type noMethod VolumeVolumeInfoPanelizationSummary
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Volume2 struct {
	// Items: A list of volumes.
	Items []*Volume `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

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

func (s *Volume2) MarshalJSON() ([]byte, error) {
	type noMethod Volume2
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Volumeannotation struct {
	// AnnotationDataId: The annotation data id for this volume annotation.
	AnnotationDataId string `json:"annotationDataId,omitempty"`

	// AnnotationDataLink: Link to get data for this annotation.
	AnnotationDataLink string `json:"annotationDataLink,omitempty"`

	// AnnotationType: The type of annotation this is.
	AnnotationType string `json:"annotationType,omitempty"`

	// ContentRanges: The content ranges to identify the selected text.
	ContentRanges *VolumeannotationContentRanges `json:"contentRanges,omitempty"`

	// Data: Data for this annotation.
	Data string `json:"data,omitempty"`

	// Deleted: Indicates that this annotation is deleted.
	Deleted bool `json:"deleted,omitempty"`

	// Id: Unique id of this volume annotation.
	Id string `json:"id,omitempty"`

	// Kind: Resource Type
	Kind string `json:"kind,omitempty"`

	// LayerId: The Layer this annotation is for.
	LayerId string `json:"layerId,omitempty"`

	// PageIds: Pages the annotation spans.
	PageIds []string `json:"pageIds,omitempty"`

	// SelectedText: Excerpt from the volume.
	SelectedText string `json:"selectedText,omitempty"`

	// SelfLink: URL to this resource.
	SelfLink string `json:"selfLink,omitempty"`

	// Updated: Timestamp for the last time this anntoation was updated.
	// (RFC 3339 UTC date-time format).
	Updated string `json:"updated,omitempty"`

	// VolumeId: The Volume this annotation is for.
	VolumeId string `json:"volumeId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AnnotationDataId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AnnotationDataId") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Volumeannotation) MarshalJSON() ([]byte, error) {
	type noMethod Volumeannotation
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VolumeannotationContentRanges: The content ranges to identify the
// selected text.
type VolumeannotationContentRanges struct {
	// CfiRange: Range in CFI format for this annotation for version above.
	CfiRange *BooksAnnotationsRange `json:"cfiRange,omitempty"`

	// ContentVersion: Content version applicable to ranges below.
	ContentVersion string `json:"contentVersion,omitempty"`

	// GbImageRange: Range in GB image format for this annotation for
	// version above.
	GbImageRange *BooksAnnotationsRange `json:"gbImageRange,omitempty"`

	// GbTextRange: Range in GB text format for this annotation for version
	// above.
	GbTextRange *BooksAnnotationsRange `json:"gbTextRange,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CfiRange") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CfiRange") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeannotationContentRanges) MarshalJSON() ([]byte, error) {
	type noMethod VolumeannotationContentRanges
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Volumeannotations struct {
	// Items: A list of volume annotations.
	Items []*Volumeannotation `json:"items,omitempty"`

	// Kind: Resource type
	Kind string `json:"kind,omitempty"`

	// NextPageToken: Token to pass in for pagination for the next page.
	// This will not be present if this request does not have more results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// TotalItems: The total number of volume annotations found.
	TotalItems int64 `json:"totalItems,omitempty"`

	// Version: The version string for all of the volume annotations in this
	// layer (not just the ones in this response). Note: the version string
	// doesn't apply to the annotation data, just the information in this
	// response (e.g. the location of annotations in the book).
	Version string `json:"version,omitempty"`

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

func (s *Volumeannotations) MarshalJSON() ([]byte, error) {
	type noMethod Volumeannotations
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Volumes struct {
	// Items: A list of volumes.
	Items []*Volume `json:"items,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// TotalItems: Total number of volumes found. This might be greater than
	// the number of volumes returned in this response if results have been
	// paginated.
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

func (s *Volumes) MarshalJSON() ([]byte, error) {
	type noMethod Volumes
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type Volumeseriesinfo struct {
	// BookDisplayNumber: The display number string. This should be used
	// only for display purposes and the actual sequence should be inferred
	// from the below orderNumber.
	BookDisplayNumber string `json:"bookDisplayNumber,omitempty"`

	// Kind: Resource type.
	Kind string `json:"kind,omitempty"`

	// ShortSeriesBookTitle: Short book title in the context of the series.
	ShortSeriesBookTitle string `json:"shortSeriesBookTitle,omitempty"`

	VolumeSeries []*VolumeseriesinfoVolumeSeries `json:"volumeSeries,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BookDisplayNumber")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BookDisplayNumber") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Volumeseriesinfo) MarshalJSON() ([]byte, error) {
	type noMethod Volumeseriesinfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type VolumeseriesinfoVolumeSeries struct {
	// Issue: List of issues. Applicable only for Collection Edition and
	// Omnibus.
	Issue []*VolumeseriesinfoVolumeSeriesIssue `json:"issue,omitempty"`

	// OrderNumber: The book order number in the series.
	OrderNumber int64 `json:"orderNumber,omitempty"`

	// SeriesBookType: The book type in the context of series. Examples -
	// Single Issue, Collection Edition, etc.
	SeriesBookType string `json:"seriesBookType,omitempty"`

	// SeriesId: The series id.
	SeriesId string `json:"seriesId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Issue") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Issue") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VolumeseriesinfoVolumeSeries) MarshalJSON() ([]byte, error) {
	type noMethod VolumeseriesinfoVolumeSeries
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type VolumeseriesinfoVolumeSeriesIssue struct {
	IssueDisplayNumber string `json:"issueDisplayNumber,omitempty"`

	IssueOrderNumber int64 `json:"issueOrderNumber,omitempty"`

	// ForceSendFields is a list of field names (e.g. "IssueDisplayNumber")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "IssueDisplayNumber") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *VolumeseriesinfoVolumeSeriesIssue) MarshalJSON() ([]byte, error) {
	type noMethod VolumeseriesinfoVolumeSeriesIssue
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "books.bookshelves.get":

type BookshelvesGetCall struct {
	s            *Service
	userId       string
	shelf        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves metadata for a specific bookshelf for the specified
// user.
func (r *BookshelvesService) Get(userId string, shelf string) *BookshelvesGetCall {
	c := &BookshelvesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.shelf = shelf
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *BookshelvesGetCall) Source(source string) *BookshelvesGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BookshelvesGetCall) Fields(s ...googleapi.Field) *BookshelvesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BookshelvesGetCall) IfNoneMatch(entityTag string) *BookshelvesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BookshelvesGetCall) Context(ctx context.Context) *BookshelvesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BookshelvesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BookshelvesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userId}/bookshelves/{shelf}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"shelf":  c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.bookshelves.get" call.
// Exactly one of *Bookshelf or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Bookshelf.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *BookshelvesGetCall) Do(opts ...googleapi.CallOption) (*Bookshelf, error) {
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
	ret := &Bookshelf{
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
	//   "description": "Retrieves metadata for a specific bookshelf for the specified user.",
	//   "httpMethod": "GET",
	//   "id": "books.bookshelves.get",
	//   "parameterOrder": [
	//     "userId",
	//     "shelf"
	//   ],
	//   "parameters": {
	//     "shelf": {
	//       "description": "ID of bookshelf to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "ID of user for whom to retrieve bookshelves.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userId}/bookshelves/{shelf}",
	//   "response": {
	//     "$ref": "Bookshelf"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.bookshelves.list":

type BookshelvesListCall struct {
	s            *Service
	userId       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of public bookshelves for the specified user.
func (r *BookshelvesService) List(userId string) *BookshelvesListCall {
	c := &BookshelvesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *BookshelvesListCall) Source(source string) *BookshelvesListCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BookshelvesListCall) Fields(s ...googleapi.Field) *BookshelvesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BookshelvesListCall) IfNoneMatch(entityTag string) *BookshelvesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BookshelvesListCall) Context(ctx context.Context) *BookshelvesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BookshelvesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BookshelvesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userId}/bookshelves")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.bookshelves.list" call.
// Exactly one of *Bookshelves or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Bookshelves.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *BookshelvesListCall) Do(opts ...googleapi.CallOption) (*Bookshelves, error) {
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
	ret := &Bookshelves{
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
	//   "description": "Retrieves a list of public bookshelves for the specified user.",
	//   "httpMethod": "GET",
	//   "id": "books.bookshelves.list",
	//   "parameterOrder": [
	//     "userId"
	//   ],
	//   "parameters": {
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "userId": {
	//       "description": "ID of user for whom to retrieve bookshelves.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userId}/bookshelves",
	//   "response": {
	//     "$ref": "Bookshelves"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.bookshelves.volumes.list":

type BookshelvesVolumesListCall struct {
	s            *Service
	userId       string
	shelf        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves volumes in a specific bookshelf for the specified
// user.
func (r *BookshelvesVolumesService) List(userId string, shelf string) *BookshelvesVolumesListCall {
	c := &BookshelvesVolumesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.userId = userId
	c.shelf = shelf
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return
func (c *BookshelvesVolumesListCall) MaxResults(maxResults int64) *BookshelvesVolumesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// ShowPreorders sets the optional parameter "showPreorders": Set to
// true to show pre-ordered books. Defaults to false.
func (c *BookshelvesVolumesListCall) ShowPreorders(showPreorders bool) *BookshelvesVolumesListCall {
	c.urlParams_.Set("showPreorders", fmt.Sprint(showPreorders))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *BookshelvesVolumesListCall) Source(source string) *BookshelvesVolumesListCall {
	c.urlParams_.Set("source", source)
	return c
}

// StartIndex sets the optional parameter "startIndex": Index of the
// first element to return (starts at 0)
func (c *BookshelvesVolumesListCall) StartIndex(startIndex int64) *BookshelvesVolumesListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *BookshelvesVolumesListCall) Fields(s ...googleapi.Field) *BookshelvesVolumesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *BookshelvesVolumesListCall) IfNoneMatch(entityTag string) *BookshelvesVolumesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *BookshelvesVolumesListCall) Context(ctx context.Context) *BookshelvesVolumesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *BookshelvesVolumesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *BookshelvesVolumesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "users/{userId}/bookshelves/{shelf}/volumes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"userId": c.userId,
		"shelf":  c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.bookshelves.volumes.list" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *BookshelvesVolumesListCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Retrieves volumes in a specific bookshelf for the specified user.",
	//   "httpMethod": "GET",
	//   "id": "books.bookshelves.volumes.list",
	//   "parameterOrder": [
	//     "userId",
	//     "shelf"
	//   ],
	//   "parameters": {
	//     "maxResults": {
	//       "description": "Maximum number of results to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "shelf": {
	//       "description": "ID of bookshelf to retrieve volumes.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "showPreorders": {
	//       "description": "Set to true to show pre-ordered books. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startIndex": {
	//       "description": "Index of the first element to return (starts at 0)",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "userId": {
	//       "description": "ID of user for whom to retrieve bookshelf volumes.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "users/{userId}/bookshelves/{shelf}/volumes",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.cloudloading.addBook":

type CloudloadingAddBookCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// AddBook:
func (r *CloudloadingService) AddBook() *CloudloadingAddBookCall {
	c := &CloudloadingAddBookCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// DriveDocumentId sets the optional parameter "drive_document_id": A
// drive document id. The upload_client_token must not be set.
func (c *CloudloadingAddBookCall) DriveDocumentId(driveDocumentId string) *CloudloadingAddBookCall {
	c.urlParams_.Set("drive_document_id", driveDocumentId)
	return c
}

// MimeType sets the optional parameter "mime_type": The document MIME
// type. It can be set only if the drive_document_id is set.
func (c *CloudloadingAddBookCall) MimeType(mimeType string) *CloudloadingAddBookCall {
	c.urlParams_.Set("mime_type", mimeType)
	return c
}

// Name sets the optional parameter "name": The document name. It can be
// set only if the drive_document_id is set.
func (c *CloudloadingAddBookCall) Name(name string) *CloudloadingAddBookCall {
	c.urlParams_.Set("name", name)
	return c
}

// UploadClientToken sets the optional parameter "upload_client_token":
func (c *CloudloadingAddBookCall) UploadClientToken(uploadClientToken string) *CloudloadingAddBookCall {
	c.urlParams_.Set("upload_client_token", uploadClientToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CloudloadingAddBookCall) Fields(s ...googleapi.Field) *CloudloadingAddBookCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CloudloadingAddBookCall) Context(ctx context.Context) *CloudloadingAddBookCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CloudloadingAddBookCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CloudloadingAddBookCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "cloudloading/addBook")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.cloudloading.addBook" call.
// Exactly one of *BooksCloudloadingResource or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *BooksCloudloadingResource.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CloudloadingAddBookCall) Do(opts ...googleapi.CallOption) (*BooksCloudloadingResource, error) {
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
	ret := &BooksCloudloadingResource{
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
	//   "description": "",
	//   "httpMethod": "POST",
	//   "id": "books.cloudloading.addBook",
	//   "parameters": {
	//     "drive_document_id": {
	//       "description": "A drive document id. The upload_client_token must not be set.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "mime_type": {
	//       "description": "The document MIME type. It can be set only if the drive_document_id is set.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "name": {
	//       "description": "The document name. It can be set only if the drive_document_id is set.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "upload_client_token": {
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "cloudloading/addBook",
	//   "response": {
	//     "$ref": "BooksCloudloadingResource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.cloudloading.deleteBook":

type CloudloadingDeleteBookCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// DeleteBook: Remove the book and its contents
func (r *CloudloadingService) DeleteBook(volumeId string) *CloudloadingDeleteBookCall {
	c := &CloudloadingDeleteBookCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("volumeId", volumeId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CloudloadingDeleteBookCall) Fields(s ...googleapi.Field) *CloudloadingDeleteBookCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CloudloadingDeleteBookCall) Context(ctx context.Context) *CloudloadingDeleteBookCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CloudloadingDeleteBookCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CloudloadingDeleteBookCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "cloudloading/deleteBook")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.cloudloading.deleteBook" call.
func (c *CloudloadingDeleteBookCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Remove the book and its contents",
	//   "httpMethod": "POST",
	//   "id": "books.cloudloading.deleteBook",
	//   "parameterOrder": [
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "volumeId": {
	//       "description": "The id of the book to be removed.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "cloudloading/deleteBook",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.cloudloading.updateBook":

type CloudloadingUpdateBookCall struct {
	s                         *Service
	bookscloudloadingresource *BooksCloudloadingResource
	urlParams_                gensupport.URLParams
	ctx_                      context.Context
	header_                   http.Header
}

// UpdateBook:
func (r *CloudloadingService) UpdateBook(bookscloudloadingresource *BooksCloudloadingResource) *CloudloadingUpdateBookCall {
	c := &CloudloadingUpdateBookCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.bookscloudloadingresource = bookscloudloadingresource
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *CloudloadingUpdateBookCall) Fields(s ...googleapi.Field) *CloudloadingUpdateBookCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *CloudloadingUpdateBookCall) Context(ctx context.Context) *CloudloadingUpdateBookCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *CloudloadingUpdateBookCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *CloudloadingUpdateBookCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.bookscloudloadingresource)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "cloudloading/updateBook")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.cloudloading.updateBook" call.
// Exactly one of *BooksCloudloadingResource or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *BooksCloudloadingResource.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *CloudloadingUpdateBookCall) Do(opts ...googleapi.CallOption) (*BooksCloudloadingResource, error) {
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
	ret := &BooksCloudloadingResource{
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
	//   "description": "",
	//   "httpMethod": "POST",
	//   "id": "books.cloudloading.updateBook",
	//   "path": "cloudloading/updateBook",
	//   "request": {
	//     "$ref": "BooksCloudloadingResource"
	//   },
	//   "response": {
	//     "$ref": "BooksCloudloadingResource"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.dictionary.listOfflineMetadata":

type DictionaryListOfflineMetadataCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// ListOfflineMetadata: Returns a list of offline dictionary metadata
// available
func (r *DictionaryService) ListOfflineMetadata(cpksver string) *DictionaryListOfflineMetadataCall {
	c := &DictionaryListOfflineMetadataCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("cpksver", cpksver)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *DictionaryListOfflineMetadataCall) Fields(s ...googleapi.Field) *DictionaryListOfflineMetadataCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *DictionaryListOfflineMetadataCall) IfNoneMatch(entityTag string) *DictionaryListOfflineMetadataCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *DictionaryListOfflineMetadataCall) Context(ctx context.Context) *DictionaryListOfflineMetadataCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *DictionaryListOfflineMetadataCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *DictionaryListOfflineMetadataCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "dictionary/listOfflineMetadata")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.dictionary.listOfflineMetadata" call.
// Exactly one of *Metadata or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Metadata.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *DictionaryListOfflineMetadataCall) Do(opts ...googleapi.CallOption) (*Metadata, error) {
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
	ret := &Metadata{
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
	//   "description": "Returns a list of offline dictionary metadata available",
	//   "httpMethod": "GET",
	//   "id": "books.dictionary.listOfflineMetadata",
	//   "parameterOrder": [
	//     "cpksver"
	//   ],
	//   "parameters": {
	//     "cpksver": {
	//       "description": "The device/version ID from which to request the data.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "dictionary/listOfflineMetadata",
	//   "response": {
	//     "$ref": "Metadata"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.layers.get":

type LayersGetCall struct {
	s            *Service
	volumeId     string
	summaryId    string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the layer summary for a volume.
func (r *LayersService) Get(volumeId string, summaryId string) *LayersGetCall {
	c := &LayersGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	c.summaryId = summaryId
	return c
}

// ContentVersion sets the optional parameter "contentVersion": The
// content version for the requested volume.
func (c *LayersGetCall) ContentVersion(contentVersion string) *LayersGetCall {
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *LayersGetCall) Source(source string) *LayersGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LayersGetCall) Fields(s ...googleapi.Field) *LayersGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LayersGetCall) IfNoneMatch(entityTag string) *LayersGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LayersGetCall) Context(ctx context.Context) *LayersGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LayersGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LayersGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}/layersummary/{summaryId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId":  c.volumeId,
		"summaryId": c.summaryId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.layers.get" call.
// Exactly one of *Layersummary or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Layersummary.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *LayersGetCall) Do(opts ...googleapi.CallOption) (*Layersummary, error) {
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
	ret := &Layersummary{
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
	//   "description": "Gets the layer summary for a volume.",
	//   "httpMethod": "GET",
	//   "id": "books.layers.get",
	//   "parameterOrder": [
	//     "volumeId",
	//     "summaryId"
	//   ],
	//   "parameters": {
	//     "contentVersion": {
	//       "description": "The content version for the requested volume.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "summaryId": {
	//       "description": "The ID for the layer to get the summary for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to retrieve layers for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/{volumeId}/layersummary/{summaryId}",
	//   "response": {
	//     "$ref": "Layersummary"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.layers.list":

type LayersListCall struct {
	s            *Service
	volumeId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List the layer summaries for a volume.
func (r *LayersService) List(volumeId string) *LayersListCall {
	c := &LayersListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	return c
}

// ContentVersion sets the optional parameter "contentVersion": The
// content version for the requested volume.
func (c *LayersListCall) ContentVersion(contentVersion string) *LayersListCall {
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return
func (c *LayersListCall) MaxResults(maxResults int64) *LayersListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": The value of the
// nextToken from the previous page.
func (c *LayersListCall) PageToken(pageToken string) *LayersListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *LayersListCall) Source(source string) *LayersListCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LayersListCall) Fields(s ...googleapi.Field) *LayersListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LayersListCall) IfNoneMatch(entityTag string) *LayersListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LayersListCall) Context(ctx context.Context) *LayersListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LayersListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LayersListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}/layersummary")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId": c.volumeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.layers.list" call.
// Exactly one of *Layersummaries or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Layersummaries.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *LayersListCall) Do(opts ...googleapi.CallOption) (*Layersummaries, error) {
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
	ret := &Layersummaries{
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
	//   "description": "List the layer summaries for a volume.",
	//   "httpMethod": "GET",
	//   "id": "books.layers.list",
	//   "parameterOrder": [
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "contentVersion": {
	//       "description": "The content version for the requested volume.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "200",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The value of the nextToken from the previous page.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to retrieve layers for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/{volumeId}/layersummary",
	//   "response": {
	//     "$ref": "Layersummaries"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.layers.annotationData.get":

type LayersAnnotationDataGetCall struct {
	s                *Service
	volumeId         string
	layerId          string
	annotationDataId string
	urlParams_       gensupport.URLParams
	ifNoneMatch_     string
	ctx_             context.Context
	header_          http.Header
}

// Get: Gets the annotation data.
func (r *LayersAnnotationDataService) Get(volumeId string, layerId string, annotationDataId string, contentVersion string) *LayersAnnotationDataGetCall {
	c := &LayersAnnotationDataGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	c.layerId = layerId
	c.annotationDataId = annotationDataId
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// AllowWebDefinitions sets the optional parameter
// "allowWebDefinitions": For the dictionary layer. Whether or not to
// allow web definitions.
func (c *LayersAnnotationDataGetCall) AllowWebDefinitions(allowWebDefinitions bool) *LayersAnnotationDataGetCall {
	c.urlParams_.Set("allowWebDefinitions", fmt.Sprint(allowWebDefinitions))
	return c
}

// H sets the optional parameter "h": The requested pixel height for any
// images. If height is provided width must also be provided.
func (c *LayersAnnotationDataGetCall) H(h int64) *LayersAnnotationDataGetCall {
	c.urlParams_.Set("h", fmt.Sprint(h))
	return c
}

// Locale sets the optional parameter "locale": The locale information
// for the data. ISO-639-1 language and ISO-3166-1 country code. Ex:
// 'en_US'.
func (c *LayersAnnotationDataGetCall) Locale(locale string) *LayersAnnotationDataGetCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Scale sets the optional parameter "scale": The requested scale for
// the image.
func (c *LayersAnnotationDataGetCall) Scale(scale int64) *LayersAnnotationDataGetCall {
	c.urlParams_.Set("scale", fmt.Sprint(scale))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *LayersAnnotationDataGetCall) Source(source string) *LayersAnnotationDataGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// W sets the optional parameter "w": The requested pixel width for any
// images. If width is provided height must also be provided.
func (c *LayersAnnotationDataGetCall) W(w int64) *LayersAnnotationDataGetCall {
	c.urlParams_.Set("w", fmt.Sprint(w))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LayersAnnotationDataGetCall) Fields(s ...googleapi.Field) *LayersAnnotationDataGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LayersAnnotationDataGetCall) IfNoneMatch(entityTag string) *LayersAnnotationDataGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LayersAnnotationDataGetCall) Context(ctx context.Context) *LayersAnnotationDataGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LayersAnnotationDataGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LayersAnnotationDataGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}/layers/{layerId}/data/{annotationDataId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId":         c.volumeId,
		"layerId":          c.layerId,
		"annotationDataId": c.annotationDataId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.layers.annotationData.get" call.
// Exactly one of *Annotationdata or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Annotationdata.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *LayersAnnotationDataGetCall) Do(opts ...googleapi.CallOption) (*Annotationdata, error) {
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
	ret := &Annotationdata{
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
	//   "description": "Gets the annotation data.",
	//   "httpMethod": "GET",
	//   "id": "books.layers.annotationData.get",
	//   "parameterOrder": [
	//     "volumeId",
	//     "layerId",
	//     "annotationDataId",
	//     "contentVersion"
	//   ],
	//   "parameters": {
	//     "allowWebDefinitions": {
	//       "description": "For the dictionary layer. Whether or not to allow web definitions.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "annotationDataId": {
	//       "description": "The ID of the annotation data to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "contentVersion": {
	//       "description": "The content version for the volume you are trying to retrieve.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "h": {
	//       "description": "The requested pixel height for any images. If height is provided width must also be provided.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "layerId": {
	//       "description": "The ID for the layer to get the annotations.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "The locale information for the data. ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "scale": {
	//       "description": "The requested scale for the image.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to retrieve annotations for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "w": {
	//       "description": "The requested pixel width for any images. If width is provided height must also be provided.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     }
	//   },
	//   "path": "volumes/{volumeId}/layers/{layerId}/data/{annotationDataId}",
	//   "response": {
	//     "$ref": "Annotationdata"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.layers.annotationData.list":

type LayersAnnotationDataListCall struct {
	s            *Service
	volumeId     string
	layerId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Gets the annotation data for a volume and layer.
func (r *LayersAnnotationDataService) List(volumeId string, layerId string, contentVersion string) *LayersAnnotationDataListCall {
	c := &LayersAnnotationDataListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	c.layerId = layerId
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// AnnotationDataId sets the optional parameter "annotationDataId": The
// list of Annotation Data Ids to retrieve. Pagination is ignored if
// this is set.
func (c *LayersAnnotationDataListCall) AnnotationDataId(annotationDataId ...string) *LayersAnnotationDataListCall {
	c.urlParams_.SetMulti("annotationDataId", append([]string{}, annotationDataId...))
	return c
}

// H sets the optional parameter "h": The requested pixel height for any
// images. If height is provided width must also be provided.
func (c *LayersAnnotationDataListCall) H(h int64) *LayersAnnotationDataListCall {
	c.urlParams_.Set("h", fmt.Sprint(h))
	return c
}

// Locale sets the optional parameter "locale": The locale information
// for the data. ISO-639-1 language and ISO-3166-1 country code. Ex:
// 'en_US'.
func (c *LayersAnnotationDataListCall) Locale(locale string) *LayersAnnotationDataListCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return
func (c *LayersAnnotationDataListCall) MaxResults(maxResults int64) *LayersAnnotationDataListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": The value of the
// nextToken from the previous page.
func (c *LayersAnnotationDataListCall) PageToken(pageToken string) *LayersAnnotationDataListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Scale sets the optional parameter "scale": The requested scale for
// the image.
func (c *LayersAnnotationDataListCall) Scale(scale int64) *LayersAnnotationDataListCall {
	c.urlParams_.Set("scale", fmt.Sprint(scale))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *LayersAnnotationDataListCall) Source(source string) *LayersAnnotationDataListCall {
	c.urlParams_.Set("source", source)
	return c
}

// UpdatedMax sets the optional parameter "updatedMax": RFC 3339
// timestamp to restrict to items updated prior to this timestamp
// (exclusive).
func (c *LayersAnnotationDataListCall) UpdatedMax(updatedMax string) *LayersAnnotationDataListCall {
	c.urlParams_.Set("updatedMax", updatedMax)
	return c
}

// UpdatedMin sets the optional parameter "updatedMin": RFC 3339
// timestamp to restrict to items updated since this timestamp
// (inclusive).
func (c *LayersAnnotationDataListCall) UpdatedMin(updatedMin string) *LayersAnnotationDataListCall {
	c.urlParams_.Set("updatedMin", updatedMin)
	return c
}

// W sets the optional parameter "w": The requested pixel width for any
// images. If width is provided height must also be provided.
func (c *LayersAnnotationDataListCall) W(w int64) *LayersAnnotationDataListCall {
	c.urlParams_.Set("w", fmt.Sprint(w))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LayersAnnotationDataListCall) Fields(s ...googleapi.Field) *LayersAnnotationDataListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LayersAnnotationDataListCall) IfNoneMatch(entityTag string) *LayersAnnotationDataListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LayersAnnotationDataListCall) Context(ctx context.Context) *LayersAnnotationDataListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LayersAnnotationDataListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LayersAnnotationDataListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}/layers/{layerId}/data")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId": c.volumeId,
		"layerId":  c.layerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.layers.annotationData.list" call.
// Exactly one of *Annotationsdata or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Annotationsdata.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *LayersAnnotationDataListCall) Do(opts ...googleapi.CallOption) (*Annotationsdata, error) {
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
	ret := &Annotationsdata{
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
	//   "description": "Gets the annotation data for a volume and layer.",
	//   "httpMethod": "GET",
	//   "id": "books.layers.annotationData.list",
	//   "parameterOrder": [
	//     "volumeId",
	//     "layerId",
	//     "contentVersion"
	//   ],
	//   "parameters": {
	//     "annotationDataId": {
	//       "description": "The list of Annotation Data Ids to retrieve. Pagination is ignored if this is set.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "contentVersion": {
	//       "description": "The content version for the requested volume.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "h": {
	//       "description": "The requested pixel height for any images. If height is provided width must also be provided.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "layerId": {
	//       "description": "The ID for the layer to get the annotation data.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "The locale information for the data. ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "200",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The value of the nextToken from the previous page.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "scale": {
	//       "description": "The requested scale for the image.",
	//       "format": "int32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMax": {
	//       "description": "RFC 3339 timestamp to restrict to items updated prior to this timestamp (exclusive).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMin": {
	//       "description": "RFC 3339 timestamp to restrict to items updated since this timestamp (inclusive).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to retrieve annotation data for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "w": {
	//       "description": "The requested pixel width for any images. If width is provided height must also be provided.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     }
	//   },
	//   "path": "volumes/{volumeId}/layers/{layerId}/data",
	//   "response": {
	//     "$ref": "Annotationsdata"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *LayersAnnotationDataListCall) Pages(ctx context.Context, f func(*Annotationsdata) error) error {
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

// method id "books.layers.volumeAnnotations.get":

type LayersVolumeAnnotationsGetCall struct {
	s            *Service
	volumeId     string
	layerId      string
	annotationId string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the volume annotation.
func (r *LayersVolumeAnnotationsService) Get(volumeId string, layerId string, annotationId string) *LayersVolumeAnnotationsGetCall {
	c := &LayersVolumeAnnotationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	c.layerId = layerId
	c.annotationId = annotationId
	return c
}

// Locale sets the optional parameter "locale": The locale information
// for the data. ISO-639-1 language and ISO-3166-1 country code. Ex:
// 'en_US'.
func (c *LayersVolumeAnnotationsGetCall) Locale(locale string) *LayersVolumeAnnotationsGetCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *LayersVolumeAnnotationsGetCall) Source(source string) *LayersVolumeAnnotationsGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LayersVolumeAnnotationsGetCall) Fields(s ...googleapi.Field) *LayersVolumeAnnotationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LayersVolumeAnnotationsGetCall) IfNoneMatch(entityTag string) *LayersVolumeAnnotationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LayersVolumeAnnotationsGetCall) Context(ctx context.Context) *LayersVolumeAnnotationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LayersVolumeAnnotationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LayersVolumeAnnotationsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}/layers/{layerId}/annotations/{annotationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId":     c.volumeId,
		"layerId":      c.layerId,
		"annotationId": c.annotationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.layers.volumeAnnotations.get" call.
// Exactly one of *Volumeannotation or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *Volumeannotation.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *LayersVolumeAnnotationsGetCall) Do(opts ...googleapi.CallOption) (*Volumeannotation, error) {
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
	ret := &Volumeannotation{
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
	//   "description": "Gets the volume annotation.",
	//   "httpMethod": "GET",
	//   "id": "books.layers.volumeAnnotations.get",
	//   "parameterOrder": [
	//     "volumeId",
	//     "layerId",
	//     "annotationId"
	//   ],
	//   "parameters": {
	//     "annotationId": {
	//       "description": "The ID of the volume annotation to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "layerId": {
	//       "description": "The ID for the layer to get the annotations.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "The locale information for the data. ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to retrieve annotations for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/{volumeId}/layers/{layerId}/annotations/{annotationId}",
	//   "response": {
	//     "$ref": "Volumeannotation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.layers.volumeAnnotations.list":

type LayersVolumeAnnotationsListCall struct {
	s            *Service
	volumeId     string
	layerId      string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Gets the volume annotations for a volume and layer.
func (r *LayersVolumeAnnotationsService) List(volumeId string, layerId string, contentVersion string) *LayersVolumeAnnotationsListCall {
	c := &LayersVolumeAnnotationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	c.layerId = layerId
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// EndOffset sets the optional parameter "endOffset": The end offset to
// end retrieving data from.
func (c *LayersVolumeAnnotationsListCall) EndOffset(endOffset string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("endOffset", endOffset)
	return c
}

// EndPosition sets the optional parameter "endPosition": The end
// position to end retrieving data from.
func (c *LayersVolumeAnnotationsListCall) EndPosition(endPosition string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("endPosition", endPosition)
	return c
}

// Locale sets the optional parameter "locale": The locale information
// for the data. ISO-639-1 language and ISO-3166-1 country code. Ex:
// 'en_US'.
func (c *LayersVolumeAnnotationsListCall) Locale(locale string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return
func (c *LayersVolumeAnnotationsListCall) MaxResults(maxResults int64) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": The value of the
// nextToken from the previous page.
func (c *LayersVolumeAnnotationsListCall) PageToken(pageToken string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Set to true to
// return deleted annotations. updatedMin must be in the request to use
// this. Defaults to false.
func (c *LayersVolumeAnnotationsListCall) ShowDeleted(showDeleted bool) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *LayersVolumeAnnotationsListCall) Source(source string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("source", source)
	return c
}

// StartOffset sets the optional parameter "startOffset": The start
// offset to start retrieving data from.
func (c *LayersVolumeAnnotationsListCall) StartOffset(startOffset string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("startOffset", startOffset)
	return c
}

// StartPosition sets the optional parameter "startPosition": The start
// position to start retrieving data from.
func (c *LayersVolumeAnnotationsListCall) StartPosition(startPosition string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("startPosition", startPosition)
	return c
}

// UpdatedMax sets the optional parameter "updatedMax": RFC 3339
// timestamp to restrict to items updated prior to this timestamp
// (exclusive).
func (c *LayersVolumeAnnotationsListCall) UpdatedMax(updatedMax string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("updatedMax", updatedMax)
	return c
}

// UpdatedMin sets the optional parameter "updatedMin": RFC 3339
// timestamp to restrict to items updated since this timestamp
// (inclusive).
func (c *LayersVolumeAnnotationsListCall) UpdatedMin(updatedMin string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("updatedMin", updatedMin)
	return c
}

// VolumeAnnotationsVersion sets the optional parameter
// "volumeAnnotationsVersion": The version of the volume annotations
// that you are requesting.
func (c *LayersVolumeAnnotationsListCall) VolumeAnnotationsVersion(volumeAnnotationsVersion string) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("volumeAnnotationsVersion", volumeAnnotationsVersion)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *LayersVolumeAnnotationsListCall) Fields(s ...googleapi.Field) *LayersVolumeAnnotationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *LayersVolumeAnnotationsListCall) IfNoneMatch(entityTag string) *LayersVolumeAnnotationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *LayersVolumeAnnotationsListCall) Context(ctx context.Context) *LayersVolumeAnnotationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *LayersVolumeAnnotationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *LayersVolumeAnnotationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}/layers/{layerId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId": c.volumeId,
		"layerId":  c.layerId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.layers.volumeAnnotations.list" call.
// Exactly one of *Volumeannotations or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *Volumeannotations.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *LayersVolumeAnnotationsListCall) Do(opts ...googleapi.CallOption) (*Volumeannotations, error) {
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
	ret := &Volumeannotations{
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
	//   "description": "Gets the volume annotations for a volume and layer.",
	//   "httpMethod": "GET",
	//   "id": "books.layers.volumeAnnotations.list",
	//   "parameterOrder": [
	//     "volumeId",
	//     "layerId",
	//     "contentVersion"
	//   ],
	//   "parameters": {
	//     "contentVersion": {
	//       "description": "The content version for the requested volume.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "endOffset": {
	//       "description": "The end offset to end retrieving data from.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "endPosition": {
	//       "description": "The end position to end retrieving data from.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "layerId": {
	//       "description": "The ID for the layer to get the annotations.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "The locale information for the data. ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "200",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The value of the nextToken from the previous page.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Set to true to return deleted annotations. updatedMin must be in the request to use this. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startOffset": {
	//       "description": "The start offset to start retrieving data from.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startPosition": {
	//       "description": "The start position to start retrieving data from.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMax": {
	//       "description": "RFC 3339 timestamp to restrict to items updated prior to this timestamp (exclusive).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMin": {
	//       "description": "RFC 3339 timestamp to restrict to items updated since this timestamp (inclusive).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeAnnotationsVersion": {
	//       "description": "The version of the volume annotations that you are requesting.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to retrieve annotations for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/{volumeId}/layers/{layerId}",
	//   "response": {
	//     "$ref": "Volumeannotations"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *LayersVolumeAnnotationsListCall) Pages(ctx context.Context, f func(*Volumeannotations) error) error {
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

// method id "books.myconfig.getUserSettings":

type MyconfigGetUserSettingsCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetUserSettings: Gets the current settings for the user.
func (r *MyconfigService) GetUserSettings() *MyconfigGetUserSettingsCall {
	c := &MyconfigGetUserSettingsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MyconfigGetUserSettingsCall) Fields(s ...googleapi.Field) *MyconfigGetUserSettingsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MyconfigGetUserSettingsCall) IfNoneMatch(entityTag string) *MyconfigGetUserSettingsCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MyconfigGetUserSettingsCall) Context(ctx context.Context) *MyconfigGetUserSettingsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MyconfigGetUserSettingsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MyconfigGetUserSettingsCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "myconfig/getUserSettings")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.myconfig.getUserSettings" call.
// Exactly one of *Usersettings or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Usersettings.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MyconfigGetUserSettingsCall) Do(opts ...googleapi.CallOption) (*Usersettings, error) {
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
	ret := &Usersettings{
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
	//   "description": "Gets the current settings for the user.",
	//   "httpMethod": "GET",
	//   "id": "books.myconfig.getUserSettings",
	//   "path": "myconfig/getUserSettings",
	//   "response": {
	//     "$ref": "Usersettings"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.myconfig.releaseDownloadAccess":

type MyconfigReleaseDownloadAccessCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// ReleaseDownloadAccess: Release downloaded content access restriction.
func (r *MyconfigService) ReleaseDownloadAccess(volumeIds []string, cpksver string) *MyconfigReleaseDownloadAccessCall {
	c := &MyconfigReleaseDownloadAccessCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.SetMulti("volumeIds", append([]string{}, volumeIds...))
	c.urlParams_.Set("cpksver", cpksver)
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1, ISO-3166-1
// codes for message localization, i.e. en_US.
func (c *MyconfigReleaseDownloadAccessCall) Locale(locale string) *MyconfigReleaseDownloadAccessCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MyconfigReleaseDownloadAccessCall) Source(source string) *MyconfigReleaseDownloadAccessCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MyconfigReleaseDownloadAccessCall) Fields(s ...googleapi.Field) *MyconfigReleaseDownloadAccessCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MyconfigReleaseDownloadAccessCall) Context(ctx context.Context) *MyconfigReleaseDownloadAccessCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MyconfigReleaseDownloadAccessCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MyconfigReleaseDownloadAccessCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "myconfig/releaseDownloadAccess")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.myconfig.releaseDownloadAccess" call.
// Exactly one of *DownloadAccesses or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *DownloadAccesses.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MyconfigReleaseDownloadAccessCall) Do(opts ...googleapi.CallOption) (*DownloadAccesses, error) {
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
	ret := &DownloadAccesses{
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
	//   "description": "Release downloaded content access restriction.",
	//   "httpMethod": "POST",
	//   "id": "books.myconfig.releaseDownloadAccess",
	//   "parameterOrder": [
	//     "volumeIds",
	//     "cpksver"
	//   ],
	//   "parameters": {
	//     "cpksver": {
	//       "description": "The device/version ID from which to release the restriction.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "ISO-639-1, ISO-3166-1 codes for message localization, i.e. en_US.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeIds": {
	//       "description": "The volume(s) to release restrictions for.",
	//       "location": "query",
	//       "repeated": true,
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "myconfig/releaseDownloadAccess",
	//   "response": {
	//     "$ref": "DownloadAccesses"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.myconfig.requestAccess":

type MyconfigRequestAccessCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// RequestAccess: Request concurrent and download access restrictions.
func (r *MyconfigService) RequestAccess(source string, volumeId string, nonce string, cpksver string) *MyconfigRequestAccessCall {
	c := &MyconfigRequestAccessCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("source", source)
	c.urlParams_.Set("volumeId", volumeId)
	c.urlParams_.Set("nonce", nonce)
	c.urlParams_.Set("cpksver", cpksver)
	return c
}

// LicenseTypes sets the optional parameter "licenseTypes": The type of
// access license to request. If not specified, the default is BOTH.
//
// Possible values:
//   "BOTH" - Both concurrent and download licenses.
//   "CONCURRENT" - Concurrent access license.
//   "DOWNLOAD" - Offline download access license.
func (c *MyconfigRequestAccessCall) LicenseTypes(licenseTypes string) *MyconfigRequestAccessCall {
	c.urlParams_.Set("licenseTypes", licenseTypes)
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1, ISO-3166-1
// codes for message localization, i.e. en_US.
func (c *MyconfigRequestAccessCall) Locale(locale string) *MyconfigRequestAccessCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MyconfigRequestAccessCall) Fields(s ...googleapi.Field) *MyconfigRequestAccessCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MyconfigRequestAccessCall) Context(ctx context.Context) *MyconfigRequestAccessCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MyconfigRequestAccessCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MyconfigRequestAccessCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "myconfig/requestAccess")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.myconfig.requestAccess" call.
// Exactly one of *RequestAccess or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *RequestAccess.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MyconfigRequestAccessCall) Do(opts ...googleapi.CallOption) (*RequestAccess, error) {
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
	ret := &RequestAccess{
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
	//   "description": "Request concurrent and download access restrictions.",
	//   "httpMethod": "POST",
	//   "id": "books.myconfig.requestAccess",
	//   "parameterOrder": [
	//     "source",
	//     "volumeId",
	//     "nonce",
	//     "cpksver"
	//   ],
	//   "parameters": {
	//     "cpksver": {
	//       "description": "The device/version ID from which to request the restrictions.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "licenseTypes": {
	//       "description": "The type of access license to request. If not specified, the default is BOTH.",
	//       "enum": [
	//         "BOTH",
	//         "CONCURRENT",
	//         "DOWNLOAD"
	//       ],
	//       "enumDescriptions": [
	//         "Both concurrent and download licenses.",
	//         "Concurrent access license.",
	//         "Offline download access license."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "ISO-639-1, ISO-3166-1 codes for message localization, i.e. en_US.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "nonce": {
	//       "description": "The client nonce value.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to request concurrent/download restrictions for.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "myconfig/requestAccess",
	//   "response": {
	//     "$ref": "RequestAccess"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.myconfig.syncVolumeLicenses":

type MyconfigSyncVolumeLicensesCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// SyncVolumeLicenses: Request downloaded content access for specified
// volumes on the My eBooks shelf.
func (r *MyconfigService) SyncVolumeLicenses(source string, nonce string, cpksver string) *MyconfigSyncVolumeLicensesCall {
	c := &MyconfigSyncVolumeLicensesCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("source", source)
	c.urlParams_.Set("nonce", nonce)
	c.urlParams_.Set("cpksver", cpksver)
	return c
}

// Features sets the optional parameter "features": List of features
// supported by the client, i.e., 'RENTALS'
//
// Possible values:
//   "RENTALS" - Client supports rentals.
func (c *MyconfigSyncVolumeLicensesCall) Features(features ...string) *MyconfigSyncVolumeLicensesCall {
	c.urlParams_.SetMulti("features", append([]string{}, features...))
	return c
}

// IncludeNonComicsSeries sets the optional parameter
// "includeNonComicsSeries": Set to true to include non-comics series.
// Defaults to false.
func (c *MyconfigSyncVolumeLicensesCall) IncludeNonComicsSeries(includeNonComicsSeries bool) *MyconfigSyncVolumeLicensesCall {
	c.urlParams_.Set("includeNonComicsSeries", fmt.Sprint(includeNonComicsSeries))
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1, ISO-3166-1
// codes for message localization, i.e. en_US.
func (c *MyconfigSyncVolumeLicensesCall) Locale(locale string) *MyconfigSyncVolumeLicensesCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// ShowPreorders sets the optional parameter "showPreorders": Set to
// true to show pre-ordered books. Defaults to false.
func (c *MyconfigSyncVolumeLicensesCall) ShowPreorders(showPreorders bool) *MyconfigSyncVolumeLicensesCall {
	c.urlParams_.Set("showPreorders", fmt.Sprint(showPreorders))
	return c
}

// VolumeIds sets the optional parameter "volumeIds": The volume(s) to
// request download restrictions for.
func (c *MyconfigSyncVolumeLicensesCall) VolumeIds(volumeIds ...string) *MyconfigSyncVolumeLicensesCall {
	c.urlParams_.SetMulti("volumeIds", append([]string{}, volumeIds...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MyconfigSyncVolumeLicensesCall) Fields(s ...googleapi.Field) *MyconfigSyncVolumeLicensesCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MyconfigSyncVolumeLicensesCall) Context(ctx context.Context) *MyconfigSyncVolumeLicensesCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MyconfigSyncVolumeLicensesCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MyconfigSyncVolumeLicensesCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "myconfig/syncVolumeLicenses")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.myconfig.syncVolumeLicenses" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *MyconfigSyncVolumeLicensesCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Request downloaded content access for specified volumes on the My eBooks shelf.",
	//   "httpMethod": "POST",
	//   "id": "books.myconfig.syncVolumeLicenses",
	//   "parameterOrder": [
	//     "source",
	//     "nonce",
	//     "cpksver"
	//   ],
	//   "parameters": {
	//     "cpksver": {
	//       "description": "The device/version ID from which to release the restriction.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "features": {
	//       "description": "List of features supported by the client, i.e., 'RENTALS'",
	//       "enum": [
	//         "RENTALS"
	//       ],
	//       "enumDescriptions": [
	//         "Client supports rentals."
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "includeNonComicsSeries": {
	//       "description": "Set to true to include non-comics series. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "locale": {
	//       "description": "ISO-639-1, ISO-3166-1 codes for message localization, i.e. en_US.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "nonce": {
	//       "description": "The client nonce value.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "showPreorders": {
	//       "description": "Set to true to show pre-ordered books. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "volumeIds": {
	//       "description": "The volume(s) to request download restrictions for.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "myconfig/syncVolumeLicenses",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.myconfig.updateUserSettings":

type MyconfigUpdateUserSettingsCall struct {
	s            *Service
	usersettings *Usersettings
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// UpdateUserSettings: Sets the settings for the user. If a sub-object
// is specified, it will overwrite the existing sub-object stored in the
// server. Unspecified sub-objects will retain the existing value.
func (r *MyconfigService) UpdateUserSettings(usersettings *Usersettings) *MyconfigUpdateUserSettingsCall {
	c := &MyconfigUpdateUserSettingsCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.usersettings = usersettings
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MyconfigUpdateUserSettingsCall) Fields(s ...googleapi.Field) *MyconfigUpdateUserSettingsCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MyconfigUpdateUserSettingsCall) Context(ctx context.Context) *MyconfigUpdateUserSettingsCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MyconfigUpdateUserSettingsCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MyconfigUpdateUserSettingsCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.usersettings)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "myconfig/updateUserSettings")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.myconfig.updateUserSettings" call.
// Exactly one of *Usersettings or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Usersettings.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MyconfigUpdateUserSettingsCall) Do(opts ...googleapi.CallOption) (*Usersettings, error) {
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
	ret := &Usersettings{
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
	//   "description": "Sets the settings for the user. If a sub-object is specified, it will overwrite the existing sub-object stored in the server. Unspecified sub-objects will retain the existing value.",
	//   "httpMethod": "POST",
	//   "id": "books.myconfig.updateUserSettings",
	//   "path": "myconfig/updateUserSettings",
	//   "request": {
	//     "$ref": "Usersettings"
	//   },
	//   "response": {
	//     "$ref": "Usersettings"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.annotations.delete":

type MylibraryAnnotationsDeleteCall struct {
	s            *Service
	annotationId string
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Delete: Deletes an annotation.
func (r *MylibraryAnnotationsService) Delete(annotationId string) *MylibraryAnnotationsDeleteCall {
	c := &MylibraryAnnotationsDeleteCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.annotationId = annotationId
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryAnnotationsDeleteCall) Source(source string) *MylibraryAnnotationsDeleteCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryAnnotationsDeleteCall) Fields(s ...googleapi.Field) *MylibraryAnnotationsDeleteCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryAnnotationsDeleteCall) Context(ctx context.Context) *MylibraryAnnotationsDeleteCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryAnnotationsDeleteCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryAnnotationsDeleteCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/annotations/{annotationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("DELETE", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"annotationId": c.annotationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.annotations.delete" call.
func (c *MylibraryAnnotationsDeleteCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Deletes an annotation.",
	//   "httpMethod": "DELETE",
	//   "id": "books.mylibrary.annotations.delete",
	//   "parameterOrder": [
	//     "annotationId"
	//   ],
	//   "parameters": {
	//     "annotationId": {
	//       "description": "The ID for the annotation to delete.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/annotations/{annotationId}",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.annotations.insert":

type MylibraryAnnotationsInsertCall struct {
	s          *Service
	annotation *Annotation
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Insert: Inserts a new annotation.
func (r *MylibraryAnnotationsService) Insert(annotation *Annotation) *MylibraryAnnotationsInsertCall {
	c := &MylibraryAnnotationsInsertCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.annotation = annotation
	return c
}

// Country sets the optional parameter "country": ISO-3166-1 code to
// override the IP-based location.
func (c *MylibraryAnnotationsInsertCall) Country(country string) *MylibraryAnnotationsInsertCall {
	c.urlParams_.Set("country", country)
	return c
}

// ShowOnlySummaryInResponse sets the optional parameter
// "showOnlySummaryInResponse": Requests that only the summary of the
// specified layer be provided in the response.
func (c *MylibraryAnnotationsInsertCall) ShowOnlySummaryInResponse(showOnlySummaryInResponse bool) *MylibraryAnnotationsInsertCall {
	c.urlParams_.Set("showOnlySummaryInResponse", fmt.Sprint(showOnlySummaryInResponse))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryAnnotationsInsertCall) Source(source string) *MylibraryAnnotationsInsertCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryAnnotationsInsertCall) Fields(s ...googleapi.Field) *MylibraryAnnotationsInsertCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryAnnotationsInsertCall) Context(ctx context.Context) *MylibraryAnnotationsInsertCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryAnnotationsInsertCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryAnnotationsInsertCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.annotation)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/annotations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.annotations.insert" call.
// Exactly one of *Annotation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Annotation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MylibraryAnnotationsInsertCall) Do(opts ...googleapi.CallOption) (*Annotation, error) {
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
	ret := &Annotation{
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
	//   "description": "Inserts a new annotation.",
	//   "httpMethod": "POST",
	//   "id": "books.mylibrary.annotations.insert",
	//   "parameters": {
	//     "country": {
	//       "description": "ISO-3166-1 code to override the IP-based location.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showOnlySummaryInResponse": {
	//       "description": "Requests that only the summary of the specified layer be provided in the response.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/annotations",
	//   "request": {
	//     "$ref": "Annotation"
	//   },
	//   "response": {
	//     "$ref": "Annotation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.annotations.list":

type MylibraryAnnotationsListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of annotations, possibly filtered.
func (r *MylibraryAnnotationsService) List() *MylibraryAnnotationsListCall {
	c := &MylibraryAnnotationsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// ContentVersion sets the optional parameter "contentVersion": The
// content version for the requested volume.
func (c *MylibraryAnnotationsListCall) ContentVersion(contentVersion string) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// LayerId sets the optional parameter "layerId": The layer ID to limit
// annotation by.
func (c *MylibraryAnnotationsListCall) LayerId(layerId string) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("layerId", layerId)
	return c
}

// LayerIds sets the optional parameter "layerIds": The layer ID(s) to
// limit annotation by.
func (c *MylibraryAnnotationsListCall) LayerIds(layerIds ...string) *MylibraryAnnotationsListCall {
	c.urlParams_.SetMulti("layerIds", append([]string{}, layerIds...))
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return
func (c *MylibraryAnnotationsListCall) MaxResults(maxResults int64) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// PageToken sets the optional parameter "pageToken": The value of the
// nextToken from the previous page.
func (c *MylibraryAnnotationsListCall) PageToken(pageToken string) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// ShowDeleted sets the optional parameter "showDeleted": Set to true to
// return deleted annotations. updatedMin must be in the request to use
// this. Defaults to false.
func (c *MylibraryAnnotationsListCall) ShowDeleted(showDeleted bool) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("showDeleted", fmt.Sprint(showDeleted))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryAnnotationsListCall) Source(source string) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("source", source)
	return c
}

// UpdatedMax sets the optional parameter "updatedMax": RFC 3339
// timestamp to restrict to items updated prior to this timestamp
// (exclusive).
func (c *MylibraryAnnotationsListCall) UpdatedMax(updatedMax string) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("updatedMax", updatedMax)
	return c
}

// UpdatedMin sets the optional parameter "updatedMin": RFC 3339
// timestamp to restrict to items updated since this timestamp
// (inclusive).
func (c *MylibraryAnnotationsListCall) UpdatedMin(updatedMin string) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("updatedMin", updatedMin)
	return c
}

// VolumeId sets the optional parameter "volumeId": The volume to
// restrict annotations to.
func (c *MylibraryAnnotationsListCall) VolumeId(volumeId string) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("volumeId", volumeId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryAnnotationsListCall) Fields(s ...googleapi.Field) *MylibraryAnnotationsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MylibraryAnnotationsListCall) IfNoneMatch(entityTag string) *MylibraryAnnotationsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryAnnotationsListCall) Context(ctx context.Context) *MylibraryAnnotationsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryAnnotationsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryAnnotationsListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/annotations")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.annotations.list" call.
// Exactly one of *Annotations or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Annotations.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MylibraryAnnotationsListCall) Do(opts ...googleapi.CallOption) (*Annotations, error) {
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
	ret := &Annotations{
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
	//   "description": "Retrieves a list of annotations, possibly filtered.",
	//   "httpMethod": "GET",
	//   "id": "books.mylibrary.annotations.list",
	//   "parameters": {
	//     "contentVersion": {
	//       "description": "The content version for the requested volume.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "layerId": {
	//       "description": "The layer ID to limit annotation by.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "layerIds": {
	//       "description": "The layer ID(s) to limit annotation by.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "40",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The value of the nextToken from the previous page.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "showDeleted": {
	//       "description": "Set to true to return deleted annotations. updatedMin must be in the request to use this. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMax": {
	//       "description": "RFC 3339 timestamp to restrict to items updated prior to this timestamp (exclusive).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "updatedMin": {
	//       "description": "RFC 3339 timestamp to restrict to items updated since this timestamp (inclusive).",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "The volume to restrict annotations to.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/annotations",
	//   "response": {
	//     "$ref": "Annotations"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *MylibraryAnnotationsListCall) Pages(ctx context.Context, f func(*Annotations) error) error {
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

// method id "books.mylibrary.annotations.summary":

type MylibraryAnnotationsSummaryCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Summary: Gets the summary of specified layers.
func (r *MylibraryAnnotationsService) Summary(layerIds []string, volumeId string) *MylibraryAnnotationsSummaryCall {
	c := &MylibraryAnnotationsSummaryCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.SetMulti("layerIds", append([]string{}, layerIds...))
	c.urlParams_.Set("volumeId", volumeId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryAnnotationsSummaryCall) Fields(s ...googleapi.Field) *MylibraryAnnotationsSummaryCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryAnnotationsSummaryCall) Context(ctx context.Context) *MylibraryAnnotationsSummaryCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryAnnotationsSummaryCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryAnnotationsSummaryCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/annotations/summary")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.annotations.summary" call.
// Exactly one of *AnnotationsSummary or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *AnnotationsSummary.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MylibraryAnnotationsSummaryCall) Do(opts ...googleapi.CallOption) (*AnnotationsSummary, error) {
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
	ret := &AnnotationsSummary{
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
	//   "description": "Gets the summary of specified layers.",
	//   "httpMethod": "POST",
	//   "id": "books.mylibrary.annotations.summary",
	//   "parameterOrder": [
	//     "layerIds",
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "layerIds": {
	//       "description": "Array of layer IDs to get the summary for.",
	//       "location": "query",
	//       "repeated": true,
	//       "required": true,
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "Volume id to get the summary for.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/annotations/summary",
	//   "response": {
	//     "$ref": "AnnotationsSummary"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.annotations.update":

type MylibraryAnnotationsUpdateCall struct {
	s            *Service
	annotationId string
	annotation   *Annotation
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// Update: Updates an existing annotation.
func (r *MylibraryAnnotationsService) Update(annotationId string, annotation *Annotation) *MylibraryAnnotationsUpdateCall {
	c := &MylibraryAnnotationsUpdateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.annotationId = annotationId
	c.annotation = annotation
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryAnnotationsUpdateCall) Source(source string) *MylibraryAnnotationsUpdateCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryAnnotationsUpdateCall) Fields(s ...googleapi.Field) *MylibraryAnnotationsUpdateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryAnnotationsUpdateCall) Context(ctx context.Context) *MylibraryAnnotationsUpdateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryAnnotationsUpdateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryAnnotationsUpdateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.annotation)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/annotations/{annotationId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("PUT", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"annotationId": c.annotationId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.annotations.update" call.
// Exactly one of *Annotation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Annotation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MylibraryAnnotationsUpdateCall) Do(opts ...googleapi.CallOption) (*Annotation, error) {
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
	ret := &Annotation{
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
	//   "description": "Updates an existing annotation.",
	//   "httpMethod": "PUT",
	//   "id": "books.mylibrary.annotations.update",
	//   "parameterOrder": [
	//     "annotationId"
	//   ],
	//   "parameters": {
	//     "annotationId": {
	//       "description": "The ID for the annotation to update.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/annotations/{annotationId}",
	//   "request": {
	//     "$ref": "Annotation"
	//   },
	//   "response": {
	//     "$ref": "Annotation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.bookshelves.addVolume":

type MylibraryBookshelvesAddVolumeCall struct {
	s          *Service
	shelf      string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// AddVolume: Adds a volume to a bookshelf.
func (r *MylibraryBookshelvesService) AddVolume(shelf string, volumeId string) *MylibraryBookshelvesAddVolumeCall {
	c := &MylibraryBookshelvesAddVolumeCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.shelf = shelf
	c.urlParams_.Set("volumeId", volumeId)
	return c
}

// Reason sets the optional parameter "reason": The reason for which the
// book is added to the library.
//
// Possible values:
//   "IOS_PREX" - Volumes added from the PREX flow on iOS.
//   "IOS_SEARCH" - Volumes added from the Search flow on iOS.
//   "ONBOARDING" - Volumes added from the Onboarding flow.
func (c *MylibraryBookshelvesAddVolumeCall) Reason(reason string) *MylibraryBookshelvesAddVolumeCall {
	c.urlParams_.Set("reason", reason)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryBookshelvesAddVolumeCall) Source(source string) *MylibraryBookshelvesAddVolumeCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryBookshelvesAddVolumeCall) Fields(s ...googleapi.Field) *MylibraryBookshelvesAddVolumeCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryBookshelvesAddVolumeCall) Context(ctx context.Context) *MylibraryBookshelvesAddVolumeCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryBookshelvesAddVolumeCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryBookshelvesAddVolumeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/bookshelves/{shelf}/addVolume")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"shelf": c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.bookshelves.addVolume" call.
func (c *MylibraryBookshelvesAddVolumeCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Adds a volume to a bookshelf.",
	//   "httpMethod": "POST",
	//   "id": "books.mylibrary.bookshelves.addVolume",
	//   "parameterOrder": [
	//     "shelf",
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "reason": {
	//       "description": "The reason for which the book is added to the library.",
	//       "enum": [
	//         "IOS_PREX",
	//         "IOS_SEARCH",
	//         "ONBOARDING"
	//       ],
	//       "enumDescriptions": [
	//         "Volumes added from the PREX flow on iOS.",
	//         "Volumes added from the Search flow on iOS.",
	//         "Volumes added from the Onboarding flow."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "shelf": {
	//       "description": "ID of bookshelf to which to add a volume.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "ID of volume to add.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/bookshelves/{shelf}/addVolume",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.bookshelves.clearVolumes":

type MylibraryBookshelvesClearVolumesCall struct {
	s          *Service
	shelf      string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// ClearVolumes: Clears all volumes from a bookshelf.
func (r *MylibraryBookshelvesService) ClearVolumes(shelf string) *MylibraryBookshelvesClearVolumesCall {
	c := &MylibraryBookshelvesClearVolumesCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.shelf = shelf
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryBookshelvesClearVolumesCall) Source(source string) *MylibraryBookshelvesClearVolumesCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryBookshelvesClearVolumesCall) Fields(s ...googleapi.Field) *MylibraryBookshelvesClearVolumesCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryBookshelvesClearVolumesCall) Context(ctx context.Context) *MylibraryBookshelvesClearVolumesCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryBookshelvesClearVolumesCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryBookshelvesClearVolumesCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/bookshelves/{shelf}/clearVolumes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"shelf": c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.bookshelves.clearVolumes" call.
func (c *MylibraryBookshelvesClearVolumesCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Clears all volumes from a bookshelf.",
	//   "httpMethod": "POST",
	//   "id": "books.mylibrary.bookshelves.clearVolumes",
	//   "parameterOrder": [
	//     "shelf"
	//   ],
	//   "parameters": {
	//     "shelf": {
	//       "description": "ID of bookshelf from which to remove a volume.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/bookshelves/{shelf}/clearVolumes",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.bookshelves.get":

type MylibraryBookshelvesGetCall struct {
	s            *Service
	shelf        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves metadata for a specific bookshelf belonging to the
// authenticated user.
func (r *MylibraryBookshelvesService) Get(shelf string) *MylibraryBookshelvesGetCall {
	c := &MylibraryBookshelvesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.shelf = shelf
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryBookshelvesGetCall) Source(source string) *MylibraryBookshelvesGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryBookshelvesGetCall) Fields(s ...googleapi.Field) *MylibraryBookshelvesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MylibraryBookshelvesGetCall) IfNoneMatch(entityTag string) *MylibraryBookshelvesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryBookshelvesGetCall) Context(ctx context.Context) *MylibraryBookshelvesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryBookshelvesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryBookshelvesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/bookshelves/{shelf}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"shelf": c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.bookshelves.get" call.
// Exactly one of *Bookshelf or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Bookshelf.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MylibraryBookshelvesGetCall) Do(opts ...googleapi.CallOption) (*Bookshelf, error) {
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
	ret := &Bookshelf{
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
	//   "description": "Retrieves metadata for a specific bookshelf belonging to the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "books.mylibrary.bookshelves.get",
	//   "parameterOrder": [
	//     "shelf"
	//   ],
	//   "parameters": {
	//     "shelf": {
	//       "description": "ID of bookshelf to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/bookshelves/{shelf}",
	//   "response": {
	//     "$ref": "Bookshelf"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.bookshelves.list":

type MylibraryBookshelvesListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Retrieves a list of bookshelves belonging to the authenticated
// user.
func (r *MylibraryBookshelvesService) List() *MylibraryBookshelvesListCall {
	c := &MylibraryBookshelvesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryBookshelvesListCall) Source(source string) *MylibraryBookshelvesListCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryBookshelvesListCall) Fields(s ...googleapi.Field) *MylibraryBookshelvesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MylibraryBookshelvesListCall) IfNoneMatch(entityTag string) *MylibraryBookshelvesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryBookshelvesListCall) Context(ctx context.Context) *MylibraryBookshelvesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryBookshelvesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryBookshelvesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/bookshelves")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.bookshelves.list" call.
// Exactly one of *Bookshelves or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Bookshelves.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *MylibraryBookshelvesListCall) Do(opts ...googleapi.CallOption) (*Bookshelves, error) {
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
	ret := &Bookshelves{
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
	//   "description": "Retrieves a list of bookshelves belonging to the authenticated user.",
	//   "httpMethod": "GET",
	//   "id": "books.mylibrary.bookshelves.list",
	//   "parameters": {
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/bookshelves",
	//   "response": {
	//     "$ref": "Bookshelves"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.bookshelves.moveVolume":

type MylibraryBookshelvesMoveVolumeCall struct {
	s          *Service
	shelf      string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// MoveVolume: Moves a volume within a bookshelf.
func (r *MylibraryBookshelvesService) MoveVolume(shelf string, volumeId string, volumePosition int64) *MylibraryBookshelvesMoveVolumeCall {
	c := &MylibraryBookshelvesMoveVolumeCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.shelf = shelf
	c.urlParams_.Set("volumeId", volumeId)
	c.urlParams_.Set("volumePosition", fmt.Sprint(volumePosition))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryBookshelvesMoveVolumeCall) Source(source string) *MylibraryBookshelvesMoveVolumeCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryBookshelvesMoveVolumeCall) Fields(s ...googleapi.Field) *MylibraryBookshelvesMoveVolumeCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryBookshelvesMoveVolumeCall) Context(ctx context.Context) *MylibraryBookshelvesMoveVolumeCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryBookshelvesMoveVolumeCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryBookshelvesMoveVolumeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/bookshelves/{shelf}/moveVolume")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"shelf": c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.bookshelves.moveVolume" call.
func (c *MylibraryBookshelvesMoveVolumeCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Moves a volume within a bookshelf.",
	//   "httpMethod": "POST",
	//   "id": "books.mylibrary.bookshelves.moveVolume",
	//   "parameterOrder": [
	//     "shelf",
	//     "volumeId",
	//     "volumePosition"
	//   ],
	//   "parameters": {
	//     "shelf": {
	//       "description": "ID of bookshelf with the volume.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "ID of volume to move.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "volumePosition": {
	//       "description": "Position on shelf to move the item (0 puts the item before the current first item, 1 puts it between the first and the second and so on.)",
	//       "format": "int32",
	//       "location": "query",
	//       "required": true,
	//       "type": "integer"
	//     }
	//   },
	//   "path": "mylibrary/bookshelves/{shelf}/moveVolume",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.bookshelves.removeVolume":

type MylibraryBookshelvesRemoveVolumeCall struct {
	s          *Service
	shelf      string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// RemoveVolume: Removes a volume from a bookshelf.
func (r *MylibraryBookshelvesService) RemoveVolume(shelf string, volumeId string) *MylibraryBookshelvesRemoveVolumeCall {
	c := &MylibraryBookshelvesRemoveVolumeCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.shelf = shelf
	c.urlParams_.Set("volumeId", volumeId)
	return c
}

// Reason sets the optional parameter "reason": The reason for which the
// book is removed from the library.
//
// Possible values:
//   "ONBOARDING" - Samples removed from the Onboarding flow.
func (c *MylibraryBookshelvesRemoveVolumeCall) Reason(reason string) *MylibraryBookshelvesRemoveVolumeCall {
	c.urlParams_.Set("reason", reason)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryBookshelvesRemoveVolumeCall) Source(source string) *MylibraryBookshelvesRemoveVolumeCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryBookshelvesRemoveVolumeCall) Fields(s ...googleapi.Field) *MylibraryBookshelvesRemoveVolumeCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryBookshelvesRemoveVolumeCall) Context(ctx context.Context) *MylibraryBookshelvesRemoveVolumeCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryBookshelvesRemoveVolumeCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryBookshelvesRemoveVolumeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/bookshelves/{shelf}/removeVolume")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"shelf": c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.bookshelves.removeVolume" call.
func (c *MylibraryBookshelvesRemoveVolumeCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Removes a volume from a bookshelf.",
	//   "httpMethod": "POST",
	//   "id": "books.mylibrary.bookshelves.removeVolume",
	//   "parameterOrder": [
	//     "shelf",
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "reason": {
	//       "description": "The reason for which the book is removed from the library.",
	//       "enum": [
	//         "ONBOARDING"
	//       ],
	//       "enumDescriptions": [
	//         "Samples removed from the Onboarding flow."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "shelf": {
	//       "description": "ID of bookshelf from which to remove a volume.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "ID of volume to remove.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/bookshelves/{shelf}/removeVolume",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.bookshelves.volumes.list":

type MylibraryBookshelvesVolumesListCall struct {
	s            *Service
	shelf        string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Gets volume information for volumes on a bookshelf.
func (r *MylibraryBookshelvesVolumesService) List(shelf string) *MylibraryBookshelvesVolumesListCall {
	c := &MylibraryBookshelvesVolumesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.shelf = shelf
	return c
}

// Country sets the optional parameter "country": ISO-3166-1 code to
// override the IP-based location.
func (c *MylibraryBookshelvesVolumesListCall) Country(country string) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("country", country)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return
func (c *MylibraryBookshelvesVolumesListCall) MaxResults(maxResults int64) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "full" - Includes all volume data.
//   "lite" - Includes a subset of fields in volumeInfo and accessInfo.
func (c *MylibraryBookshelvesVolumesListCall) Projection(projection string) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Q sets the optional parameter "q": Full-text search query string in
// this bookshelf.
func (c *MylibraryBookshelvesVolumesListCall) Q(q string) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("q", q)
	return c
}

// ShowPreorders sets the optional parameter "showPreorders": Set to
// true to show pre-ordered books. Defaults to false.
func (c *MylibraryBookshelvesVolumesListCall) ShowPreorders(showPreorders bool) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("showPreorders", fmt.Sprint(showPreorders))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryBookshelvesVolumesListCall) Source(source string) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("source", source)
	return c
}

// StartIndex sets the optional parameter "startIndex": Index of the
// first element to return (starts at 0)
func (c *MylibraryBookshelvesVolumesListCall) StartIndex(startIndex int64) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryBookshelvesVolumesListCall) Fields(s ...googleapi.Field) *MylibraryBookshelvesVolumesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MylibraryBookshelvesVolumesListCall) IfNoneMatch(entityTag string) *MylibraryBookshelvesVolumesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryBookshelvesVolumesListCall) Context(ctx context.Context) *MylibraryBookshelvesVolumesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryBookshelvesVolumesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryBookshelvesVolumesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/bookshelves/{shelf}/volumes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"shelf": c.shelf,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.bookshelves.volumes.list" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *MylibraryBookshelvesVolumesListCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Gets volume information for volumes on a bookshelf.",
	//   "httpMethod": "GET",
	//   "id": "books.mylibrary.bookshelves.volumes.list",
	//   "parameterOrder": [
	//     "shelf"
	//   ],
	//   "parameters": {
	//     "country": {
	//       "description": "ISO-3166-1 code to override the IP-based location.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "full",
	//         "lite"
	//       ],
	//       "enumDescriptions": [
	//         "Includes all volume data.",
	//         "Includes a subset of fields in volumeInfo and accessInfo."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Full-text search query string in this bookshelf.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "shelf": {
	//       "description": "The bookshelf ID or name retrieve volumes for.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "showPreorders": {
	//       "description": "Set to true to show pre-ordered books. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startIndex": {
	//       "description": "Index of the first element to return (starts at 0)",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     }
	//   },
	//   "path": "mylibrary/bookshelves/{shelf}/volumes",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.readingpositions.get":

type MylibraryReadingpositionsGetCall struct {
	s            *Service
	volumeId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Retrieves my reading position information for a volume.
func (r *MylibraryReadingpositionsService) Get(volumeId string) *MylibraryReadingpositionsGetCall {
	c := &MylibraryReadingpositionsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	return c
}

// ContentVersion sets the optional parameter "contentVersion": Volume
// content version for which this reading position is requested.
func (c *MylibraryReadingpositionsGetCall) ContentVersion(contentVersion string) *MylibraryReadingpositionsGetCall {
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryReadingpositionsGetCall) Source(source string) *MylibraryReadingpositionsGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryReadingpositionsGetCall) Fields(s ...googleapi.Field) *MylibraryReadingpositionsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *MylibraryReadingpositionsGetCall) IfNoneMatch(entityTag string) *MylibraryReadingpositionsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryReadingpositionsGetCall) Context(ctx context.Context) *MylibraryReadingpositionsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryReadingpositionsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryReadingpositionsGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/readingpositions/{volumeId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId": c.volumeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.readingpositions.get" call.
// Exactly one of *ReadingPosition or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *ReadingPosition.ServerResponse.Header or (if a response was returned
// at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *MylibraryReadingpositionsGetCall) Do(opts ...googleapi.CallOption) (*ReadingPosition, error) {
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
	ret := &ReadingPosition{
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
	//   "description": "Retrieves my reading position information for a volume.",
	//   "httpMethod": "GET",
	//   "id": "books.mylibrary.readingpositions.get",
	//   "parameterOrder": [
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "contentVersion": {
	//       "description": "Volume content version for which this reading position is requested.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "ID of volume for which to retrieve a reading position.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/readingpositions/{volumeId}",
	//   "response": {
	//     "$ref": "ReadingPosition"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.mylibrary.readingpositions.setPosition":

type MylibraryReadingpositionsSetPositionCall struct {
	s          *Service
	volumeId   string
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// SetPosition: Sets my reading position information for a volume.
func (r *MylibraryReadingpositionsService) SetPosition(volumeId string, timestamp string, position string) *MylibraryReadingpositionsSetPositionCall {
	c := &MylibraryReadingpositionsSetPositionCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	c.urlParams_.Set("timestamp", timestamp)
	c.urlParams_.Set("position", position)
	return c
}

// Action sets the optional parameter "action": Action that caused this
// reading position to be set.
//
// Possible values:
//   "bookmark" - User chose bookmark within volume.
//   "chapter" - User selected chapter from list.
//   "next-page" - Next page event.
//   "prev-page" - Previous page event.
//   "scroll" - User navigated to page.
//   "search" - User chose search results within volume.
func (c *MylibraryReadingpositionsSetPositionCall) Action(action string) *MylibraryReadingpositionsSetPositionCall {
	c.urlParams_.Set("action", action)
	return c
}

// ContentVersion sets the optional parameter "contentVersion": Volume
// content version for which this reading position applies.
func (c *MylibraryReadingpositionsSetPositionCall) ContentVersion(contentVersion string) *MylibraryReadingpositionsSetPositionCall {
	c.urlParams_.Set("contentVersion", contentVersion)
	return c
}

// DeviceCookie sets the optional parameter "deviceCookie": Random
// persistent device cookie optional on set position.
func (c *MylibraryReadingpositionsSetPositionCall) DeviceCookie(deviceCookie string) *MylibraryReadingpositionsSetPositionCall {
	c.urlParams_.Set("deviceCookie", deviceCookie)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *MylibraryReadingpositionsSetPositionCall) Source(source string) *MylibraryReadingpositionsSetPositionCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *MylibraryReadingpositionsSetPositionCall) Fields(s ...googleapi.Field) *MylibraryReadingpositionsSetPositionCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *MylibraryReadingpositionsSetPositionCall) Context(ctx context.Context) *MylibraryReadingpositionsSetPositionCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *MylibraryReadingpositionsSetPositionCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *MylibraryReadingpositionsSetPositionCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "mylibrary/readingpositions/{volumeId}/setPosition")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId": c.volumeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.mylibrary.readingpositions.setPosition" call.
func (c *MylibraryReadingpositionsSetPositionCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "Sets my reading position information for a volume.",
	//   "httpMethod": "POST",
	//   "id": "books.mylibrary.readingpositions.setPosition",
	//   "parameterOrder": [
	//     "volumeId",
	//     "timestamp",
	//     "position"
	//   ],
	//   "parameters": {
	//     "action": {
	//       "description": "Action that caused this reading position to be set.",
	//       "enum": [
	//         "bookmark",
	//         "chapter",
	//         "next-page",
	//         "prev-page",
	//         "scroll",
	//         "search"
	//       ],
	//       "enumDescriptions": [
	//         "User chose bookmark within volume.",
	//         "User selected chapter from list.",
	//         "Next page event.",
	//         "Previous page event.",
	//         "User navigated to page.",
	//         "User chose search results within volume."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "contentVersion": {
	//       "description": "Volume content version for which this reading position applies.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "deviceCookie": {
	//       "description": "Random persistent device cookie optional on set position.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "position": {
	//       "description": "Position string for the new volume reading position.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "timestamp": {
	//       "description": "RFC 3339 UTC format timestamp associated with this reading position.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "ID of volume for which to update the reading position.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "mylibrary/readingpositions/{volumeId}/setPosition",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.notification.get":

type NotificationGetCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns notification details for a given notification id.
func (r *NotificationService) Get(notificationId string) *NotificationGetCall {
	c := &NotificationGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("notification_id", notificationId)
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Ex: 'en_US'. Used for generating
// notification title and body.
func (c *NotificationGetCall) Locale(locale string) *NotificationGetCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *NotificationGetCall) Source(source string) *NotificationGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *NotificationGetCall) Fields(s ...googleapi.Field) *NotificationGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *NotificationGetCall) IfNoneMatch(entityTag string) *NotificationGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *NotificationGetCall) Context(ctx context.Context) *NotificationGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *NotificationGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *NotificationGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "notification/get")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.notification.get" call.
// Exactly one of *Notification or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Notification.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *NotificationGetCall) Do(opts ...googleapi.CallOption) (*Notification, error) {
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
	ret := &Notification{
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
	//   "description": "Returns notification details for a given notification id.",
	//   "httpMethod": "GET",
	//   "id": "books.notification.get",
	//   "parameterOrder": [
	//     "notification_id"
	//   ],
	//   "parameters": {
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'. Used for generating notification title and body.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "notification_id": {
	//       "description": "String to identify the notification.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "notification/get",
	//   "response": {
	//     "$ref": "Notification"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.onboarding.listCategories":

type OnboardingListCategoriesCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// ListCategories: List categories for onboarding experience.
func (r *OnboardingService) ListCategories() *OnboardingListCategoriesCall {
	c := &OnboardingListCategoriesCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Default is en-US if unset.
func (c *OnboardingListCategoriesCall) Locale(locale string) *OnboardingListCategoriesCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OnboardingListCategoriesCall) Fields(s ...googleapi.Field) *OnboardingListCategoriesCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *OnboardingListCategoriesCall) IfNoneMatch(entityTag string) *OnboardingListCategoriesCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OnboardingListCategoriesCall) Context(ctx context.Context) *OnboardingListCategoriesCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OnboardingListCategoriesCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OnboardingListCategoriesCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "onboarding/listCategories")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.onboarding.listCategories" call.
// Exactly one of *Category or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Category.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *OnboardingListCategoriesCall) Do(opts ...googleapi.CallOption) (*Category, error) {
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
	ret := &Category{
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
	//   "description": "List categories for onboarding experience.",
	//   "httpMethod": "GET",
	//   "id": "books.onboarding.listCategories",
	//   "parameters": {
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Default is en-US if unset.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "onboarding/listCategories",
	//   "response": {
	//     "$ref": "Category"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.onboarding.listCategoryVolumes":

type OnboardingListCategoryVolumesCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// ListCategoryVolumes: List available volumes under categories for
// onboarding experience.
func (r *OnboardingService) ListCategoryVolumes() *OnboardingListCategoryVolumesCall {
	c := &OnboardingListCategoryVolumesCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// CategoryId sets the optional parameter "categoryId": List of category
// ids requested.
func (c *OnboardingListCategoryVolumesCall) CategoryId(categoryId ...string) *OnboardingListCategoryVolumesCall {
	c.urlParams_.SetMulti("categoryId", append([]string{}, categoryId...))
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Default is en-US if unset.
func (c *OnboardingListCategoryVolumesCall) Locale(locale string) *OnboardingListCategoryVolumesCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxAllowedMaturityRating sets the optional parameter
// "maxAllowedMaturityRating": The maximum allowed maturity rating of
// returned volumes. Books with a higher maturity rating are filtered
// out.
//
// Possible values:
//   "mature" - Show books which are rated mature or lower.
//   "not-mature" - Show books which are rated not mature.
func (c *OnboardingListCategoryVolumesCall) MaxAllowedMaturityRating(maxAllowedMaturityRating string) *OnboardingListCategoryVolumesCall {
	c.urlParams_.Set("maxAllowedMaturityRating", maxAllowedMaturityRating)
	return c
}

// PageSize sets the optional parameter "pageSize": Number of maximum
// results per page to be included in the response.
func (c *OnboardingListCategoryVolumesCall) PageSize(pageSize int64) *OnboardingListCategoryVolumesCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": The value of the
// nextToken from the previous page.
func (c *OnboardingListCategoryVolumesCall) PageToken(pageToken string) *OnboardingListCategoryVolumesCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OnboardingListCategoryVolumesCall) Fields(s ...googleapi.Field) *OnboardingListCategoryVolumesCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *OnboardingListCategoryVolumesCall) IfNoneMatch(entityTag string) *OnboardingListCategoryVolumesCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OnboardingListCategoryVolumesCall) Context(ctx context.Context) *OnboardingListCategoryVolumesCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OnboardingListCategoryVolumesCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OnboardingListCategoryVolumesCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "onboarding/listCategoryVolumes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.onboarding.listCategoryVolumes" call.
// Exactly one of *Volume2 or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volume2.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *OnboardingListCategoryVolumesCall) Do(opts ...googleapi.CallOption) (*Volume2, error) {
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
	ret := &Volume2{
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
	//   "description": "List available volumes under categories for onboarding experience.",
	//   "httpMethod": "GET",
	//   "id": "books.onboarding.listCategoryVolumes",
	//   "parameters": {
	//     "categoryId": {
	//       "description": "List of category ids requested.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Default is en-US if unset.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxAllowedMaturityRating": {
	//       "description": "The maximum allowed maturity rating of returned volumes. Books with a higher maturity rating are filtered out.",
	//       "enum": [
	//         "mature",
	//         "not-mature"
	//       ],
	//       "enumDescriptions": [
	//         "Show books which are rated mature or lower.",
	//         "Show books which are rated not mature."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "Number of maximum results per page to be included in the response.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The value of the nextToken from the previous page.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "onboarding/listCategoryVolumes",
	//   "response": {
	//     "$ref": "Volume2"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *OnboardingListCategoryVolumesCall) Pages(ctx context.Context, f func(*Volume2) error) error {
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

// method id "books.personalizedstream.get":

type PersonalizedstreamGetCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a stream of personalized book clusters
func (r *PersonalizedstreamService) Get() *PersonalizedstreamGetCall {
	c := &PersonalizedstreamGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Ex: 'en_US'. Used for generating
// recommendations.
func (c *PersonalizedstreamGetCall) Locale(locale string) *PersonalizedstreamGetCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxAllowedMaturityRating sets the optional parameter
// "maxAllowedMaturityRating": The maximum allowed maturity rating of
// returned recommendations. Books with a higher maturity rating are
// filtered out.
//
// Possible values:
//   "mature" - Show books which are rated mature or lower.
//   "not-mature" - Show books which are rated not mature.
func (c *PersonalizedstreamGetCall) MaxAllowedMaturityRating(maxAllowedMaturityRating string) *PersonalizedstreamGetCall {
	c.urlParams_.Set("maxAllowedMaturityRating", maxAllowedMaturityRating)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *PersonalizedstreamGetCall) Source(source string) *PersonalizedstreamGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PersonalizedstreamGetCall) Fields(s ...googleapi.Field) *PersonalizedstreamGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PersonalizedstreamGetCall) IfNoneMatch(entityTag string) *PersonalizedstreamGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PersonalizedstreamGetCall) Context(ctx context.Context) *PersonalizedstreamGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PersonalizedstreamGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PersonalizedstreamGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "personalizedstream/get")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.personalizedstream.get" call.
// Exactly one of *Discoveryclusters or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *Discoveryclusters.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *PersonalizedstreamGetCall) Do(opts ...googleapi.CallOption) (*Discoveryclusters, error) {
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
	ret := &Discoveryclusters{
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
	//   "description": "Returns a stream of personalized book clusters",
	//   "httpMethod": "GET",
	//   "id": "books.personalizedstream.get",
	//   "parameters": {
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'. Used for generating recommendations.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxAllowedMaturityRating": {
	//       "description": "The maximum allowed maturity rating of returned recommendations. Books with a higher maturity rating are filtered out.",
	//       "enum": [
	//         "mature",
	//         "not-mature"
	//       ],
	//       "enumDescriptions": [
	//         "Show books which are rated mature or lower.",
	//         "Show books which are rated not mature."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "personalizedstream/get",
	//   "response": {
	//     "$ref": "Discoveryclusters"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.promooffer.accept":

type PromoofferAcceptCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Accept:
func (r *PromoofferService) Accept() *PromoofferAcceptCall {
	c := &PromoofferAcceptCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// AndroidId sets the optional parameter "androidId": device android_id
func (c *PromoofferAcceptCall) AndroidId(androidId string) *PromoofferAcceptCall {
	c.urlParams_.Set("androidId", androidId)
	return c
}

// Device sets the optional parameter "device": device device
func (c *PromoofferAcceptCall) Device(device string) *PromoofferAcceptCall {
	c.urlParams_.Set("device", device)
	return c
}

// Manufacturer sets the optional parameter "manufacturer": device
// manufacturer
func (c *PromoofferAcceptCall) Manufacturer(manufacturer string) *PromoofferAcceptCall {
	c.urlParams_.Set("manufacturer", manufacturer)
	return c
}

// Model sets the optional parameter "model": device model
func (c *PromoofferAcceptCall) Model(model string) *PromoofferAcceptCall {
	c.urlParams_.Set("model", model)
	return c
}

// OfferId sets the optional parameter "offerId":
func (c *PromoofferAcceptCall) OfferId(offerId string) *PromoofferAcceptCall {
	c.urlParams_.Set("offerId", offerId)
	return c
}

// Product sets the optional parameter "product": device product
func (c *PromoofferAcceptCall) Product(product string) *PromoofferAcceptCall {
	c.urlParams_.Set("product", product)
	return c
}

// Serial sets the optional parameter "serial": device serial
func (c *PromoofferAcceptCall) Serial(serial string) *PromoofferAcceptCall {
	c.urlParams_.Set("serial", serial)
	return c
}

// VolumeId sets the optional parameter "volumeId": Volume id to
// exercise the offer
func (c *PromoofferAcceptCall) VolumeId(volumeId string) *PromoofferAcceptCall {
	c.urlParams_.Set("volumeId", volumeId)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PromoofferAcceptCall) Fields(s ...googleapi.Field) *PromoofferAcceptCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PromoofferAcceptCall) Context(ctx context.Context) *PromoofferAcceptCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PromoofferAcceptCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PromoofferAcceptCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "promooffer/accept")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.promooffer.accept" call.
func (c *PromoofferAcceptCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "",
	//   "httpMethod": "POST",
	//   "id": "books.promooffer.accept",
	//   "parameters": {
	//     "androidId": {
	//       "description": "device android_id",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "device": {
	//       "description": "device device",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "manufacturer": {
	//       "description": "device manufacturer",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "model": {
	//       "description": "device model",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "offerId": {
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "product": {
	//       "description": "device product",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "serial": {
	//       "description": "device serial",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "Volume id to exercise the offer",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "promooffer/accept",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.promooffer.dismiss":

type PromoofferDismissCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Dismiss:
func (r *PromoofferService) Dismiss() *PromoofferDismissCall {
	c := &PromoofferDismissCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// AndroidId sets the optional parameter "androidId": device android_id
func (c *PromoofferDismissCall) AndroidId(androidId string) *PromoofferDismissCall {
	c.urlParams_.Set("androidId", androidId)
	return c
}

// Device sets the optional parameter "device": device device
func (c *PromoofferDismissCall) Device(device string) *PromoofferDismissCall {
	c.urlParams_.Set("device", device)
	return c
}

// Manufacturer sets the optional parameter "manufacturer": device
// manufacturer
func (c *PromoofferDismissCall) Manufacturer(manufacturer string) *PromoofferDismissCall {
	c.urlParams_.Set("manufacturer", manufacturer)
	return c
}

// Model sets the optional parameter "model": device model
func (c *PromoofferDismissCall) Model(model string) *PromoofferDismissCall {
	c.urlParams_.Set("model", model)
	return c
}

// OfferId sets the optional parameter "offerId": Offer to dimiss
func (c *PromoofferDismissCall) OfferId(offerId string) *PromoofferDismissCall {
	c.urlParams_.Set("offerId", offerId)
	return c
}

// Product sets the optional parameter "product": device product
func (c *PromoofferDismissCall) Product(product string) *PromoofferDismissCall {
	c.urlParams_.Set("product", product)
	return c
}

// Serial sets the optional parameter "serial": device serial
func (c *PromoofferDismissCall) Serial(serial string) *PromoofferDismissCall {
	c.urlParams_.Set("serial", serial)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PromoofferDismissCall) Fields(s ...googleapi.Field) *PromoofferDismissCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PromoofferDismissCall) Context(ctx context.Context) *PromoofferDismissCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PromoofferDismissCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PromoofferDismissCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "promooffer/dismiss")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.promooffer.dismiss" call.
func (c *PromoofferDismissCall) Do(opts ...googleapi.CallOption) error {
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
	//   "description": "",
	//   "httpMethod": "POST",
	//   "id": "books.promooffer.dismiss",
	//   "parameters": {
	//     "androidId": {
	//       "description": "device android_id",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "device": {
	//       "description": "device device",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "manufacturer": {
	//       "description": "device manufacturer",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "model": {
	//       "description": "device model",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "offerId": {
	//       "description": "Offer to dimiss",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "product": {
	//       "description": "device product",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "serial": {
	//       "description": "device serial",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "promooffer/dismiss",
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.promooffer.get":

type PromoofferGetCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns a list of promo offers available to the user
func (r *PromoofferService) Get() *PromoofferGetCall {
	c := &PromoofferGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// AndroidId sets the optional parameter "androidId": device android_id
func (c *PromoofferGetCall) AndroidId(androidId string) *PromoofferGetCall {
	c.urlParams_.Set("androidId", androidId)
	return c
}

// Device sets the optional parameter "device": device device
func (c *PromoofferGetCall) Device(device string) *PromoofferGetCall {
	c.urlParams_.Set("device", device)
	return c
}

// Manufacturer sets the optional parameter "manufacturer": device
// manufacturer
func (c *PromoofferGetCall) Manufacturer(manufacturer string) *PromoofferGetCall {
	c.urlParams_.Set("manufacturer", manufacturer)
	return c
}

// Model sets the optional parameter "model": device model
func (c *PromoofferGetCall) Model(model string) *PromoofferGetCall {
	c.urlParams_.Set("model", model)
	return c
}

// Product sets the optional parameter "product": device product
func (c *PromoofferGetCall) Product(product string) *PromoofferGetCall {
	c.urlParams_.Set("product", product)
	return c
}

// Serial sets the optional parameter "serial": device serial
func (c *PromoofferGetCall) Serial(serial string) *PromoofferGetCall {
	c.urlParams_.Set("serial", serial)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *PromoofferGetCall) Fields(s ...googleapi.Field) *PromoofferGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *PromoofferGetCall) IfNoneMatch(entityTag string) *PromoofferGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *PromoofferGetCall) Context(ctx context.Context) *PromoofferGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *PromoofferGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *PromoofferGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "promooffer/get")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.promooffer.get" call.
// Exactly one of *Offers or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Offers.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *PromoofferGetCall) Do(opts ...googleapi.CallOption) (*Offers, error) {
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
	ret := &Offers{
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
	//   "description": "Returns a list of promo offers available to the user",
	//   "httpMethod": "GET",
	//   "id": "books.promooffer.get",
	//   "parameters": {
	//     "androidId": {
	//       "description": "device android_id",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "device": {
	//       "description": "device device",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "manufacturer": {
	//       "description": "device manufacturer",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "model": {
	//       "description": "device model",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "product": {
	//       "description": "device product",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "serial": {
	//       "description": "device serial",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "promooffer/get",
	//   "response": {
	//     "$ref": "Offers"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.series.get":

type SeriesGetCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns Series metadata for the given series ids.
func (r *SeriesService) Get(seriesId []string) *SeriesGetCall {
	c := &SeriesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.SetMulti("series_id", append([]string{}, seriesId...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SeriesGetCall) Fields(s ...googleapi.Field) *SeriesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SeriesGetCall) IfNoneMatch(entityTag string) *SeriesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SeriesGetCall) Context(ctx context.Context) *SeriesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SeriesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SeriesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "series/get")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.series.get" call.
// Exactly one of *Series or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Series.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *SeriesGetCall) Do(opts ...googleapi.CallOption) (*Series, error) {
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
	ret := &Series{
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
	//   "description": "Returns Series metadata for the given series ids.",
	//   "httpMethod": "GET",
	//   "id": "books.series.get",
	//   "parameterOrder": [
	//     "series_id"
	//   ],
	//   "parameters": {
	//     "series_id": {
	//       "description": "String that identifies the series",
	//       "location": "query",
	//       "repeated": true,
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "series/get",
	//   "response": {
	//     "$ref": "Series"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.series.membership.get":

type SeriesMembershipGetCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Returns Series membership data given the series id.
func (r *SeriesMembershipService) Get(seriesId string) *SeriesMembershipGetCall {
	c := &SeriesMembershipGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("series_id", seriesId)
	return c
}

// PageSize sets the optional parameter "page_size": Number of maximum
// results per page to be included in the response.
func (c *SeriesMembershipGetCall) PageSize(pageSize int64) *SeriesMembershipGetCall {
	c.urlParams_.Set("page_size", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "page_token": The value of the
// nextToken from the previous page.
func (c *SeriesMembershipGetCall) PageToken(pageToken string) *SeriesMembershipGetCall {
	c.urlParams_.Set("page_token", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SeriesMembershipGetCall) Fields(s ...googleapi.Field) *SeriesMembershipGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *SeriesMembershipGetCall) IfNoneMatch(entityTag string) *SeriesMembershipGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SeriesMembershipGetCall) Context(ctx context.Context) *SeriesMembershipGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SeriesMembershipGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SeriesMembershipGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "series/membership/get")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.series.membership.get" call.
// Exactly one of *Seriesmembership or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *Seriesmembership.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SeriesMembershipGetCall) Do(opts ...googleapi.CallOption) (*Seriesmembership, error) {
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
	ret := &Seriesmembership{
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
	//   "description": "Returns Series membership data given the series id.",
	//   "httpMethod": "GET",
	//   "id": "books.series.membership.get",
	//   "parameterOrder": [
	//     "series_id"
	//   ],
	//   "parameters": {
	//     "page_size": {
	//       "description": "Number of maximum results per page to be included in the response.",
	//       "format": "uint32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "page_token": {
	//       "description": "The value of the nextToken from the previous page.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "series_id": {
	//       "description": "String that identifies the series",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "series/membership/get",
	//   "response": {
	//     "$ref": "Seriesmembership"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.volumes.get":

type VolumesGetCall struct {
	s            *Service
	volumeId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets volume information for a single volume.
func (r *VolumesService) Get(volumeId string) *VolumesGetCall {
	c := &VolumesGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	return c
}

// Country sets the optional parameter "country": ISO-3166-1 code to
// override the IP-based location.
func (c *VolumesGetCall) Country(country string) *VolumesGetCall {
	c.urlParams_.Set("country", country)
	return c
}

// IncludeNonComicsSeries sets the optional parameter
// "includeNonComicsSeries": Set to true to include non-comics series.
// Defaults to false.
func (c *VolumesGetCall) IncludeNonComicsSeries(includeNonComicsSeries bool) *VolumesGetCall {
	c.urlParams_.Set("includeNonComicsSeries", fmt.Sprint(includeNonComicsSeries))
	return c
}

// Partner sets the optional parameter "partner": Brand results for
// partner ID.
func (c *VolumesGetCall) Partner(partner string) *VolumesGetCall {
	c.urlParams_.Set("partner", partner)
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "full" - Includes all volume data.
//   "lite" - Includes a subset of fields in volumeInfo and accessInfo.
func (c *VolumesGetCall) Projection(projection string) *VolumesGetCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *VolumesGetCall) Source(source string) *VolumesGetCall {
	c.urlParams_.Set("source", source)
	return c
}

// UserLibraryConsistentRead sets the optional parameter
// "user_library_consistent_read":
func (c *VolumesGetCall) UserLibraryConsistentRead(userLibraryConsistentRead bool) *VolumesGetCall {
	c.urlParams_.Set("user_library_consistent_read", fmt.Sprint(userLibraryConsistentRead))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VolumesGetCall) Fields(s ...googleapi.Field) *VolumesGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *VolumesGetCall) IfNoneMatch(entityTag string) *VolumesGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VolumesGetCall) Context(ctx context.Context) *VolumesGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VolumesGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VolumesGetCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId": c.volumeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.volumes.get" call.
// Exactly one of *Volume or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volume.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *VolumesGetCall) Do(opts ...googleapi.CallOption) (*Volume, error) {
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
	ret := &Volume{
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
	//   "description": "Gets volume information for a single volume.",
	//   "httpMethod": "GET",
	//   "id": "books.volumes.get",
	//   "parameterOrder": [
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "country": {
	//       "description": "ISO-3166-1 code to override the IP-based location.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "includeNonComicsSeries": {
	//       "description": "Set to true to include non-comics series. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "partner": {
	//       "description": "Brand results for partner ID.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "full",
	//         "lite"
	//       ],
	//       "enumDescriptions": [
	//         "Includes all volume data.",
	//         "Includes a subset of fields in volumeInfo and accessInfo."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "user_library_consistent_read": {
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "volumeId": {
	//       "description": "ID of volume to retrieve.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/{volumeId}",
	//   "response": {
	//     "$ref": "Volume"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.volumes.list":

type VolumesListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Performs a book search.
func (r *VolumesService) List(q string) *VolumesListCall {
	c := &VolumesListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("q", q)
	return c
}

// Download sets the optional parameter "download": Restrict to volumes
// by download availability.
//
// Possible values:
//   "epub" - All volumes with epub.
func (c *VolumesListCall) Download(download string) *VolumesListCall {
	c.urlParams_.Set("download", download)
	return c
}

// Filter sets the optional parameter "filter": Filter search results.
//
// Possible values:
//   "ebooks" - All Google eBooks.
//   "free-ebooks" - Google eBook with full volume text viewability.
//   "full" - Public can view entire volume text.
//   "paid-ebooks" - Google eBook with a price.
//   "partial" - Public able to see parts of text.
func (c *VolumesListCall) Filter(filter string) *VolumesListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// LangRestrict sets the optional parameter "langRestrict": Restrict
// results to books with this language code.
func (c *VolumesListCall) LangRestrict(langRestrict string) *VolumesListCall {
	c.urlParams_.Set("langRestrict", langRestrict)
	return c
}

// LibraryRestrict sets the optional parameter "libraryRestrict":
// Restrict search to this user's library.
//
// Possible values:
//   "my-library" - Restrict to the user's library, any shelf.
//   "no-restrict" - Do not restrict based on user's library.
func (c *VolumesListCall) LibraryRestrict(libraryRestrict string) *VolumesListCall {
	c.urlParams_.Set("libraryRestrict", libraryRestrict)
	return c
}

// MaxAllowedMaturityRating sets the optional parameter
// "maxAllowedMaturityRating": The maximum allowed maturity rating of
// returned recommendations. Books with a higher maturity rating are
// filtered out.
//
// Possible values:
//   "mature" - Show books which are rated mature or lower.
//   "not-mature" - Show books which are rated not mature.
func (c *VolumesListCall) MaxAllowedMaturityRating(maxAllowedMaturityRating string) *VolumesListCall {
	c.urlParams_.Set("maxAllowedMaturityRating", maxAllowedMaturityRating)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return.
func (c *VolumesListCall) MaxResults(maxResults int64) *VolumesListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// OrderBy sets the optional parameter "orderBy": Sort search results.
//
// Possible values:
//   "newest" - Most recently published.
//   "relevance" - Relevance to search terms.
func (c *VolumesListCall) OrderBy(orderBy string) *VolumesListCall {
	c.urlParams_.Set("orderBy", orderBy)
	return c
}

// Partner sets the optional parameter "partner": Restrict and brand
// results for partner ID.
func (c *VolumesListCall) Partner(partner string) *VolumesListCall {
	c.urlParams_.Set("partner", partner)
	return c
}

// PrintType sets the optional parameter "printType": Restrict to books
// or magazines.
//
// Possible values:
//   "all" - All volume content types.
//   "books" - Just books.
//   "magazines" - Just magazines.
func (c *VolumesListCall) PrintType(printType string) *VolumesListCall {
	c.urlParams_.Set("printType", printType)
	return c
}

// Projection sets the optional parameter "projection": Restrict
// information returned to a set of selected fields.
//
// Possible values:
//   "full" - Includes all volume data.
//   "lite" - Includes a subset of fields in volumeInfo and accessInfo.
func (c *VolumesListCall) Projection(projection string) *VolumesListCall {
	c.urlParams_.Set("projection", projection)
	return c
}

// ShowPreorders sets the optional parameter "showPreorders": Set to
// true to show books available for preorder. Defaults to false.
func (c *VolumesListCall) ShowPreorders(showPreorders bool) *VolumesListCall {
	c.urlParams_.Set("showPreorders", fmt.Sprint(showPreorders))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *VolumesListCall) Source(source string) *VolumesListCall {
	c.urlParams_.Set("source", source)
	return c
}

// StartIndex sets the optional parameter "startIndex": Index of the
// first result to return (starts at 0)
func (c *VolumesListCall) StartIndex(startIndex int64) *VolumesListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VolumesListCall) Fields(s ...googleapi.Field) *VolumesListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *VolumesListCall) IfNoneMatch(entityTag string) *VolumesListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VolumesListCall) Context(ctx context.Context) *VolumesListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VolumesListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VolumesListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.volumes.list" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *VolumesListCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Performs a book search.",
	//   "httpMethod": "GET",
	//   "id": "books.volumes.list",
	//   "parameterOrder": [
	//     "q"
	//   ],
	//   "parameters": {
	//     "download": {
	//       "description": "Restrict to volumes by download availability.",
	//       "enum": [
	//         "epub"
	//       ],
	//       "enumDescriptions": [
	//         "All volumes with epub."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "filter": {
	//       "description": "Filter search results.",
	//       "enum": [
	//         "ebooks",
	//         "free-ebooks",
	//         "full",
	//         "paid-ebooks",
	//         "partial"
	//       ],
	//       "enumDescriptions": [
	//         "All Google eBooks.",
	//         "Google eBook with full volume text viewability.",
	//         "Public can view entire volume text.",
	//         "Google eBook with a price.",
	//         "Public able to see parts of text."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "langRestrict": {
	//       "description": "Restrict results to books with this language code.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "libraryRestrict": {
	//       "description": "Restrict search to this user's library.",
	//       "enum": [
	//         "my-library",
	//         "no-restrict"
	//       ],
	//       "enumDescriptions": [
	//         "Restrict to the user's library, any shelf.",
	//         "Do not restrict based on user's library."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxAllowedMaturityRating": {
	//       "description": "The maximum allowed maturity rating of returned recommendations. Books with a higher maturity rating are filtered out.",
	//       "enum": [
	//         "mature",
	//         "not-mature"
	//       ],
	//       "enumDescriptions": [
	//         "Show books which are rated mature or lower.",
	//         "Show books which are rated not mature."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return.",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "40",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "orderBy": {
	//       "description": "Sort search results.",
	//       "enum": [
	//         "newest",
	//         "relevance"
	//       ],
	//       "enumDescriptions": [
	//         "Most recently published.",
	//         "Relevance to search terms."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "partner": {
	//       "description": "Restrict and brand results for partner ID.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "printType": {
	//       "description": "Restrict to books or magazines.",
	//       "enum": [
	//         "all",
	//         "books",
	//         "magazines"
	//       ],
	//       "enumDescriptions": [
	//         "All volume content types.",
	//         "Just books.",
	//         "Just magazines."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projection": {
	//       "description": "Restrict information returned to a set of selected fields.",
	//       "enum": [
	//         "full",
	//         "lite"
	//       ],
	//       "enumDescriptions": [
	//         "Includes all volume data.",
	//         "Includes a subset of fields in volumeInfo and accessInfo."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "q": {
	//       "description": "Full-text search query string.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "showPreorders": {
	//       "description": "Set to true to show books available for preorder. Defaults to false.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startIndex": {
	//       "description": "Index of the first result to return (starts at 0)",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     }
	//   },
	//   "path": "volumes",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.volumes.associated.list":

type VolumesAssociatedListCall struct {
	s            *Service
	volumeId     string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Return a list of associated books.
func (r *VolumesAssociatedService) List(volumeId string) *VolumesAssociatedListCall {
	c := &VolumesAssociatedListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.volumeId = volumeId
	return c
}

// Association sets the optional parameter "association": Association
// type.
//
// Possible values:
//   "end-of-sample" - Recommendations for display end-of-sample.
//   "end-of-volume" - Recommendations for display end-of-volume.
//   "related-for-play" - Related volumes for Play Store.
func (c *VolumesAssociatedListCall) Association(association string) *VolumesAssociatedListCall {
	c.urlParams_.Set("association", association)
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Ex: 'en_US'. Used for generating
// recommendations.
func (c *VolumesAssociatedListCall) Locale(locale string) *VolumesAssociatedListCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxAllowedMaturityRating sets the optional parameter
// "maxAllowedMaturityRating": The maximum allowed maturity rating of
// returned recommendations. Books with a higher maturity rating are
// filtered out.
//
// Possible values:
//   "mature" - Show books which are rated mature or lower.
//   "not-mature" - Show books which are rated not mature.
func (c *VolumesAssociatedListCall) MaxAllowedMaturityRating(maxAllowedMaturityRating string) *VolumesAssociatedListCall {
	c.urlParams_.Set("maxAllowedMaturityRating", maxAllowedMaturityRating)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *VolumesAssociatedListCall) Source(source string) *VolumesAssociatedListCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VolumesAssociatedListCall) Fields(s ...googleapi.Field) *VolumesAssociatedListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *VolumesAssociatedListCall) IfNoneMatch(entityTag string) *VolumesAssociatedListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VolumesAssociatedListCall) Context(ctx context.Context) *VolumesAssociatedListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VolumesAssociatedListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VolumesAssociatedListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/{volumeId}/associated")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"volumeId": c.volumeId,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.volumes.associated.list" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *VolumesAssociatedListCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Return a list of associated books.",
	//   "httpMethod": "GET",
	//   "id": "books.volumes.associated.list",
	//   "parameterOrder": [
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "association": {
	//       "description": "Association type.",
	//       "enum": [
	//         "end-of-sample",
	//         "end-of-volume",
	//         "related-for-play"
	//       ],
	//       "enumDescriptions": [
	//         "Recommendations for display end-of-sample.",
	//         "Recommendations for display end-of-volume.",
	//         "Related volumes for Play Store."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'. Used for generating recommendations.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxAllowedMaturityRating": {
	//       "description": "The maximum allowed maturity rating of returned recommendations. Books with a higher maturity rating are filtered out.",
	//       "enum": [
	//         "mature",
	//         "not-mature"
	//       ],
	//       "enumDescriptions": [
	//         "Show books which are rated mature or lower.",
	//         "Show books which are rated not mature."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "ID of the source volume.",
	//       "location": "path",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/{volumeId}/associated",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.volumes.mybooks.list":

type VolumesMybooksListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Return a list of books in My Library.
func (r *VolumesMybooksService) List() *VolumesMybooksListCall {
	c := &VolumesMybooksListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// AcquireMethod sets the optional parameter "acquireMethod": How the
// book was acquired
//
// Possible values:
//   "FAMILY_SHARED" - Books acquired via Family Sharing
//   "PREORDERED" - Preordered books (not yet available)
//   "PREVIOUSLY_RENTED" - User-rented books past their expiration time
//   "PUBLIC_DOMAIN" - Public domain books
//   "PURCHASED" - Purchased books
//   "RENTED" - User-rented books
//   "SAMPLE" - Sample books
//   "UPLOADED" - User uploaded books
func (c *VolumesMybooksListCall) AcquireMethod(acquireMethod ...string) *VolumesMybooksListCall {
	c.urlParams_.SetMulti("acquireMethod", append([]string{}, acquireMethod...))
	return c
}

// Country sets the optional parameter "country": ISO-3166-1 code to
// override the IP-based location.
func (c *VolumesMybooksListCall) Country(country string) *VolumesMybooksListCall {
	c.urlParams_.Set("country", country)
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Ex:'en_US'. Used for generating
// recommendations.
func (c *VolumesMybooksListCall) Locale(locale string) *VolumesMybooksListCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return.
func (c *VolumesMybooksListCall) MaxResults(maxResults int64) *VolumesMybooksListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// ProcessingState sets the optional parameter "processingState": The
// processing state of the user uploaded volumes to be returned.
// Applicable only if the UPLOADED is specified in the acquireMethod.
//
// Possible values:
//   "COMPLETED_FAILED" - The volume processing hase failed.
//   "COMPLETED_SUCCESS" - The volume processing was completed.
//   "RUNNING" - The volume processing is not completed.
func (c *VolumesMybooksListCall) ProcessingState(processingState ...string) *VolumesMybooksListCall {
	c.urlParams_.SetMulti("processingState", append([]string{}, processingState...))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *VolumesMybooksListCall) Source(source string) *VolumesMybooksListCall {
	c.urlParams_.Set("source", source)
	return c
}

// StartIndex sets the optional parameter "startIndex": Index of the
// first result to return (starts at 0)
func (c *VolumesMybooksListCall) StartIndex(startIndex int64) *VolumesMybooksListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VolumesMybooksListCall) Fields(s ...googleapi.Field) *VolumesMybooksListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *VolumesMybooksListCall) IfNoneMatch(entityTag string) *VolumesMybooksListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VolumesMybooksListCall) Context(ctx context.Context) *VolumesMybooksListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VolumesMybooksListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VolumesMybooksListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/mybooks")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.volumes.mybooks.list" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *VolumesMybooksListCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Return a list of books in My Library.",
	//   "httpMethod": "GET",
	//   "id": "books.volumes.mybooks.list",
	//   "parameters": {
	//     "acquireMethod": {
	//       "description": "How the book was acquired",
	//       "enum": [
	//         "FAMILY_SHARED",
	//         "PREORDERED",
	//         "PREVIOUSLY_RENTED",
	//         "PUBLIC_DOMAIN",
	//         "PURCHASED",
	//         "RENTED",
	//         "SAMPLE",
	//         "UPLOADED"
	//       ],
	//       "enumDescriptions": [
	//         "Books acquired via Family Sharing",
	//         "Preordered books (not yet available)",
	//         "User-rented books past their expiration time",
	//         "Public domain books",
	//         "Purchased books",
	//         "User-rented books",
	//         "Sample books",
	//         "User uploaded books"
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "country": {
	//       "description": "ISO-3166-1 code to override the IP-based location.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Ex:'en_US'. Used for generating recommendations.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return.",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "100",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "processingState": {
	//       "description": "The processing state of the user uploaded volumes to be returned. Applicable only if the UPLOADED is specified in the acquireMethod.",
	//       "enum": [
	//         "COMPLETED_FAILED",
	//         "COMPLETED_SUCCESS",
	//         "RUNNING"
	//       ],
	//       "enumDescriptions": [
	//         "The volume processing hase failed.",
	//         "The volume processing was completed.",
	//         "The volume processing is not completed."
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startIndex": {
	//       "description": "Index of the first result to return (starts at 0)",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     }
	//   },
	//   "path": "volumes/mybooks",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.volumes.recommended.list":

type VolumesRecommendedListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Return a list of recommended books for the current user.
func (r *VolumesRecommendedService) List() *VolumesRecommendedListCall {
	c := &VolumesRecommendedListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Ex: 'en_US'. Used for generating
// recommendations.
func (c *VolumesRecommendedListCall) Locale(locale string) *VolumesRecommendedListCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxAllowedMaturityRating sets the optional parameter
// "maxAllowedMaturityRating": The maximum allowed maturity rating of
// returned recommendations. Books with a higher maturity rating are
// filtered out.
//
// Possible values:
//   "mature" - Show books which are rated mature or lower.
//   "not-mature" - Show books which are rated not mature.
func (c *VolumesRecommendedListCall) MaxAllowedMaturityRating(maxAllowedMaturityRating string) *VolumesRecommendedListCall {
	c.urlParams_.Set("maxAllowedMaturityRating", maxAllowedMaturityRating)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *VolumesRecommendedListCall) Source(source string) *VolumesRecommendedListCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VolumesRecommendedListCall) Fields(s ...googleapi.Field) *VolumesRecommendedListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *VolumesRecommendedListCall) IfNoneMatch(entityTag string) *VolumesRecommendedListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VolumesRecommendedListCall) Context(ctx context.Context) *VolumesRecommendedListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VolumesRecommendedListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VolumesRecommendedListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/recommended")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.volumes.recommended.list" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *VolumesRecommendedListCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Return a list of recommended books for the current user.",
	//   "httpMethod": "GET",
	//   "id": "books.volumes.recommended.list",
	//   "parameters": {
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'. Used for generating recommendations.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxAllowedMaturityRating": {
	//       "description": "The maximum allowed maturity rating of returned recommendations. Books with a higher maturity rating are filtered out.",
	//       "enum": [
	//         "mature",
	//         "not-mature"
	//       ],
	//       "enumDescriptions": [
	//         "Show books which are rated mature or lower.",
	//         "Show books which are rated not mature."
	//       ],
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/recommended",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.volumes.recommended.rate":

type VolumesRecommendedRateCall struct {
	s          *Service
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Rate: Rate a recommended book for the current user.
func (r *VolumesRecommendedService) Rate(rating string, volumeId string) *VolumesRecommendedRateCall {
	c := &VolumesRecommendedRateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.urlParams_.Set("rating", rating)
	c.urlParams_.Set("volumeId", volumeId)
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Ex: 'en_US'. Used for generating
// recommendations.
func (c *VolumesRecommendedRateCall) Locale(locale string) *VolumesRecommendedRateCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *VolumesRecommendedRateCall) Source(source string) *VolumesRecommendedRateCall {
	c.urlParams_.Set("source", source)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VolumesRecommendedRateCall) Fields(s ...googleapi.Field) *VolumesRecommendedRateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VolumesRecommendedRateCall) Context(ctx context.Context) *VolumesRecommendedRateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VolumesRecommendedRateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VolumesRecommendedRateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/recommended/rate")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.volumes.recommended.rate" call.
// Exactly one of *BooksVolumesRecommendedRateResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *BooksVolumesRecommendedRateResponse.ServerResponse.Header or
// (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *VolumesRecommendedRateCall) Do(opts ...googleapi.CallOption) (*BooksVolumesRecommendedRateResponse, error) {
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
	ret := &BooksVolumesRecommendedRateResponse{
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
	//   "description": "Rate a recommended book for the current user.",
	//   "httpMethod": "POST",
	//   "id": "books.volumes.recommended.rate",
	//   "parameterOrder": [
	//     "rating",
	//     "volumeId"
	//   ],
	//   "parameters": {
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'. Used for generating recommendations.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "rating": {
	//       "description": "Rating to be given to the volume.",
	//       "enum": [
	//         "HAVE_IT",
	//         "NOT_INTERESTED"
	//       ],
	//       "enumDescriptions": [
	//         "Rating indicating a dismissal due to ownership.",
	//         "Rating indicating a negative dismissal of a volume."
	//       ],
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "volumeId": {
	//       "description": "ID of the source volume.",
	//       "location": "query",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/recommended/rate",
	//   "response": {
	//     "$ref": "BooksVolumesRecommendedRateResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}

// method id "books.volumes.useruploaded.list":

type VolumesUseruploadedListCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Return a list of books uploaded by the current user.
func (r *VolumesUseruploadedService) List() *VolumesUseruploadedListCall {
	c := &VolumesUseruploadedListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Locale sets the optional parameter "locale": ISO-639-1 language and
// ISO-3166-1 country code. Ex: 'en_US'. Used for generating
// recommendations.
func (c *VolumesUseruploadedListCall) Locale(locale string) *VolumesUseruploadedListCall {
	c.urlParams_.Set("locale", locale)
	return c
}

// MaxResults sets the optional parameter "maxResults": Maximum number
// of results to return.
func (c *VolumesUseruploadedListCall) MaxResults(maxResults int64) *VolumesUseruploadedListCall {
	c.urlParams_.Set("maxResults", fmt.Sprint(maxResults))
	return c
}

// ProcessingState sets the optional parameter "processingState": The
// processing state of the user uploaded volumes to be returned.
//
// Possible values:
//   "COMPLETED_FAILED" - The volume processing hase failed.
//   "COMPLETED_SUCCESS" - The volume processing was completed.
//   "RUNNING" - The volume processing is not completed.
func (c *VolumesUseruploadedListCall) ProcessingState(processingState ...string) *VolumesUseruploadedListCall {
	c.urlParams_.SetMulti("processingState", append([]string{}, processingState...))
	return c
}

// Source sets the optional parameter "source": String to identify the
// originator of this request.
func (c *VolumesUseruploadedListCall) Source(source string) *VolumesUseruploadedListCall {
	c.urlParams_.Set("source", source)
	return c
}

// StartIndex sets the optional parameter "startIndex": Index of the
// first result to return (starts at 0)
func (c *VolumesUseruploadedListCall) StartIndex(startIndex int64) *VolumesUseruploadedListCall {
	c.urlParams_.Set("startIndex", fmt.Sprint(startIndex))
	return c
}

// VolumeId sets the optional parameter "volumeId": The ids of the
// volumes to be returned. If not specified all that match the
// processingState are returned.
func (c *VolumesUseruploadedListCall) VolumeId(volumeId ...string) *VolumesUseruploadedListCall {
	c.urlParams_.SetMulti("volumeId", append([]string{}, volumeId...))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *VolumesUseruploadedListCall) Fields(s ...googleapi.Field) *VolumesUseruploadedListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *VolumesUseruploadedListCall) IfNoneMatch(entityTag string) *VolumesUseruploadedListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *VolumesUseruploadedListCall) Context(ctx context.Context) *VolumesUseruploadedListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *VolumesUseruploadedListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *VolumesUseruploadedListCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "volumes/useruploaded")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "books.volumes.useruploaded.list" call.
// Exactly one of *Volumes or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Volumes.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *VolumesUseruploadedListCall) Do(opts ...googleapi.CallOption) (*Volumes, error) {
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
	ret := &Volumes{
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
	//   "description": "Return a list of books uploaded by the current user.",
	//   "httpMethod": "GET",
	//   "id": "books.volumes.useruploaded.list",
	//   "parameters": {
	//     "locale": {
	//       "description": "ISO-639-1 language and ISO-3166-1 country code. Ex: 'en_US'. Used for generating recommendations.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "maxResults": {
	//       "description": "Maximum number of results to return.",
	//       "format": "uint32",
	//       "location": "query",
	//       "maximum": "40",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "processingState": {
	//       "description": "The processing state of the user uploaded volumes to be returned.",
	//       "enum": [
	//         "COMPLETED_FAILED",
	//         "COMPLETED_SUCCESS",
	//         "RUNNING"
	//       ],
	//       "enumDescriptions": [
	//         "The volume processing hase failed.",
	//         "The volume processing was completed.",
	//         "The volume processing is not completed."
	//       ],
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     },
	//     "source": {
	//       "description": "String to identify the originator of this request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "startIndex": {
	//       "description": "Index of the first result to return (starts at 0)",
	//       "format": "uint32",
	//       "location": "query",
	//       "minimum": "0",
	//       "type": "integer"
	//     },
	//     "volumeId": {
	//       "description": "The ids of the volumes to be returned. If not specified all that match the processingState are returned.",
	//       "location": "query",
	//       "repeated": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "volumes/useruploaded",
	//   "response": {
	//     "$ref": "Volumes"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/books"
	//   ]
	// }

}
