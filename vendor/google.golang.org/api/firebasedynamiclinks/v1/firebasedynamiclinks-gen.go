// Package firebasedynamiclinks provides access to the Firebase Dynamic Links API.
//
// See https://firebase.google.com/docs/dynamic-links/
//
// Usage example:
//
//   import "google.golang.org/api/firebasedynamiclinks/v1"
//   ...
//   firebasedynamiclinksService, err := firebasedynamiclinks.New(oauthHttpClient)
package firebasedynamiclinks // import "google.golang.org/api/firebasedynamiclinks/v1"

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

const apiId = "firebasedynamiclinks:v1"
const apiName = "firebasedynamiclinks"
const apiVersion = "v1"
const basePath = "https://firebasedynamiclinks.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and administer all your Firebase data and settings
	FirebaseScope = "https://www.googleapis.com/auth/firebase"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.ShortLinks = NewShortLinksService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	ShortLinks *ShortLinksService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewShortLinksService(s *Service) *ShortLinksService {
	rs := &ShortLinksService{s: s}
	return rs
}

type ShortLinksService struct {
	s *Service
}

// AnalyticsInfo: Tracking parameters supported by Dynamic Link.
type AnalyticsInfo struct {
	// GooglePlayAnalytics: Google Play Campaign Measurements.
	GooglePlayAnalytics *GooglePlayAnalytics `json:"googlePlayAnalytics,omitempty"`

	// ItunesConnectAnalytics: iTunes Connect App Analytics.
	ItunesConnectAnalytics *ITunesConnectAnalytics `json:"itunesConnectAnalytics,omitempty"`

	// ForceSendFields is a list of field names (e.g. "GooglePlayAnalytics")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "GooglePlayAnalytics") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *AnalyticsInfo) MarshalJSON() ([]byte, error) {
	type noMethod AnalyticsInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// AndroidInfo: Android related attributes to the Dynamic Link.
type AndroidInfo struct {
	// AndroidFallbackLink: Link to open on Android if the app is not
	// installed.
	AndroidFallbackLink string `json:"androidFallbackLink,omitempty"`

	// AndroidLink: If specified, this overrides the ‘link’ parameter on
	// Android.
	AndroidLink string `json:"androidLink,omitempty"`

	// AndroidMinPackageVersionCode: Minimum version code for the Android
	// app. If the installed app’s version
	// code is lower, then the user is taken to the Play Store.
	AndroidMinPackageVersionCode string `json:"androidMinPackageVersionCode,omitempty"`

	// AndroidPackageName: Android package name of the app.
	AndroidPackageName string `json:"androidPackageName,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AndroidFallbackLink")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AndroidFallbackLink") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *AndroidInfo) MarshalJSON() ([]byte, error) {
	type noMethod AndroidInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateShortDynamicLinkRequest: Request to create a short Dynamic
// Link.
type CreateShortDynamicLinkRequest struct {
	// DynamicLinkInfo: Information about the Dynamic Link to be
	// shortened.
	// [Learn
	// more](https://firebase.google.com/docs/dynamic-links/android#create-a-
	// dynamic-link-programmatically).
	DynamicLinkInfo *DynamicLinkInfo `json:"dynamicLinkInfo,omitempty"`

	// LongDynamicLink: Full long Dynamic Link URL with desired query
	// parameters specified.
	// For
	// example,
	// "https://sample.app.goo.gl/?link=http://www.google.com&apn=co
	// m.sample",
	// [Learn
	// more](https://firebase.google.com/docs/dynamic-links/android#create-a-
	// dynamic-link-programmatically).
	LongDynamicLink string `json:"longDynamicLink,omitempty"`

	// Suffix: Short Dynamic Link suffix. Optional.
	Suffix *Suffix `json:"suffix,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DynamicLinkInfo") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DynamicLinkInfo") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *CreateShortDynamicLinkRequest) MarshalJSON() ([]byte, error) {
	type noMethod CreateShortDynamicLinkRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// CreateShortDynamicLinkResponse: Response to create a short Dynamic
// Link.
type CreateShortDynamicLinkResponse struct {
	// PreviewLink: Preivew link to show the link flow chart.
	PreviewLink string `json:"previewLink,omitempty"`

	// ShortLink: Short Dynamic Link value. e.g.
	// https://abcd.app.goo.gl/wxyz
	ShortLink string `json:"shortLink,omitempty"`

	// Warning: Information about potential warnings on link creation.
	Warning []*DynamicLinkWarning `json:"warning,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "PreviewLink") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "PreviewLink") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateShortDynamicLinkResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateShortDynamicLinkResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DynamicLinkInfo: Information about a Dynamic Link.
type DynamicLinkInfo struct {
	// AnalyticsInfo: Parameters used for tracking. See all tracking
	// parameters in
	// the
	// [documentation](https://firebase.google.com/docs/dynamic-links/and
	// roid#create-a-dynamic-link-programmatically).
	AnalyticsInfo *AnalyticsInfo `json:"analyticsInfo,omitempty"`

	// AndroidInfo: Android related information. See Android related
	// parameters in
	// the
	// [documentation](https://firebase.google.com/docs/dynamic-links/and
	// roid#create-a-dynamic-link-programmatically).
	AndroidInfo *AndroidInfo `json:"androidInfo,omitempty"`

	// DynamicLinkDomain: Dynamic Links domain that the project owns, e.g.
	// abcd.app.goo.gl
	// [Learn
	// more](https://firebase.google.com/docs/dynamic-links/android#set-up-fi
	// rebase-and-the-dynamic-links-sdk)
	// on how to set up Dynamic Link domain associated with your Firebase
	// project.
	//
	// Required.
	DynamicLinkDomain string `json:"dynamicLinkDomain,omitempty"`

	// IosInfo: iOS related information. See iOS related parameters in
	// the
	// [documentation](https://firebase.google.com/docs/dynamic-links/ios
	// #create-a-dynamic-link-programmatically).
	IosInfo *IosInfo `json:"iosInfo,omitempty"`

	// Link: The link your app will open, You can specify any URL your app
	// can handle.
	// This link must be a well-formatted URL, be properly URL-encoded, and
	// use
	// the HTTP or HTTPS scheme. See 'link' parameters in
	// the
	// [documentation](https://firebase.google.com/docs/dynamic-links/and
	// roid#create-a-dynamic-link-programmatically).
	//
	// Required.
	Link string `json:"link,omitempty"`

	// SocialMetaTagInfo: Parameters for social meta tag params.
	// Used to set meta tag data for link previews on social sites.
	SocialMetaTagInfo *SocialMetaTagInfo `json:"socialMetaTagInfo,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AnalyticsInfo") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AnalyticsInfo") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DynamicLinkInfo) MarshalJSON() ([]byte, error) {
	type noMethod DynamicLinkInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DynamicLinkWarning: Dynamic Links warning messages.
type DynamicLinkWarning struct {
	// WarningCode: The warning code.
	//
	// Possible values:
	//   "CODE_UNSPECIFIED" - Unknown code.
	//   "NOT_IN_PROJECT_ANDROID_PACKAGE_NAME" - The Android package does
	// not match any in developer's DevConsole project.
	//   "NOT_INTEGER_ANDROID_PACKAGE_MIN_VERSION" - The Android minimum
	// version code has to be a valid integer.
	//   "UNNECESSARY_ANDROID_PACKAGE_MIN_VERSION" - Android package min
	// version param is not needed, e.g. when
	// 'apn' is missing.
	//   "NOT_URI_ANDROID_LINK" - Android link is not a valid URI.
	//   "UNNECESSARY_ANDROID_LINK" - Android link param is not needed, e.g.
	// when param 'al' and 'link' have
	// the same value..
	//   "NOT_URI_ANDROID_FALLBACK_LINK" - Android fallback link is not a
	// valid URI.
	//   "BAD_URI_SCHEME_ANDROID_FALLBACK_LINK" - Android fallback link has
	// an invalid (non http/https) URI scheme.
	//   "NOT_IN_PROJECT_IOS_BUNDLE_ID" - The iOS bundle ID does not match
	// any in developer's DevConsole project.
	//   "NOT_IN_PROJECT_IPAD_BUNDLE_ID" - The iPad bundle ID does not match
	// any in developer's DevConsole project.
	//   "UNNECESSARY_IOS_URL_SCHEME" - iOS URL scheme is not needed, e.g.
	// when 'ibi' are 'ipbi' are all missing.
	//   "NOT_NUMERIC_IOS_APP_STORE_ID" - iOS app store ID format is
	// incorrect, e.g. not numeric.
	//   "UNNECESSARY_IOS_APP_STORE_ID" - iOS app store ID is not needed.
	//   "NOT_URI_IOS_FALLBACK_LINK" - iOS fallback link is not a valid URI.
	//   "BAD_URI_SCHEME_IOS_FALLBACK_LINK" - iOS fallback link has an
	// invalid (non http/https) URI scheme.
	//   "NOT_URI_IPAD_FALLBACK_LINK" - iPad fallback link is not a valid
	// URI.
	//   "BAD_URI_SCHEME_IPAD_FALLBACK_LINK" - iPad fallback link has an
	// invalid (non http/https) URI scheme.
	//   "BAD_DEBUG_PARAM" - Debug param format is incorrect.
	//   "BAD_AD_PARAM" - isAd param format is incorrect.
	//   "DEPRECATED_PARAM" - Indicates a certain param is deprecated.
	//   "UNRECOGNIZED_PARAM" - Indicates certain paramater is not
	// recognized.
	//   "TOO_LONG_PARAM" - Indicates certain paramater is too long.
	//   "NOT_URI_SOCIAL_IMAGE_LINK" - Social meta tag image link is not a
	// valid URI.
	//   "BAD_URI_SCHEME_SOCIAL_IMAGE_LINK" - Social meta tag image link has
	// an invalid (non http/https) URI scheme.
	//   "NOT_URI_SOCIAL_URL"
	//   "BAD_URI_SCHEME_SOCIAL_URL"
	//   "LINK_LENGTH_TOO_LONG" - Dynamic Link URL length is too long.
	//   "LINK_WITH_FRAGMENTS" - Dynamic Link URL contains fragments.
	//   "NOT_MATCHING_IOS_BUNDLE_ID_AND_STORE_ID" - The iOS bundle ID does
	// not match with the given iOS store ID.
	WarningCode string `json:"warningCode,omitempty"`

	// WarningMessage: The warning message to help developers improve their
	// requests.
	WarningMessage string `json:"warningMessage,omitempty"`

	// ForceSendFields is a list of field names (e.g. "WarningCode") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "WarningCode") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DynamicLinkWarning) MarshalJSON() ([]byte, error) {
	type noMethod DynamicLinkWarning
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GooglePlayAnalytics: Parameters for Google Play Campaign
// Measurements.
// [Learn
// more](https://developers.google.com/analytics/devguides/collection/and
// roid/v4/campaigns#campaign-params)
type GooglePlayAnalytics struct {
	// Gclid: [AdWords autotagging
	// parameter](https://support.google.com/analytics/answer/1033981?hl=en);
	//
	// used to measure Google AdWords ads. This value is generated
	// dynamically
	// and should never be modified.
	Gclid string `json:"gclid,omitempty"`

	// UtmCampaign: Campaign name; used for keyword analysis to identify a
	// specific product
	// promotion or strategic campaign.
	UtmCampaign string `json:"utmCampaign,omitempty"`

	// UtmContent: Campaign content; used for A/B testing and
	// content-targeted ads to
	// differentiate ads or links that point to the same URL.
	UtmContent string `json:"utmContent,omitempty"`

	// UtmMedium: Campaign medium; used to identify a medium such as email
	// or cost-per-click.
	UtmMedium string `json:"utmMedium,omitempty"`

	// UtmSource: Campaign source; used to identify a search engine,
	// newsletter, or other
	// source.
	UtmSource string `json:"utmSource,omitempty"`

	// UtmTerm: Campaign term; used with paid search to supply the keywords
	// for ads.
	UtmTerm string `json:"utmTerm,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Gclid") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Gclid") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GooglePlayAnalytics) MarshalJSON() ([]byte, error) {
	type noMethod GooglePlayAnalytics
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ITunesConnectAnalytics: Parameters for iTunes Connect App Analytics.
type ITunesConnectAnalytics struct {
	// At: Affiliate token used to create affiliate-coded links.
	At string `json:"at,omitempty"`

	// Ct: Campaign text that developers can optionally add to any link in
	// order to
	// track sales from a specific marketing campaign.
	Ct string `json:"ct,omitempty"`

	// Mt: iTune media types, including music, podcasts, audiobooks and so
	// on.
	Mt string `json:"mt,omitempty"`

	// Pt: Provider token that enables analytics for Dynamic Links from
	// within iTunes
	// Connect.
	Pt string `json:"pt,omitempty"`

	// ForceSendFields is a list of field names (e.g. "At") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "At") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ITunesConnectAnalytics) MarshalJSON() ([]byte, error) {
	type noMethod ITunesConnectAnalytics
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IosInfo: iOS related attributes to the Dynamic Link..
type IosInfo struct {
	// IosAppStoreId: iOS App Store ID.
	IosAppStoreId string `json:"iosAppStoreId,omitempty"`

	// IosBundleId: iOS bundle ID of the app.
	IosBundleId string `json:"iosBundleId,omitempty"`

	// IosCustomScheme: Custom (destination) scheme to use for iOS. By
	// default, we’ll use the
	// bundle ID as the custom scheme. Developer can override this behavior
	// using
	// this param.
	IosCustomScheme string `json:"iosCustomScheme,omitempty"`

	// IosFallbackLink: Link to open on iOS if the app is not installed.
	IosFallbackLink string `json:"iosFallbackLink,omitempty"`

	// IosIpadBundleId: iPad bundle ID of the app.
	IosIpadBundleId string `json:"iosIpadBundleId,omitempty"`

	// IosIpadFallbackLink: If specified, this overrides the
	// ios_fallback_link value on iPads.
	IosIpadFallbackLink string `json:"iosIpadFallbackLink,omitempty"`

	// ForceSendFields is a list of field names (e.g. "IosAppStoreId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "IosAppStoreId") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IosInfo) MarshalJSON() ([]byte, error) {
	type noMethod IosInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SocialMetaTagInfo: Parameters for social meta tag params.
// Used to set meta tag data for link previews on social sites.
type SocialMetaTagInfo struct {
	// SocialDescription: A short description of the link. Optional.
	SocialDescription string `json:"socialDescription,omitempty"`

	// SocialImageLink: An image url string. Optional.
	SocialImageLink string `json:"socialImageLink,omitempty"`

	// SocialTitle: Title to be displayed. Optional.
	SocialTitle string `json:"socialTitle,omitempty"`

	// ForceSendFields is a list of field names (e.g. "SocialDescription")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "SocialDescription") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *SocialMetaTagInfo) MarshalJSON() ([]byte, error) {
	type noMethod SocialMetaTagInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Suffix: Short Dynamic Link suffix.
type Suffix struct {
	// Option: Suffix option.
	//
	// Possible values:
	//   "OPTION_UNSPECIFIED" - The suffix option is not specified, performs
	// as NOT_GUESSABLE .
	//   "UNGUESSABLE" - Short Dynamic Link suffix is a base62 [0-9A-Za-z]
	// encoded string of
	// a random generated 96 bit random number, which has a length of 17
	// chars.
	// For example, "nlAR8U4SlKRZw1cb2".
	// It prevents other people from guessing and crawling short Dynamic
	// Links
	// that contain personal identifiable information.
	//   "SHORT" - Short Dynamic Link suffix is a base62 [0-9A-Za-z] string
	// starting with a
	// length of 4 chars. the length will increase when all the space
	// is
	// occupied.
	Option string `json:"option,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Option") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Option") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Suffix) MarshalJSON() ([]byte, error) {
	type noMethod Suffix
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "firebasedynamiclinks.shortLinks.create":

type ShortLinksCreateCall struct {
	s                             *Service
	createshortdynamiclinkrequest *CreateShortDynamicLinkRequest
	urlParams_                    gensupport.URLParams
	ctx_                          context.Context
	header_                       http.Header
}

// Create: Creates a short Dynamic Link given either a valid long
// Dynamic Link or
// details such as Dynamic Link domain, Android and iOS app
// information.
// The created short Dynamic Link will not expire.
//
// Repeated calls with the same long Dynamic Link or Dynamic Link
// information
// will produce the same short Dynamic Link.
//
// The Dynamic Link domain in the request must be owned by
// requester's
// Firebase project.
func (r *ShortLinksService) Create(createshortdynamiclinkrequest *CreateShortDynamicLinkRequest) *ShortLinksCreateCall {
	c := &ShortLinksCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.createshortdynamiclinkrequest = createshortdynamiclinkrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ShortLinksCreateCall) Fields(s ...googleapi.Field) *ShortLinksCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ShortLinksCreateCall) Context(ctx context.Context) *ShortLinksCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ShortLinksCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ShortLinksCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.createshortdynamiclinkrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1/shortLinks")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "firebasedynamiclinks.shortLinks.create" call.
// Exactly one of *CreateShortDynamicLinkResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *CreateShortDynamicLinkResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ShortLinksCreateCall) Do(opts ...googleapi.CallOption) (*CreateShortDynamicLinkResponse, error) {
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
	ret := &CreateShortDynamicLinkResponse{
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
	//   "description": "Creates a short Dynamic Link given either a valid long Dynamic Link or\ndetails such as Dynamic Link domain, Android and iOS app information.\nThe created short Dynamic Link will not expire.\n\nRepeated calls with the same long Dynamic Link or Dynamic Link information\nwill produce the same short Dynamic Link.\n\nThe Dynamic Link domain in the request must be owned by requester's\nFirebase project.",
	//   "flatPath": "v1/shortLinks",
	//   "httpMethod": "POST",
	//   "id": "firebasedynamiclinks.shortLinks.create",
	//   "parameterOrder": [],
	//   "parameters": {},
	//   "path": "v1/shortLinks",
	//   "request": {
	//     "$ref": "CreateShortDynamicLinkRequest"
	//   },
	//   "response": {
	//     "$ref": "CreateShortDynamicLinkResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/firebase"
	//   ]
	// }

}
