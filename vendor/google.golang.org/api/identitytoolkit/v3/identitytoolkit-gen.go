// Package identitytoolkit provides access to the Google Identity Toolkit API.
//
// See https://developers.google.com/identity-toolkit/v3/
//
// Usage example:
//
//   import "google.golang.org/api/identitytoolkit/v3"
//   ...
//   identitytoolkitService, err := identitytoolkit.New(oauthHttpClient)
package identitytoolkit // import "google.golang.org/api/identitytoolkit/v3"

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

const apiId = "identitytoolkit:v3"
const apiName = "identitytoolkit"
const apiVersion = "v3"
const basePath = "https://www.googleapis.com/identitytoolkit/v3/relyingparty/"

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
	s.Relyingparty = NewRelyingpartyService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Relyingparty *RelyingpartyService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewRelyingpartyService(s *Service) *RelyingpartyService {
	rs := &RelyingpartyService{s: s}
	return rs
}

type RelyingpartyService struct {
	s *Service
}

// CreateAuthUriResponse: Response of creating the IDP authentication
// URL.
type CreateAuthUriResponse struct {
	// AllProviders: all providers the user has once used to do federated
	// login
	AllProviders []string `json:"allProviders,omitempty"`

	// AuthUri: The URI used by the IDP to authenticate the user.
	AuthUri string `json:"authUri,omitempty"`

	// CaptchaRequired: True if captcha is required.
	CaptchaRequired bool `json:"captchaRequired,omitempty"`

	// ForExistingProvider: True if the authUri is for user's existing
	// provider.
	ForExistingProvider bool `json:"forExistingProvider,omitempty"`

	// Kind: The fixed string identitytoolkit#CreateAuthUriResponse".
	Kind string `json:"kind,omitempty"`

	// ProviderId: The provider ID of the auth URI.
	ProviderId string `json:"providerId,omitempty"`

	// Registered: Whether the user is registered if the identifier is an
	// email.
	Registered bool `json:"registered,omitempty"`

	// SessionId: Session ID which should be passed in the following
	// verifyAssertion request.
	SessionId string `json:"sessionId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AllProviders") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllProviders") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *CreateAuthUriResponse) MarshalJSON() ([]byte, error) {
	type noMethod CreateAuthUriResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeleteAccountResponse: Respone of deleting account.
type DeleteAccountResponse struct {
	// Kind: The fixed string "identitytoolkit#DeleteAccountResponse".
	Kind string `json:"kind,omitempty"`

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

func (s *DeleteAccountResponse) MarshalJSON() ([]byte, error) {
	type noMethod DeleteAccountResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DownloadAccountResponse: Respone of downloading accounts in batch.
type DownloadAccountResponse struct {
	// Kind: The fixed string "identitytoolkit#DownloadAccountResponse".
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The next page token. To be used in a subsequent
	// request to return the next page of results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Users: The user accounts data.
	Users []*UserInfo `json:"users,omitempty"`

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

func (s *DownloadAccountResponse) MarshalJSON() ([]byte, error) {
	type noMethod DownloadAccountResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// EmailTemplate: Template for an email template.
type EmailTemplate struct {
	// Body: Email body.
	Body string `json:"body,omitempty"`

	// Format: Email body format.
	Format string `json:"format,omitempty"`

	// From: From address of the email.
	From string `json:"from,omitempty"`

	// FromDisplayName: From display name.
	FromDisplayName string `json:"fromDisplayName,omitempty"`

	// ReplyTo: Reply-to address.
	ReplyTo string `json:"replyTo,omitempty"`

	// Subject: Subject of the email.
	Subject string `json:"subject,omitempty"`

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

func (s *EmailTemplate) MarshalJSON() ([]byte, error) {
	type noMethod EmailTemplate
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GetAccountInfoResponse: Response of getting account information.
type GetAccountInfoResponse struct {
	// Kind: The fixed string "identitytoolkit#GetAccountInfoResponse".
	Kind string `json:"kind,omitempty"`

	// Users: The info of the users.
	Users []*UserInfo `json:"users,omitempty"`

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

func (s *GetAccountInfoResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetAccountInfoResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GetOobConfirmationCodeResponse: Response of getting a code for user
// confirmation (reset password, change email etc.).
type GetOobConfirmationCodeResponse struct {
	// Email: The email address that the email is sent to.
	Email string `json:"email,omitempty"`

	// Kind: The fixed string
	// "identitytoolkit#GetOobConfirmationCodeResponse".
	Kind string `json:"kind,omitempty"`

	// OobCode: The code to be send to the user.
	OobCode string `json:"oobCode,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Email") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Email") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *GetOobConfirmationCodeResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetOobConfirmationCodeResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// GetRecaptchaParamResponse: Response of getting recaptcha param.
type GetRecaptchaParamResponse struct {
	// Kind: The fixed string "identitytoolkit#GetRecaptchaParamResponse".
	Kind string `json:"kind,omitempty"`

	// RecaptchaSiteKey: Site key registered at recaptcha.
	RecaptchaSiteKey string `json:"recaptchaSiteKey,omitempty"`

	// RecaptchaStoken: The stoken field for the recaptcha widget, used to
	// request captcha challenge.
	RecaptchaStoken string `json:"recaptchaStoken,omitempty"`

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

func (s *GetRecaptchaParamResponse) MarshalJSON() ([]byte, error) {
	type noMethod GetRecaptchaParamResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyCreateAuthUriRequest: Request to get the
// IDP authentication URL.
type IdentitytoolkitRelyingpartyCreateAuthUriRequest struct {
	// AppId: The app ID of the mobile app, base64(CERT_SHA1):PACKAGE_NAME
	// for Android, BUNDLE_ID for iOS.
	AppId string `json:"appId,omitempty"`

	// AuthFlowType: Explicitly specify the auth flow type. Currently only
	// support "CODE_FLOW" type. The field is only used for Google provider.
	AuthFlowType string `json:"authFlowType,omitempty"`

	// ClientId: The relying party OAuth client ID.
	ClientId string `json:"clientId,omitempty"`

	// Context: The opaque value used by the client to maintain context info
	// between the authentication request and the IDP callback.
	Context string `json:"context,omitempty"`

	// ContinueUri: The URI to which the IDP redirects the user after the
	// federated login flow.
	ContinueUri string `json:"continueUri,omitempty"`

	// CustomParameter: The query parameter that client can customize by
	// themselves in auth url. The following parameters are reserved for
	// server so that they cannot be customized by clients: client_id,
	// response_type, scope, redirect_uri, state, oauth_token.
	CustomParameter map[string]string `json:"customParameter,omitempty"`

	// HostedDomain: The hosted domain to restrict sign-in to accounts at
	// that domain for Google Apps hosted accounts.
	HostedDomain string `json:"hostedDomain,omitempty"`

	// Identifier: The email or federated ID of the user.
	Identifier string `json:"identifier,omitempty"`

	// OauthConsumerKey: The developer's consumer key for OpenId OAuth
	// Extension
	OauthConsumerKey string `json:"oauthConsumerKey,omitempty"`

	// OauthScope: Additional oauth scopes, beyond the basid user profile,
	// that the user would be prompted to grant
	OauthScope string `json:"oauthScope,omitempty"`

	// OpenidRealm: Optional realm for OpenID protocol. The sub string
	// "scheme://domain:port" of the param "continueUri" is used if this is
	// not set.
	OpenidRealm string `json:"openidRealm,omitempty"`

	// OtaApp: The native app package for OTA installation.
	OtaApp string `json:"otaApp,omitempty"`

	// ProviderId: The IdP ID. For white listed IdPs it's a short domain
	// name e.g. google.com, aol.com, live.net and yahoo.com. For other
	// OpenID IdPs it's the OP identifier.
	ProviderId string `json:"providerId,omitempty"`

	// SessionId: The session_id passed by client.
	SessionId string `json:"sessionId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AppId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AppId") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyCreateAuthUriRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyCreateAuthUriRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyDeleteAccountRequest: Request to delete
// account.
type IdentitytoolkitRelyingpartyDeleteAccountRequest struct {
	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// IdToken: The GITKit token or STS id token of the authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// LocalId: The local ID of the user.
	LocalId string `json:"localId,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "DelegatedProjectNumber") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DelegatedProjectNumber")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyDeleteAccountRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyDeleteAccountRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyDownloadAccountRequest: Request to
// download user account in batch.
type IdentitytoolkitRelyingpartyDownloadAccountRequest struct {
	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// MaxResults: The max number of results to return in the response.
	MaxResults int64 `json:"maxResults,omitempty"`

	// NextPageToken: The token for the next page. This should be taken from
	// the previous response.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "DelegatedProjectNumber") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DelegatedProjectNumber")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyDownloadAccountRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyDownloadAccountRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyGetAccountInfoRequest: Request to get the
// account information.
type IdentitytoolkitRelyingpartyGetAccountInfoRequest struct {
	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// Email: The list of emails of the users to inquiry.
	Email []string `json:"email,omitempty"`

	// IdToken: The GITKit token of the authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// LocalId: The list of local ID's of the users to inquiry.
	LocalId []string `json:"localId,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "DelegatedProjectNumber") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DelegatedProjectNumber")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyGetAccountInfoRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyGetAccountInfoRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyGetProjectConfigResponse: Response of
// getting the project configuration.
type IdentitytoolkitRelyingpartyGetProjectConfigResponse struct {
	// AllowPasswordUser: Whether to allow password user sign in or sign up.
	AllowPasswordUser bool `json:"allowPasswordUser,omitempty"`

	// ApiKey: Browser API key, needed when making http request to Apiary.
	ApiKey string `json:"apiKey,omitempty"`

	// AuthorizedDomains: Authorized domains.
	AuthorizedDomains []string `json:"authorizedDomains,omitempty"`

	// ChangeEmailTemplate: Change email template.
	ChangeEmailTemplate *EmailTemplate `json:"changeEmailTemplate,omitempty"`

	DynamicLinksDomain string `json:"dynamicLinksDomain,omitempty"`

	// EnableAnonymousUser: Whether anonymous user is enabled.
	EnableAnonymousUser bool `json:"enableAnonymousUser,omitempty"`

	// IdpConfig: OAuth2 provider configuration.
	IdpConfig []*IdpConfig `json:"idpConfig,omitempty"`

	// LegacyResetPasswordTemplate: Legacy reset password email template.
	LegacyResetPasswordTemplate *EmailTemplate `json:"legacyResetPasswordTemplate,omitempty"`

	// ProjectId: Project ID of the relying party.
	ProjectId string `json:"projectId,omitempty"`

	// ResetPasswordTemplate: Reset password email template.
	ResetPasswordTemplate *EmailTemplate `json:"resetPasswordTemplate,omitempty"`

	// UseEmailSending: Whether to use email sending provided by Firebear.
	UseEmailSending bool `json:"useEmailSending,omitempty"`

	// VerifyEmailTemplate: Verify email template.
	VerifyEmailTemplate *EmailTemplate `json:"verifyEmailTemplate,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "AllowPasswordUser")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllowPasswordUser") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyGetProjectConfigResponse) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyGetProjectConfigResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyResetPasswordRequest: Request to reset the
// password.
type IdentitytoolkitRelyingpartyResetPasswordRequest struct {
	// Email: The email address of the user.
	Email string `json:"email,omitempty"`

	// NewPassword: The new password inputted by the user.
	NewPassword string `json:"newPassword,omitempty"`

	// OldPassword: The old password inputted by the user.
	OldPassword string `json:"oldPassword,omitempty"`

	// OobCode: The confirmation code.
	OobCode string `json:"oobCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Email") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Email") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyResetPasswordRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyResetPasswordRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartySetAccountInfoRequest: Request to set the
// account information.
type IdentitytoolkitRelyingpartySetAccountInfoRequest struct {
	// CaptchaChallenge: The captcha challenge.
	CaptchaChallenge string `json:"captchaChallenge,omitempty"`

	// CaptchaResponse: Response to the captcha.
	CaptchaResponse string `json:"captchaResponse,omitempty"`

	// CreatedAt: The timestamp when the account is created.
	CreatedAt int64 `json:"createdAt,omitempty,string"`

	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// DeleteAttribute: The attributes users request to delete.
	DeleteAttribute []string `json:"deleteAttribute,omitempty"`

	// DeleteProvider: The IDPs the user request to delete.
	DeleteProvider []string `json:"deleteProvider,omitempty"`

	// DisableUser: Whether to disable the user.
	DisableUser bool `json:"disableUser,omitempty"`

	// DisplayName: The name of the user.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The email of the user.
	Email string `json:"email,omitempty"`

	// EmailVerified: Mark the email as verified or not.
	EmailVerified bool `json:"emailVerified,omitempty"`

	// IdToken: The GITKit token of the authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// InstanceId: Instance id token of the app.
	InstanceId string `json:"instanceId,omitempty"`

	// LastLoginAt: Last login timestamp.
	LastLoginAt int64 `json:"lastLoginAt,omitempty,string"`

	// LocalId: The local ID of the user.
	LocalId string `json:"localId,omitempty"`

	// OobCode: The out-of-band code of the change email request.
	OobCode string `json:"oobCode,omitempty"`

	// Password: The new password of the user.
	Password string `json:"password,omitempty"`

	// PhotoUrl: The photo url of the user.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// Provider: The associated IDPs of the user.
	Provider []string `json:"provider,omitempty"`

	// ReturnSecureToken: Whether return sts id token and refresh token
	// instead of gitkit token.
	ReturnSecureToken bool `json:"returnSecureToken,omitempty"`

	// UpgradeToFederatedLogin: Mark the user to upgrade to federated login.
	UpgradeToFederatedLogin bool `json:"upgradeToFederatedLogin,omitempty"`

	// ValidSince: Timestamp in seconds for valid login token.
	ValidSince int64 `json:"validSince,omitempty,string"`

	// ForceSendFields is a list of field names (e.g. "CaptchaChallenge") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CaptchaChallenge") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartySetAccountInfoRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartySetAccountInfoRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartySetProjectConfigRequest: Request to set
// the project configuration.
type IdentitytoolkitRelyingpartySetProjectConfigRequest struct {
	// AllowPasswordUser: Whether to allow password user sign in or sign up.
	AllowPasswordUser bool `json:"allowPasswordUser,omitempty"`

	// ApiKey: Browser API key, needed when making http request to Apiary.
	ApiKey string `json:"apiKey,omitempty"`

	// AuthorizedDomains: Authorized domains for widget redirect.
	AuthorizedDomains []string `json:"authorizedDomains,omitempty"`

	// ChangeEmailTemplate: Change email template.
	ChangeEmailTemplate *EmailTemplate `json:"changeEmailTemplate,omitempty"`

	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// EnableAnonymousUser: Whether to enable anonymous user.
	EnableAnonymousUser bool `json:"enableAnonymousUser,omitempty"`

	// IdpConfig: Oauth2 provider configuration.
	IdpConfig []*IdpConfig `json:"idpConfig,omitempty"`

	// LegacyResetPasswordTemplate: Legacy reset password email template.
	LegacyResetPasswordTemplate *EmailTemplate `json:"legacyResetPasswordTemplate,omitempty"`

	// ResetPasswordTemplate: Reset password email template.
	ResetPasswordTemplate *EmailTemplate `json:"resetPasswordTemplate,omitempty"`

	// UseEmailSending: Whether to use email sending provided by Firebear.
	UseEmailSending bool `json:"useEmailSending,omitempty"`

	// VerifyEmailTemplate: Verify email template.
	VerifyEmailTemplate *EmailTemplate `json:"verifyEmailTemplate,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AllowPasswordUser")
	// to unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllowPasswordUser") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartySetProjectConfigRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartySetProjectConfigRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartySetProjectConfigResponse: Response of
// setting the project configuration.
type IdentitytoolkitRelyingpartySetProjectConfigResponse struct {
	// ProjectId: Project ID of the relying party.
	ProjectId string `json:"projectId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ProjectId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ProjectId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartySetProjectConfigResponse) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartySetProjectConfigResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartySignOutUserRequest: Request to sign out
// user.
type IdentitytoolkitRelyingpartySignOutUserRequest struct {
	// InstanceId: Instance id token of the app.
	InstanceId string `json:"instanceId,omitempty"`

	// LocalId: The local ID of the user.
	LocalId string `json:"localId,omitempty"`

	// ForceSendFields is a list of field names (e.g. "InstanceId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "InstanceId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartySignOutUserRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartySignOutUserRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartySignOutUserResponse: Response of signing
// out user.
type IdentitytoolkitRelyingpartySignOutUserResponse struct {
	// LocalId: The local ID of the user.
	LocalId string `json:"localId,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "LocalId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LocalId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartySignOutUserResponse) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartySignOutUserResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartySignupNewUserRequest: Request to signup
// new user, create anonymous user or anonymous user reauth.
type IdentitytoolkitRelyingpartySignupNewUserRequest struct {
	// CaptchaChallenge: The captcha challenge.
	CaptchaChallenge string `json:"captchaChallenge,omitempty"`

	// CaptchaResponse: Response to the captcha.
	CaptchaResponse string `json:"captchaResponse,omitempty"`

	// Disabled: Whether to disable the user. Only can be used by service
	// account.
	Disabled bool `json:"disabled,omitempty"`

	// DisplayName: The name of the user.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The email of the user.
	Email string `json:"email,omitempty"`

	// EmailVerified: Mark the email as verified or not. Only can be used by
	// service account.
	EmailVerified bool `json:"emailVerified,omitempty"`

	// IdToken: The GITKit token of the authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// InstanceId: Instance id token of the app.
	InstanceId string `json:"instanceId,omitempty"`

	// Password: The new password of the user.
	Password string `json:"password,omitempty"`

	// PhotoUrl: The photo url of the user.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CaptchaChallenge") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CaptchaChallenge") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartySignupNewUserRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartySignupNewUserRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyUploadAccountRequest: Request to upload
// user account in batch.
type IdentitytoolkitRelyingpartyUploadAccountRequest struct {
	// AllowOverwrite: Whether allow overwrite existing account when user
	// local_id exists.
	AllowOverwrite bool `json:"allowOverwrite,omitempty"`

	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// HashAlgorithm: The password hash algorithm.
	HashAlgorithm string `json:"hashAlgorithm,omitempty"`

	// MemoryCost: Memory cost for hash calculation. Used by scrypt similar
	// algorithms.
	MemoryCost int64 `json:"memoryCost,omitempty"`

	// Rounds: Rounds for hash calculation. Used by scrypt and similar
	// algorithms.
	Rounds int64 `json:"rounds,omitempty"`

	// SaltSeparator: The salt separator.
	SaltSeparator string `json:"saltSeparator,omitempty"`

	// SanityCheck: If true, backend will do sanity check(including
	// duplicate email and federated id) when uploading account.
	SanityCheck bool `json:"sanityCheck,omitempty"`

	// SignerKey: The key for to hash the password.
	SignerKey string `json:"signerKey,omitempty"`

	// TargetProjectId: Specify which project (field value is actually
	// project id) to operate. Only used when provided credential.
	TargetProjectId string `json:"targetProjectId,omitempty"`

	// Users: The account info to be stored.
	Users []*UserInfo `json:"users,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AllowOverwrite") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AllowOverwrite") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyUploadAccountRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyUploadAccountRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyVerifyAssertionRequest: Request to verify
// the IDP assertion.
type IdentitytoolkitRelyingpartyVerifyAssertionRequest struct {
	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// IdToken: The GITKit token of the authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// InstanceId: Instance id token of the app.
	InstanceId string `json:"instanceId,omitempty"`

	// PendingIdToken: The GITKit token for the non-trusted IDP pending to
	// be confirmed by the user.
	PendingIdToken string `json:"pendingIdToken,omitempty"`

	// PostBody: The post body if the request is a HTTP POST.
	PostBody string `json:"postBody,omitempty"`

	// RequestUri: The URI to which the IDP redirects the user back. It may
	// contain federated login result params added by the IDP.
	RequestUri string `json:"requestUri,omitempty"`

	// ReturnIdpCredential: Whether return 200 and IDP credential rather
	// than throw exception when federated id is already linked.
	ReturnIdpCredential bool `json:"returnIdpCredential,omitempty"`

	// ReturnRefreshToken: Whether to return refresh tokens.
	ReturnRefreshToken bool `json:"returnRefreshToken,omitempty"`

	// ReturnSecureToken: Whether return sts id token and refresh token
	// instead of gitkit token.
	ReturnSecureToken bool `json:"returnSecureToken,omitempty"`

	// SessionId: Session ID, which should match the one in previous
	// createAuthUri request.
	SessionId string `json:"sessionId,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "DelegatedProjectNumber") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DelegatedProjectNumber")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyVerifyAssertionRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyVerifyAssertionRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyVerifyCustomTokenRequest: Request to
// verify a custom token
type IdentitytoolkitRelyingpartyVerifyCustomTokenRequest struct {
	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// InstanceId: Instance id token of the app.
	InstanceId string `json:"instanceId,omitempty"`

	// ReturnSecureToken: Whether return sts id token and refresh token
	// instead of gitkit token.
	ReturnSecureToken bool `json:"returnSecureToken,omitempty"`

	// Token: The custom token to verify
	Token string `json:"token,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "DelegatedProjectNumber") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DelegatedProjectNumber")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyVerifyCustomTokenRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyVerifyCustomTokenRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdentitytoolkitRelyingpartyVerifyPasswordRequest: Request to verify
// the password.
type IdentitytoolkitRelyingpartyVerifyPasswordRequest struct {
	// CaptchaChallenge: The captcha challenge.
	CaptchaChallenge string `json:"captchaChallenge,omitempty"`

	// CaptchaResponse: Response to the captcha.
	CaptchaResponse string `json:"captchaResponse,omitempty"`

	// DelegatedProjectNumber: GCP project number of the requesting
	// delegated app. Currently only intended for Firebase V1 migration.
	DelegatedProjectNumber int64 `json:"delegatedProjectNumber,omitempty,string"`

	// Email: The email of the user.
	Email string `json:"email,omitempty"`

	// IdToken: The GITKit token of the authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// InstanceId: Instance id token of the app.
	InstanceId string `json:"instanceId,omitempty"`

	// Password: The password inputed by the user.
	Password string `json:"password,omitempty"`

	// PendingIdToken: The GITKit token for the non-trusted IDP, which is to
	// be confirmed by the user.
	PendingIdToken string `json:"pendingIdToken,omitempty"`

	// ReturnSecureToken: Whether return sts id token and refresh token
	// instead of gitkit token.
	ReturnSecureToken bool `json:"returnSecureToken,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CaptchaChallenge") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CaptchaChallenge") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *IdentitytoolkitRelyingpartyVerifyPasswordRequest) MarshalJSON() ([]byte, error) {
	type noMethod IdentitytoolkitRelyingpartyVerifyPasswordRequest
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// IdpConfig: Template for a single idp configuration.
type IdpConfig struct {
	// ClientId: OAuth2 client ID.
	ClientId string `json:"clientId,omitempty"`

	// Enabled: Whether this IDP is enabled.
	Enabled bool `json:"enabled,omitempty"`

	// ExperimentPercent: Percent of users who will be prompted/redirected
	// federated login for this IDP.
	ExperimentPercent int64 `json:"experimentPercent,omitempty"`

	// Provider: OAuth2 provider.
	Provider string `json:"provider,omitempty"`

	// Secret: OAuth2 client secret.
	Secret string `json:"secret,omitempty"`

	// WhitelistedAudiences: Whitelisted client IDs for audience check.
	WhitelistedAudiences []string `json:"whitelistedAudiences,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ClientId") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ClientId") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *IdpConfig) MarshalJSON() ([]byte, error) {
	type noMethod IdpConfig
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Relyingparty: Request of getting a code for user confirmation (reset
// password, change email etc.)
type Relyingparty struct {
	// CaptchaResp: The recaptcha response from the user.
	CaptchaResp string `json:"captchaResp,omitempty"`

	// Challenge: The recaptcha challenge presented to the user.
	Challenge string `json:"challenge,omitempty"`

	// Email: The email of the user.
	Email string `json:"email,omitempty"`

	// IdToken: The user's Gitkit login token for email change.
	IdToken string `json:"idToken,omitempty"`

	// Kind: The fixed string "identitytoolkit#relyingparty".
	Kind string `json:"kind,omitempty"`

	// NewEmail: The new email if the code is for email change.
	NewEmail string `json:"newEmail,omitempty"`

	// RequestType: The request type.
	RequestType string `json:"requestType,omitempty"`

	// UserIp: The IP address of the user.
	UserIp string `json:"userIp,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CaptchaResp") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CaptchaResp") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Relyingparty) MarshalJSON() ([]byte, error) {
	type noMethod Relyingparty
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ResetPasswordResponse: Response of resetting the password.
type ResetPasswordResponse struct {
	// Email: The user's email. If the out-of-band code is for email
	// recovery, the user's original email.
	Email string `json:"email,omitempty"`

	// Kind: The fixed string "identitytoolkit#ResetPasswordResponse".
	Kind string `json:"kind,omitempty"`

	// NewEmail: If the out-of-band code is for email recovery, the user's
	// new email.
	NewEmail string `json:"newEmail,omitempty"`

	// RequestType: The request type.
	RequestType string `json:"requestType,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Email") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Email") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ResetPasswordResponse) MarshalJSON() ([]byte, error) {
	type noMethod ResetPasswordResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SetAccountInfoResponse: Respone of setting the account information.
type SetAccountInfoResponse struct {
	// DisplayName: The name of the user.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The email of the user.
	Email string `json:"email,omitempty"`

	// ExpiresIn: If idToken is STS id token, then this field will be
	// expiration time of STS id token in seconds.
	ExpiresIn int64 `json:"expiresIn,omitempty,string"`

	// IdToken: The Gitkit id token to login the newly sign up user.
	IdToken string `json:"idToken,omitempty"`

	// Kind: The fixed string "identitytoolkit#SetAccountInfoResponse".
	Kind string `json:"kind,omitempty"`

	// LocalId: The local ID of the user.
	LocalId string `json:"localId,omitempty"`

	// NewEmail: The new email the user attempts to change to.
	NewEmail string `json:"newEmail,omitempty"`

	// PasswordHash: The user's hashed password.
	PasswordHash string `json:"passwordHash,omitempty"`

	// PhotoUrl: The photo url of the user.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// ProviderUserInfo: The user's profiles at the associated IdPs.
	ProviderUserInfo []*SetAccountInfoResponseProviderUserInfo `json:"providerUserInfo,omitempty"`

	// RefreshToken: If idToken is STS id token, then this field will be
	// refresh token.
	RefreshToken string `json:"refreshToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *SetAccountInfoResponse) MarshalJSON() ([]byte, error) {
	type noMethod SetAccountInfoResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type SetAccountInfoResponseProviderUserInfo struct {
	// DisplayName: The user's display name at the IDP.
	DisplayName string `json:"displayName,omitempty"`

	// FederatedId: User's identifier at IDP.
	FederatedId string `json:"federatedId,omitempty"`

	// PhotoUrl: The user's photo url at the IDP.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// ProviderId: The IdP ID. For whitelisted IdPs it's a short domain
	// name, e.g., google.com, aol.com, live.net and yahoo.com. For other
	// OpenID IdPs it's the OP identifier.
	ProviderId string `json:"providerId,omitempty"`

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

func (s *SetAccountInfoResponseProviderUserInfo) MarshalJSON() ([]byte, error) {
	type noMethod SetAccountInfoResponseProviderUserInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SignupNewUserResponse: Response of signing up new user, creating
// anonymous user or anonymous user reauth.
type SignupNewUserResponse struct {
	// DisplayName: The name of the user.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The email of the user.
	Email string `json:"email,omitempty"`

	// ExpiresIn: If idToken is STS id token, then this field will be
	// expiration time of STS id token in seconds.
	ExpiresIn int64 `json:"expiresIn,omitempty,string"`

	// IdToken: The Gitkit id token to login the newly sign up user.
	IdToken string `json:"idToken,omitempty"`

	// Kind: The fixed string "identitytoolkit#SignupNewUserResponse".
	Kind string `json:"kind,omitempty"`

	// LocalId: The RP local ID of the user.
	LocalId string `json:"localId,omitempty"`

	// RefreshToken: If idToken is STS id token, then this field will be
	// refresh token.
	RefreshToken string `json:"refreshToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *SignupNewUserResponse) MarshalJSON() ([]byte, error) {
	type noMethod SignupNewUserResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UploadAccountResponse: Respone of uploading accounts in batch.
type UploadAccountResponse struct {
	// Error: The error encountered while processing the account info.
	Error []*UploadAccountResponseError `json:"error,omitempty"`

	// Kind: The fixed string "identitytoolkit#UploadAccountResponse".
	Kind string `json:"kind,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Error") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Error") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UploadAccountResponse) MarshalJSON() ([]byte, error) {
	type noMethod UploadAccountResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type UploadAccountResponseError struct {
	// Index: The index of the malformed account, starting from 0.
	Index int64 `json:"index,omitempty"`

	// Message: Detailed error message for the account info.
	Message string `json:"message,omitempty"`

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

func (s *UploadAccountResponseError) MarshalJSON() ([]byte, error) {
	type noMethod UploadAccountResponseError
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// UserInfo: Template for an individual account info.
type UserInfo struct {
	// CreatedAt: User creation timestamp.
	CreatedAt int64 `json:"createdAt,omitempty,string"`

	// CustomAuth: Whether the user is authenticated by the developer.
	CustomAuth bool `json:"customAuth,omitempty"`

	// Disabled: Whether the user is disabled.
	Disabled bool `json:"disabled,omitempty"`

	// DisplayName: The name of the user.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The email of the user.
	Email string `json:"email,omitempty"`

	// EmailVerified: Whether the email has been verified.
	EmailVerified bool `json:"emailVerified,omitempty"`

	// LastLoginAt: last login timestamp.
	LastLoginAt int64 `json:"lastLoginAt,omitempty,string"`

	// LocalId: The local ID of the user.
	LocalId string `json:"localId,omitempty"`

	// PasswordHash: The user's hashed password.
	PasswordHash string `json:"passwordHash,omitempty"`

	// PasswordUpdatedAt: The timestamp when the password was last updated.
	PasswordUpdatedAt float64 `json:"passwordUpdatedAt,omitempty"`

	// PhotoUrl: The URL of the user profile photo.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// ProviderUserInfo: The IDP of the user.
	ProviderUserInfo []*UserInfoProviderUserInfo `json:"providerUserInfo,omitempty"`

	// RawPassword: The user's plain text password.
	RawPassword string `json:"rawPassword,omitempty"`

	// Salt: The user's password salt.
	Salt string `json:"salt,omitempty"`

	// ScreenName: User's screen name at Twitter or login name at Github.
	ScreenName string `json:"screenName,omitempty"`

	// ValidSince: Timestamp in seconds for valid login token.
	ValidSince int64 `json:"validSince,omitempty,string"`

	// Version: Version of the user's password.
	Version int64 `json:"version,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CreatedAt") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreatedAt") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *UserInfo) MarshalJSON() ([]byte, error) {
	type noMethod UserInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type UserInfoProviderUserInfo struct {
	// DisplayName: The user's display name at the IDP.
	DisplayName string `json:"displayName,omitempty"`

	// Email: User's email at IDP.
	Email string `json:"email,omitempty"`

	// FederatedId: User's identifier at IDP.
	FederatedId string `json:"federatedId,omitempty"`

	// PhotoUrl: The user's photo url at the IDP.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// ProviderId: The IdP ID. For white listed IdPs it's a short domain
	// name, e.g., google.com, aol.com, live.net and yahoo.com. For other
	// OpenID IdPs it's the OP identifier.
	ProviderId string `json:"providerId,omitempty"`

	// RawId: User's raw identifier directly returned from IDP.
	RawId string `json:"rawId,omitempty"`

	// ScreenName: User's screen name at Twitter or login name at Github.
	ScreenName string `json:"screenName,omitempty"`

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

func (s *UserInfoProviderUserInfo) MarshalJSON() ([]byte, error) {
	type noMethod UserInfoProviderUserInfo
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VerifyAssertionResponse: Response of verifying the IDP assertion.
type VerifyAssertionResponse struct {
	// Action: The action code.
	Action string `json:"action,omitempty"`

	// AppInstallationUrl: URL for OTA app installation.
	AppInstallationUrl string `json:"appInstallationUrl,omitempty"`

	// AppScheme: The custom scheme used by mobile app.
	AppScheme string `json:"appScheme,omitempty"`

	// Context: The opaque value used by the client to maintain context info
	// between the authentication request and the IDP callback.
	Context string `json:"context,omitempty"`

	// DateOfBirth: The birth date of the IdP account.
	DateOfBirth string `json:"dateOfBirth,omitempty"`

	// DisplayName: The display name of the user.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The email returned by the IdP. NOTE: The federated login user
	// may not own the email.
	Email string `json:"email,omitempty"`

	// EmailRecycled: It's true if the email is recycled.
	EmailRecycled bool `json:"emailRecycled,omitempty"`

	// EmailVerified: The value is true if the IDP is also the email
	// provider. It means the user owns the email.
	EmailVerified bool `json:"emailVerified,omitempty"`

	// ErrorMessage: Client error code.
	ErrorMessage string `json:"errorMessage,omitempty"`

	// ExpiresIn: If idToken is STS id token, then this field will be
	// expiration time of STS id token in seconds.
	ExpiresIn int64 `json:"expiresIn,omitempty,string"`

	// FederatedId: The unique ID identifies the IdP account.
	FederatedId string `json:"federatedId,omitempty"`

	// FirstName: The first name of the user.
	FirstName string `json:"firstName,omitempty"`

	// FullName: The full name of the user.
	FullName string `json:"fullName,omitempty"`

	// IdToken: The ID token.
	IdToken string `json:"idToken,omitempty"`

	// InputEmail: It's the identifier param in the createAuthUri request if
	// the identifier is an email. It can be used to check whether the user
	// input email is different from the asserted email.
	InputEmail string `json:"inputEmail,omitempty"`

	// Kind: The fixed string "identitytoolkit#VerifyAssertionResponse".
	Kind string `json:"kind,omitempty"`

	// Language: The language preference of the user.
	Language string `json:"language,omitempty"`

	// LastName: The last name of the user.
	LastName string `json:"lastName,omitempty"`

	// LocalId: The RP local ID if it's already been mapped to the IdP
	// account identified by the federated ID.
	LocalId string `json:"localId,omitempty"`

	// NeedConfirmation: Whether the assertion is from a non-trusted IDP and
	// need account linking confirmation.
	NeedConfirmation bool `json:"needConfirmation,omitempty"`

	// NeedEmail: Whether need client to supply email to complete the
	// federated login flow.
	NeedEmail bool `json:"needEmail,omitempty"`

	// NickName: The nick name of the user.
	NickName string `json:"nickName,omitempty"`

	// OauthAccessToken: The OAuth2 access token.
	OauthAccessToken string `json:"oauthAccessToken,omitempty"`

	// OauthAuthorizationCode: The OAuth2 authorization code.
	OauthAuthorizationCode string `json:"oauthAuthorizationCode,omitempty"`

	// OauthExpireIn: The lifetime in seconds of the OAuth2 access token.
	OauthExpireIn int64 `json:"oauthExpireIn,omitempty"`

	// OauthIdToken: The OIDC id token.
	OauthIdToken string `json:"oauthIdToken,omitempty"`

	// OauthRequestToken: The user approved request token for the OpenID
	// OAuth extension.
	OauthRequestToken string `json:"oauthRequestToken,omitempty"`

	// OauthScope: The scope for the OpenID OAuth extension.
	OauthScope string `json:"oauthScope,omitempty"`

	// OauthTokenSecret: The OAuth1 access token secret.
	OauthTokenSecret string `json:"oauthTokenSecret,omitempty"`

	// OriginalEmail: The original email stored in the mapping storage. It's
	// returned when the federated ID is associated to a different email.
	OriginalEmail string `json:"originalEmail,omitempty"`

	// PhotoUrl: The URI of the public accessible profiel picture.
	PhotoUrl string `json:"photoUrl,omitempty"`

	// ProviderId: The IdP ID. For white listed IdPs it's a short domain
	// name e.g. google.com, aol.com, live.net and yahoo.com. If the
	// "providerId" param is set to OpenID OP identifer other than the
	// whilte listed IdPs the OP identifier is returned. If the "identifier"
	// param is federated ID in the createAuthUri request. The domain part
	// of the federated ID is returned.
	ProviderId string `json:"providerId,omitempty"`

	// RawUserInfo: Raw IDP-returned user info.
	RawUserInfo string `json:"rawUserInfo,omitempty"`

	// RefreshToken: If idToken is STS id token, then this field will be
	// refresh token.
	RefreshToken string `json:"refreshToken,omitempty"`

	// ScreenName: The screen_name of a Twitter user or the login name at
	// Github.
	ScreenName string `json:"screenName,omitempty"`

	// TimeZone: The timezone of the user.
	TimeZone string `json:"timeZone,omitempty"`

	// VerifiedProvider: When action is 'map', contains the idps which can
	// be used for confirmation.
	VerifiedProvider []string `json:"verifiedProvider,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Action") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Action") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VerifyAssertionResponse) MarshalJSON() ([]byte, error) {
	type noMethod VerifyAssertionResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VerifyCustomTokenResponse: Response from verifying a custom token
type VerifyCustomTokenResponse struct {
	// ExpiresIn: If idToken is STS id token, then this field will be
	// expiration time of STS id token in seconds.
	ExpiresIn int64 `json:"expiresIn,omitempty,string"`

	// IdToken: The GITKit token for authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// Kind: The fixed string "identitytoolkit#VerifyCustomTokenResponse".
	Kind string `json:"kind,omitempty"`

	// RefreshToken: If idToken is STS id token, then this field will be
	// refresh token.
	RefreshToken string `json:"refreshToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "ExpiresIn") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ExpiresIn") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *VerifyCustomTokenResponse) MarshalJSON() ([]byte, error) {
	type noMethod VerifyCustomTokenResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// VerifyPasswordResponse: Request of verifying the password.
type VerifyPasswordResponse struct {
	// DisplayName: The name of the user.
	DisplayName string `json:"displayName,omitempty"`

	// Email: The email returned by the IdP. NOTE: The federated login user
	// may not own the email.
	Email string `json:"email,omitempty"`

	// ExpiresIn: If idToken is STS id token, then this field will be
	// expiration time of STS id token in seconds.
	ExpiresIn int64 `json:"expiresIn,omitempty,string"`

	// IdToken: The GITKit token for authenticated user.
	IdToken string `json:"idToken,omitempty"`

	// Kind: The fixed string "identitytoolkit#VerifyPasswordResponse".
	Kind string `json:"kind,omitempty"`

	// LocalId: The RP local ID if it's already been mapped to the IdP
	// account identified by the federated ID.
	LocalId string `json:"localId,omitempty"`

	// OauthAccessToken: The OAuth2 access token.
	OauthAccessToken string `json:"oauthAccessToken,omitempty"`

	// OauthAuthorizationCode: The OAuth2 authorization code.
	OauthAuthorizationCode string `json:"oauthAuthorizationCode,omitempty"`

	// OauthExpireIn: The lifetime in seconds of the OAuth2 access token.
	OauthExpireIn int64 `json:"oauthExpireIn,omitempty"`

	// PhotoUrl: The URI of the user's photo at IdP
	PhotoUrl string `json:"photoUrl,omitempty"`

	// RefreshToken: If idToken is STS id token, then this field will be
	// refresh token.
	RefreshToken string `json:"refreshToken,omitempty"`

	// Registered: Whether the email is registered.
	Registered bool `json:"registered,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

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

func (s *VerifyPasswordResponse) MarshalJSON() ([]byte, error) {
	type noMethod VerifyPasswordResponse
	raw := noMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// method id "identitytoolkit.relyingparty.createAuthUri":

type RelyingpartyCreateAuthUriCall struct {
	s                                               *Service
	identitytoolkitrelyingpartycreateauthurirequest *IdentitytoolkitRelyingpartyCreateAuthUriRequest
	urlParams_                                      gensupport.URLParams
	ctx_                                            context.Context
	header_                                         http.Header
}

// CreateAuthUri: Creates the URI used by the IdP to authenticate the
// user.
func (r *RelyingpartyService) CreateAuthUri(identitytoolkitrelyingpartycreateauthurirequest *IdentitytoolkitRelyingpartyCreateAuthUriRequest) *RelyingpartyCreateAuthUriCall {
	c := &RelyingpartyCreateAuthUriCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartycreateauthurirequest = identitytoolkitrelyingpartycreateauthurirequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyCreateAuthUriCall) Fields(s ...googleapi.Field) *RelyingpartyCreateAuthUriCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyCreateAuthUriCall) Context(ctx context.Context) *RelyingpartyCreateAuthUriCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyCreateAuthUriCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyCreateAuthUriCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartycreateauthurirequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "createAuthUri")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.createAuthUri" call.
// Exactly one of *CreateAuthUriResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *CreateAuthUriResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyCreateAuthUriCall) Do(opts ...googleapi.CallOption) (*CreateAuthUriResponse, error) {
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
	ret := &CreateAuthUriResponse{
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
	//   "description": "Creates the URI used by the IdP to authenticate the user.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.createAuthUri",
	//   "path": "createAuthUri",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyCreateAuthUriRequest"
	//   },
	//   "response": {
	//     "$ref": "CreateAuthUriResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.deleteAccount":

type RelyingpartyDeleteAccountCall struct {
	s                                               *Service
	identitytoolkitrelyingpartydeleteaccountrequest *IdentitytoolkitRelyingpartyDeleteAccountRequest
	urlParams_                                      gensupport.URLParams
	ctx_                                            context.Context
	header_                                         http.Header
}

// DeleteAccount: Delete user account.
func (r *RelyingpartyService) DeleteAccount(identitytoolkitrelyingpartydeleteaccountrequest *IdentitytoolkitRelyingpartyDeleteAccountRequest) *RelyingpartyDeleteAccountCall {
	c := &RelyingpartyDeleteAccountCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartydeleteaccountrequest = identitytoolkitrelyingpartydeleteaccountrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyDeleteAccountCall) Fields(s ...googleapi.Field) *RelyingpartyDeleteAccountCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyDeleteAccountCall) Context(ctx context.Context) *RelyingpartyDeleteAccountCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyDeleteAccountCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyDeleteAccountCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartydeleteaccountrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "deleteAccount")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.deleteAccount" call.
// Exactly one of *DeleteAccountResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *DeleteAccountResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyDeleteAccountCall) Do(opts ...googleapi.CallOption) (*DeleteAccountResponse, error) {
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
	ret := &DeleteAccountResponse{
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
	//   "description": "Delete user account.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.deleteAccount",
	//   "path": "deleteAccount",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyDeleteAccountRequest"
	//   },
	//   "response": {
	//     "$ref": "DeleteAccountResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.downloadAccount":

type RelyingpartyDownloadAccountCall struct {
	s                                                 *Service
	identitytoolkitrelyingpartydownloadaccountrequest *IdentitytoolkitRelyingpartyDownloadAccountRequest
	urlParams_                                        gensupport.URLParams
	ctx_                                              context.Context
	header_                                           http.Header
}

// DownloadAccount: Batch download user accounts.
func (r *RelyingpartyService) DownloadAccount(identitytoolkitrelyingpartydownloadaccountrequest *IdentitytoolkitRelyingpartyDownloadAccountRequest) *RelyingpartyDownloadAccountCall {
	c := &RelyingpartyDownloadAccountCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartydownloadaccountrequest = identitytoolkitrelyingpartydownloadaccountrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyDownloadAccountCall) Fields(s ...googleapi.Field) *RelyingpartyDownloadAccountCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyDownloadAccountCall) Context(ctx context.Context) *RelyingpartyDownloadAccountCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyDownloadAccountCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyDownloadAccountCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartydownloadaccountrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "downloadAccount")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.downloadAccount" call.
// Exactly one of *DownloadAccountResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *DownloadAccountResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyDownloadAccountCall) Do(opts ...googleapi.CallOption) (*DownloadAccountResponse, error) {
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
	ret := &DownloadAccountResponse{
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
	//   "description": "Batch download user accounts.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.downloadAccount",
	//   "path": "downloadAccount",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyDownloadAccountRequest"
	//   },
	//   "response": {
	//     "$ref": "DownloadAccountResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/firebase"
	//   ]
	// }

}

// method id "identitytoolkit.relyingparty.getAccountInfo":

type RelyingpartyGetAccountInfoCall struct {
	s                                                *Service
	identitytoolkitrelyingpartygetaccountinforequest *IdentitytoolkitRelyingpartyGetAccountInfoRequest
	urlParams_                                       gensupport.URLParams
	ctx_                                             context.Context
	header_                                          http.Header
}

// GetAccountInfo: Returns the account info.
func (r *RelyingpartyService) GetAccountInfo(identitytoolkitrelyingpartygetaccountinforequest *IdentitytoolkitRelyingpartyGetAccountInfoRequest) *RelyingpartyGetAccountInfoCall {
	c := &RelyingpartyGetAccountInfoCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartygetaccountinforequest = identitytoolkitrelyingpartygetaccountinforequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyGetAccountInfoCall) Fields(s ...googleapi.Field) *RelyingpartyGetAccountInfoCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyGetAccountInfoCall) Context(ctx context.Context) *RelyingpartyGetAccountInfoCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyGetAccountInfoCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyGetAccountInfoCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartygetaccountinforequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "getAccountInfo")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.getAccountInfo" call.
// Exactly one of *GetAccountInfoResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *GetAccountInfoResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyGetAccountInfoCall) Do(opts ...googleapi.CallOption) (*GetAccountInfoResponse, error) {
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
	ret := &GetAccountInfoResponse{
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
	//   "description": "Returns the account info.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.getAccountInfo",
	//   "path": "getAccountInfo",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyGetAccountInfoRequest"
	//   },
	//   "response": {
	//     "$ref": "GetAccountInfoResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.getOobConfirmationCode":

type RelyingpartyGetOobConfirmationCodeCall struct {
	s            *Service
	relyingparty *Relyingparty
	urlParams_   gensupport.URLParams
	ctx_         context.Context
	header_      http.Header
}

// GetOobConfirmationCode: Get a code for user action confirmation.
func (r *RelyingpartyService) GetOobConfirmationCode(relyingparty *Relyingparty) *RelyingpartyGetOobConfirmationCodeCall {
	c := &RelyingpartyGetOobConfirmationCodeCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.relyingparty = relyingparty
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyGetOobConfirmationCodeCall) Fields(s ...googleapi.Field) *RelyingpartyGetOobConfirmationCodeCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyGetOobConfirmationCodeCall) Context(ctx context.Context) *RelyingpartyGetOobConfirmationCodeCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyGetOobConfirmationCodeCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyGetOobConfirmationCodeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.relyingparty)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "getOobConfirmationCode")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.getOobConfirmationCode" call.
// Exactly one of *GetOobConfirmationCodeResponse or error will be
// non-nil. Any non-2xx status code is an error. Response headers are in
// either *GetOobConfirmationCodeResponse.ServerResponse.Header or (if a
// response was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyGetOobConfirmationCodeCall) Do(opts ...googleapi.CallOption) (*GetOobConfirmationCodeResponse, error) {
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
	ret := &GetOobConfirmationCodeResponse{
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
	//   "description": "Get a code for user action confirmation.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.getOobConfirmationCode",
	//   "path": "getOobConfirmationCode",
	//   "request": {
	//     "$ref": "Relyingparty"
	//   },
	//   "response": {
	//     "$ref": "GetOobConfirmationCodeResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.getProjectConfig":

type RelyingpartyGetProjectConfigCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetProjectConfig: Get project configuration.
func (r *RelyingpartyService) GetProjectConfig() *RelyingpartyGetProjectConfigCall {
	c := &RelyingpartyGetProjectConfigCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// DelegatedProjectNumber sets the optional parameter
// "delegatedProjectNumber": Delegated GCP project number of the
// request.
func (c *RelyingpartyGetProjectConfigCall) DelegatedProjectNumber(delegatedProjectNumber string) *RelyingpartyGetProjectConfigCall {
	c.urlParams_.Set("delegatedProjectNumber", delegatedProjectNumber)
	return c
}

// ProjectNumber sets the optional parameter "projectNumber": GCP
// project number of the request.
func (c *RelyingpartyGetProjectConfigCall) ProjectNumber(projectNumber string) *RelyingpartyGetProjectConfigCall {
	c.urlParams_.Set("projectNumber", projectNumber)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyGetProjectConfigCall) Fields(s ...googleapi.Field) *RelyingpartyGetProjectConfigCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RelyingpartyGetProjectConfigCall) IfNoneMatch(entityTag string) *RelyingpartyGetProjectConfigCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyGetProjectConfigCall) Context(ctx context.Context) *RelyingpartyGetProjectConfigCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyGetProjectConfigCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyGetProjectConfigCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "getProjectConfig")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.getProjectConfig" call.
// Exactly one of *IdentitytoolkitRelyingpartyGetProjectConfigResponse
// or error will be non-nil. Any non-2xx status code is an error.
// Response headers are in either
// *IdentitytoolkitRelyingpartyGetProjectConfigResponse.ServerResponse.He
// ader or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *RelyingpartyGetProjectConfigCall) Do(opts ...googleapi.CallOption) (*IdentitytoolkitRelyingpartyGetProjectConfigResponse, error) {
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
	ret := &IdentitytoolkitRelyingpartyGetProjectConfigResponse{
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
	//   "description": "Get project configuration.",
	//   "httpMethod": "GET",
	//   "id": "identitytoolkit.relyingparty.getProjectConfig",
	//   "parameters": {
	//     "delegatedProjectNumber": {
	//       "description": "Delegated GCP project number of the request.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "projectNumber": {
	//       "description": "GCP project number of the request.",
	//       "location": "query",
	//       "type": "string"
	//     }
	//   },
	//   "path": "getProjectConfig",
	//   "response": {
	//     "$ref": "IdentitytoolkitRelyingpartyGetProjectConfigResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.getPublicKeys":

type RelyingpartyGetPublicKeysCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetPublicKeys: Get token signing public key.
func (r *RelyingpartyService) GetPublicKeys() *RelyingpartyGetPublicKeysCall {
	c := &RelyingpartyGetPublicKeysCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyGetPublicKeysCall) Fields(s ...googleapi.Field) *RelyingpartyGetPublicKeysCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RelyingpartyGetPublicKeysCall) IfNoneMatch(entityTag string) *RelyingpartyGetPublicKeysCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyGetPublicKeysCall) Context(ctx context.Context) *RelyingpartyGetPublicKeysCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyGetPublicKeysCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyGetPublicKeysCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "publicKeys")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.getPublicKeys" call.
func (c *RelyingpartyGetPublicKeysCall) Do(opts ...googleapi.CallOption) (map[string]string, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	var ret map[string]string
	target := &ret
	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Get token signing public key.",
	//   "httpMethod": "GET",
	//   "id": "identitytoolkit.relyingparty.getPublicKeys",
	//   "path": "publicKeys",
	//   "response": {
	//     "$ref": "IdentitytoolkitRelyingpartyGetPublicKeysResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.getRecaptchaParam":

type RelyingpartyGetRecaptchaParamCall struct {
	s            *Service
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// GetRecaptchaParam: Get recaptcha secure param.
func (r *RelyingpartyService) GetRecaptchaParam() *RelyingpartyGetRecaptchaParamCall {
	c := &RelyingpartyGetRecaptchaParamCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyGetRecaptchaParamCall) Fields(s ...googleapi.Field) *RelyingpartyGetRecaptchaParamCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *RelyingpartyGetRecaptchaParamCall) IfNoneMatch(entityTag string) *RelyingpartyGetRecaptchaParamCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyGetRecaptchaParamCall) Context(ctx context.Context) *RelyingpartyGetRecaptchaParamCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyGetRecaptchaParamCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyGetRecaptchaParamCall) doRequest(alt string) (*http.Response, error) {
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
	urls := googleapi.ResolveRelative(c.s.BasePath, "getRecaptchaParam")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("GET", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.getRecaptchaParam" call.
// Exactly one of *GetRecaptchaParamResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *GetRecaptchaParamResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyGetRecaptchaParamCall) Do(opts ...googleapi.CallOption) (*GetRecaptchaParamResponse, error) {
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
	ret := &GetRecaptchaParamResponse{
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
	//   "description": "Get recaptcha secure param.",
	//   "httpMethod": "GET",
	//   "id": "identitytoolkit.relyingparty.getRecaptchaParam",
	//   "path": "getRecaptchaParam",
	//   "response": {
	//     "$ref": "GetRecaptchaParamResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.resetPassword":

type RelyingpartyResetPasswordCall struct {
	s                                               *Service
	identitytoolkitrelyingpartyresetpasswordrequest *IdentitytoolkitRelyingpartyResetPasswordRequest
	urlParams_                                      gensupport.URLParams
	ctx_                                            context.Context
	header_                                         http.Header
}

// ResetPassword: Reset password for a user.
func (r *RelyingpartyService) ResetPassword(identitytoolkitrelyingpartyresetpasswordrequest *IdentitytoolkitRelyingpartyResetPasswordRequest) *RelyingpartyResetPasswordCall {
	c := &RelyingpartyResetPasswordCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartyresetpasswordrequest = identitytoolkitrelyingpartyresetpasswordrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyResetPasswordCall) Fields(s ...googleapi.Field) *RelyingpartyResetPasswordCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyResetPasswordCall) Context(ctx context.Context) *RelyingpartyResetPasswordCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyResetPasswordCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyResetPasswordCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartyresetpasswordrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "resetPassword")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.resetPassword" call.
// Exactly one of *ResetPasswordResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ResetPasswordResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyResetPasswordCall) Do(opts ...googleapi.CallOption) (*ResetPasswordResponse, error) {
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
	ret := &ResetPasswordResponse{
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
	//   "description": "Reset password for a user.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.resetPassword",
	//   "path": "resetPassword",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyResetPasswordRequest"
	//   },
	//   "response": {
	//     "$ref": "ResetPasswordResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.setAccountInfo":

type RelyingpartySetAccountInfoCall struct {
	s                                                *Service
	identitytoolkitrelyingpartysetaccountinforequest *IdentitytoolkitRelyingpartySetAccountInfoRequest
	urlParams_                                       gensupport.URLParams
	ctx_                                             context.Context
	header_                                          http.Header
}

// SetAccountInfo: Set account info for a user.
func (r *RelyingpartyService) SetAccountInfo(identitytoolkitrelyingpartysetaccountinforequest *IdentitytoolkitRelyingpartySetAccountInfoRequest) *RelyingpartySetAccountInfoCall {
	c := &RelyingpartySetAccountInfoCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartysetaccountinforequest = identitytoolkitrelyingpartysetaccountinforequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartySetAccountInfoCall) Fields(s ...googleapi.Field) *RelyingpartySetAccountInfoCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartySetAccountInfoCall) Context(ctx context.Context) *RelyingpartySetAccountInfoCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartySetAccountInfoCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartySetAccountInfoCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartysetaccountinforequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "setAccountInfo")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.setAccountInfo" call.
// Exactly one of *SetAccountInfoResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *SetAccountInfoResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartySetAccountInfoCall) Do(opts ...googleapi.CallOption) (*SetAccountInfoResponse, error) {
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
	ret := &SetAccountInfoResponse{
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
	//   "description": "Set account info for a user.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.setAccountInfo",
	//   "path": "setAccountInfo",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartySetAccountInfoRequest"
	//   },
	//   "response": {
	//     "$ref": "SetAccountInfoResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.setProjectConfig":

type RelyingpartySetProjectConfigCall struct {
	s                                                  *Service
	identitytoolkitrelyingpartysetprojectconfigrequest *IdentitytoolkitRelyingpartySetProjectConfigRequest
	urlParams_                                         gensupport.URLParams
	ctx_                                               context.Context
	header_                                            http.Header
}

// SetProjectConfig: Set project configuration.
func (r *RelyingpartyService) SetProjectConfig(identitytoolkitrelyingpartysetprojectconfigrequest *IdentitytoolkitRelyingpartySetProjectConfigRequest) *RelyingpartySetProjectConfigCall {
	c := &RelyingpartySetProjectConfigCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartysetprojectconfigrequest = identitytoolkitrelyingpartysetprojectconfigrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartySetProjectConfigCall) Fields(s ...googleapi.Field) *RelyingpartySetProjectConfigCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartySetProjectConfigCall) Context(ctx context.Context) *RelyingpartySetProjectConfigCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartySetProjectConfigCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartySetProjectConfigCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartysetprojectconfigrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "setProjectConfig")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.setProjectConfig" call.
// Exactly one of *IdentitytoolkitRelyingpartySetProjectConfigResponse
// or error will be non-nil. Any non-2xx status code is an error.
// Response headers are in either
// *IdentitytoolkitRelyingpartySetProjectConfigResponse.ServerResponse.He
// ader or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *RelyingpartySetProjectConfigCall) Do(opts ...googleapi.CallOption) (*IdentitytoolkitRelyingpartySetProjectConfigResponse, error) {
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
	ret := &IdentitytoolkitRelyingpartySetProjectConfigResponse{
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
	//   "description": "Set project configuration.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.setProjectConfig",
	//   "path": "setProjectConfig",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartySetProjectConfigRequest"
	//   },
	//   "response": {
	//     "$ref": "IdentitytoolkitRelyingpartySetProjectConfigResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.signOutUser":

type RelyingpartySignOutUserCall struct {
	s                                             *Service
	identitytoolkitrelyingpartysignoutuserrequest *IdentitytoolkitRelyingpartySignOutUserRequest
	urlParams_                                    gensupport.URLParams
	ctx_                                          context.Context
	header_                                       http.Header
}

// SignOutUser: Sign out user.
func (r *RelyingpartyService) SignOutUser(identitytoolkitrelyingpartysignoutuserrequest *IdentitytoolkitRelyingpartySignOutUserRequest) *RelyingpartySignOutUserCall {
	c := &RelyingpartySignOutUserCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartysignoutuserrequest = identitytoolkitrelyingpartysignoutuserrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartySignOutUserCall) Fields(s ...googleapi.Field) *RelyingpartySignOutUserCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartySignOutUserCall) Context(ctx context.Context) *RelyingpartySignOutUserCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartySignOutUserCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartySignOutUserCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartysignoutuserrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "signOutUser")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.signOutUser" call.
// Exactly one of *IdentitytoolkitRelyingpartySignOutUserResponse or
// error will be non-nil. Any non-2xx status code is an error. Response
// headers are in either
// *IdentitytoolkitRelyingpartySignOutUserResponse.ServerResponse.Header
// or (if a response was returned at all) in
// error.(*googleapi.Error).Header. Use googleapi.IsNotModified to check
// whether the returned error was because http.StatusNotModified was
// returned.
func (c *RelyingpartySignOutUserCall) Do(opts ...googleapi.CallOption) (*IdentitytoolkitRelyingpartySignOutUserResponse, error) {
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
	ret := &IdentitytoolkitRelyingpartySignOutUserResponse{
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
	//   "description": "Sign out user.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.signOutUser",
	//   "path": "signOutUser",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartySignOutUserRequest"
	//   },
	//   "response": {
	//     "$ref": "IdentitytoolkitRelyingpartySignOutUserResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.signupNewUser":

type RelyingpartySignupNewUserCall struct {
	s                                               *Service
	identitytoolkitrelyingpartysignupnewuserrequest *IdentitytoolkitRelyingpartySignupNewUserRequest
	urlParams_                                      gensupport.URLParams
	ctx_                                            context.Context
	header_                                         http.Header
}

// SignupNewUser: Signup new user.
func (r *RelyingpartyService) SignupNewUser(identitytoolkitrelyingpartysignupnewuserrequest *IdentitytoolkitRelyingpartySignupNewUserRequest) *RelyingpartySignupNewUserCall {
	c := &RelyingpartySignupNewUserCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartysignupnewuserrequest = identitytoolkitrelyingpartysignupnewuserrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartySignupNewUserCall) Fields(s ...googleapi.Field) *RelyingpartySignupNewUserCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartySignupNewUserCall) Context(ctx context.Context) *RelyingpartySignupNewUserCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartySignupNewUserCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartySignupNewUserCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartysignupnewuserrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "signupNewUser")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.signupNewUser" call.
// Exactly one of *SignupNewUserResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *SignupNewUserResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartySignupNewUserCall) Do(opts ...googleapi.CallOption) (*SignupNewUserResponse, error) {
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
	ret := &SignupNewUserResponse{
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
	//   "description": "Signup new user.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.signupNewUser",
	//   "path": "signupNewUser",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartySignupNewUserRequest"
	//   },
	//   "response": {
	//     "$ref": "SignupNewUserResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.uploadAccount":

type RelyingpartyUploadAccountCall struct {
	s                                               *Service
	identitytoolkitrelyingpartyuploadaccountrequest *IdentitytoolkitRelyingpartyUploadAccountRequest
	urlParams_                                      gensupport.URLParams
	ctx_                                            context.Context
	header_                                         http.Header
}

// UploadAccount: Batch upload existing user accounts.
func (r *RelyingpartyService) UploadAccount(identitytoolkitrelyingpartyuploadaccountrequest *IdentitytoolkitRelyingpartyUploadAccountRequest) *RelyingpartyUploadAccountCall {
	c := &RelyingpartyUploadAccountCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartyuploadaccountrequest = identitytoolkitrelyingpartyuploadaccountrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyUploadAccountCall) Fields(s ...googleapi.Field) *RelyingpartyUploadAccountCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyUploadAccountCall) Context(ctx context.Context) *RelyingpartyUploadAccountCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyUploadAccountCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyUploadAccountCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartyuploadaccountrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "uploadAccount")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.uploadAccount" call.
// Exactly one of *UploadAccountResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *UploadAccountResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyUploadAccountCall) Do(opts ...googleapi.CallOption) (*UploadAccountResponse, error) {
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
	ret := &UploadAccountResponse{
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
	//   "description": "Batch upload existing user accounts.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.uploadAccount",
	//   "path": "uploadAccount",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyUploadAccountRequest"
	//   },
	//   "response": {
	//     "$ref": "UploadAccountResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/firebase"
	//   ]
	// }

}

// method id "identitytoolkit.relyingparty.verifyAssertion":

type RelyingpartyVerifyAssertionCall struct {
	s                                                 *Service
	identitytoolkitrelyingpartyverifyassertionrequest *IdentitytoolkitRelyingpartyVerifyAssertionRequest
	urlParams_                                        gensupport.URLParams
	ctx_                                              context.Context
	header_                                           http.Header
}

// VerifyAssertion: Verifies the assertion returned by the IdP.
func (r *RelyingpartyService) VerifyAssertion(identitytoolkitrelyingpartyverifyassertionrequest *IdentitytoolkitRelyingpartyVerifyAssertionRequest) *RelyingpartyVerifyAssertionCall {
	c := &RelyingpartyVerifyAssertionCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartyverifyassertionrequest = identitytoolkitrelyingpartyverifyassertionrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyVerifyAssertionCall) Fields(s ...googleapi.Field) *RelyingpartyVerifyAssertionCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyVerifyAssertionCall) Context(ctx context.Context) *RelyingpartyVerifyAssertionCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyVerifyAssertionCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyVerifyAssertionCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartyverifyassertionrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "verifyAssertion")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.verifyAssertion" call.
// Exactly one of *VerifyAssertionResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *VerifyAssertionResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyVerifyAssertionCall) Do(opts ...googleapi.CallOption) (*VerifyAssertionResponse, error) {
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
	ret := &VerifyAssertionResponse{
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
	//   "description": "Verifies the assertion returned by the IdP.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.verifyAssertion",
	//   "path": "verifyAssertion",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyVerifyAssertionRequest"
	//   },
	//   "response": {
	//     "$ref": "VerifyAssertionResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.verifyCustomToken":

type RelyingpartyVerifyCustomTokenCall struct {
	s                                                   *Service
	identitytoolkitrelyingpartyverifycustomtokenrequest *IdentitytoolkitRelyingpartyVerifyCustomTokenRequest
	urlParams_                                          gensupport.URLParams
	ctx_                                                context.Context
	header_                                             http.Header
}

// VerifyCustomToken: Verifies the developer asserted ID token.
func (r *RelyingpartyService) VerifyCustomToken(identitytoolkitrelyingpartyverifycustomtokenrequest *IdentitytoolkitRelyingpartyVerifyCustomTokenRequest) *RelyingpartyVerifyCustomTokenCall {
	c := &RelyingpartyVerifyCustomTokenCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartyverifycustomtokenrequest = identitytoolkitrelyingpartyverifycustomtokenrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyVerifyCustomTokenCall) Fields(s ...googleapi.Field) *RelyingpartyVerifyCustomTokenCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyVerifyCustomTokenCall) Context(ctx context.Context) *RelyingpartyVerifyCustomTokenCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyVerifyCustomTokenCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyVerifyCustomTokenCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartyverifycustomtokenrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "verifyCustomToken")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.verifyCustomToken" call.
// Exactly one of *VerifyCustomTokenResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *VerifyCustomTokenResponse.ServerResponse.Header or (if a response
// was returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyVerifyCustomTokenCall) Do(opts ...googleapi.CallOption) (*VerifyCustomTokenResponse, error) {
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
	ret := &VerifyCustomTokenResponse{
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
	//   "description": "Verifies the developer asserted ID token.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.verifyCustomToken",
	//   "path": "verifyCustomToken",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyVerifyCustomTokenRequest"
	//   },
	//   "response": {
	//     "$ref": "VerifyCustomTokenResponse"
	//   }
	// }

}

// method id "identitytoolkit.relyingparty.verifyPassword":

type RelyingpartyVerifyPasswordCall struct {
	s                                                *Service
	identitytoolkitrelyingpartyverifypasswordrequest *IdentitytoolkitRelyingpartyVerifyPasswordRequest
	urlParams_                                       gensupport.URLParams
	ctx_                                             context.Context
	header_                                          http.Header
}

// VerifyPassword: Verifies the user entered password.
func (r *RelyingpartyService) VerifyPassword(identitytoolkitrelyingpartyverifypasswordrequest *IdentitytoolkitRelyingpartyVerifyPasswordRequest) *RelyingpartyVerifyPasswordCall {
	c := &RelyingpartyVerifyPasswordCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.identitytoolkitrelyingpartyverifypasswordrequest = identitytoolkitrelyingpartyverifypasswordrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *RelyingpartyVerifyPasswordCall) Fields(s ...googleapi.Field) *RelyingpartyVerifyPasswordCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *RelyingpartyVerifyPasswordCall) Context(ctx context.Context) *RelyingpartyVerifyPasswordCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *RelyingpartyVerifyPasswordCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *RelyingpartyVerifyPasswordCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.identitytoolkitrelyingpartyverifypasswordrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	urls := googleapi.ResolveRelative(c.s.BasePath, "verifyPassword")
	urls += "?" + c.urlParams_.Encode()
	req, _ := http.NewRequest("POST", urls, body)
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "identitytoolkit.relyingparty.verifyPassword" call.
// Exactly one of *VerifyPasswordResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *VerifyPasswordResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *RelyingpartyVerifyPasswordCall) Do(opts ...googleapi.CallOption) (*VerifyPasswordResponse, error) {
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
	ret := &VerifyPasswordResponse{
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
	//   "description": "Verifies the user entered password.",
	//   "httpMethod": "POST",
	//   "id": "identitytoolkit.relyingparty.verifyPassword",
	//   "path": "verifyPassword",
	//   "request": {
	//     "$ref": "IdentitytoolkitRelyingpartyVerifyPasswordRequest"
	//   },
	//   "response": {
	//     "$ref": "VerifyPasswordResponse"
	//   }
	// }

}
