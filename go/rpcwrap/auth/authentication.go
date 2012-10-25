package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/proto"
)

// UnusedArgument is a type used to indicate an argument that is
// necessary because of net/rpc requirements.
type UnusedArgument string

// cramMD5Credentials maps usernames to lists of secrets.
type cramMD5Credentials map[string][]string

// AuthenticatorCRAMMD5 is an authenticator that uses the SASL
// CRAM-MD5 authentication mechanism.
type AuthenticatorCRAMMD5 struct {
	Credentials cramMD5Credentials
}

// Load loads the contents of a JSON file named
// filename into c.
func (c *cramMD5Credentials) Load(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, c); err != nil {
		return err
	}
	relog.Info("Loaded credentials from %s.", filename)
	return nil
}

// CRAMMD5MaxRequests is the maximum number of requests that an
// unauthenticated client is allowed to make (this should be enough to
// perform authentication).
const CRAMMD5MaxRequests = 2

var (
	AuthenticationServer        = rpc.NewServer()
	DefaultAuthenticatorCRAMMD5 = NewAuthenticatorCRAMMD5()
)

// AuthenticationFailed is returned when the client fails to
// authenticate.
var AuthenticationFailed = errors.New("authentication error: authentication failed")

func NewAuthenticatorCRAMMD5() *AuthenticatorCRAMMD5 {
	return &AuthenticatorCRAMMD5{make(cramMD5Credentials)}
}

// LoadCredentials loads credentials stored in the JSON file named
// filename into the default authenticator.
func LoadCredentials(filename string) error {
	return DefaultAuthenticatorCRAMMD5.Credentials.Load(filename)
}

// Authenticate returns true if it the client manages to authenticate
// the codec in at most maxRequest number of requests.
func Authenticate(c rpc.ServerCodec, context *proto.Context) (bool, error) {
	auth := newAuthenticatedCodec(c)
	for i := 0; i < CRAMMD5MaxRequests; i++ {
		err := AuthenticationServer.ServeRequestWithContext(auth, context)
		if err != nil {
			return false, err
		}
		if auth.OK() {
			return true, nil
		}
	}
	return false, nil
}

// GetNewChallenge gives the client a new challenge for
// authentication.
func (a *AuthenticatorCRAMMD5) GetNewChallenge(_ UnusedArgument, reply *GetNewChallengeReply) error {
	var err error
	reply.Challenge, err = CRAMMD5GetChallenge()
	if err != nil {
		return err
	}
	return nil
}

// Authenticate checks if the client proof is correct.
func (a *AuthenticatorCRAMMD5) Authenticate(context *proto.Context, req *AuthenticateRequest, reply *AuthenticateReply) error {
	username := strings.SplitN(req.Proof, " ", 2)[0]
	secrets, ok := a.Credentials[username]
	if !ok {
		relog.Warning("failed authentication attempt: wrong user: %#v", username)
		return AuthenticationFailed
	}
	if !req.state.challengeIssued {
		relog.Warning("failed authentication attempt: challenge was not issued")
		return AuthenticationFailed
	}
	for _, secret := range secrets {
		if expected := CRAMMD5GetExpected(username, secret, req.state.challenge); expected == req.Proof {
			context.Username = username
			return nil
		}
	}
	relog.Warning("failed authentication attempt: wrong proof")
	return AuthenticationFailed
}

type GetNewChallengeReply struct {
	Challenge string
}

type AuthenticateReply struct{}

type AuthenticateRequest struct {
	Proof string
	state authenticationState
}

// authenticationState maintains the state of the authentication and
// is passed between authenticatedCodec and AuthenticateRequest.
type authenticationState struct {
	authenticated   bool
	challenge       string
	challengeIssued bool
}

// authenticatedCodec is a codec that uses AuthenticatorCRAMMD5 to
// authenticate a request (it implements rpc.ServerCodec). Any
// requests performed before successful authentication will fail.
type authenticatedCodec struct {
	rpc.ServerCodec
	currentMethod string
	state         authenticationState
}

// OK returns true if the codec is authenticated.
func (c *authenticatedCodec) OK() bool {
	return c.state.authenticated
}

// newAuthenticatedCodec returns a fresh authenticatedCodec (in a
// non-authenticated state).
func newAuthenticatedCodec(codec rpc.ServerCodec) *authenticatedCodec {
	return &authenticatedCodec{ServerCodec: codec}
}

// isAuthenticationMethod returns true if method is the name of an
// authentication method.
func isAuthenticationMethod(method string) bool {
	return strings.HasPrefix(method, "AuthenticatorCRAMMD5.")
}

func (c *authenticatedCodec) ReadRequestHeader(r *rpc.Request) error {
	err := c.ServerCodec.ReadRequestHeader(r)

	if err != nil {
		return err
	}

	c.currentMethod = r.ServiceMethod
	return err
}

func (c *authenticatedCodec) ReadRequestBody(body interface{}) error {
	err := c.ServerCodec.ReadRequestBody(body)

	if err != nil {
		return err
	}

	if !isAuthenticationMethod(c.currentMethod) {
		return fmt.Errorf("authentication error: authentication required for %s", c.currentMethod)
	}

	if b, ok := body.(*AuthenticateRequest); ok {
		b.state = c.state
	}

	return err
}

func (c *authenticatedCodec) WriteResponse(r *rpc.Response, body interface{}, last bool) error {
	c.transferState(body)
	return c.ServerCodec.WriteResponse(r, body, last)
}

// transferState transfers the authentication state from body
// (returned by an authentication rpc call) to the codec.
func (c *authenticatedCodec) transferState(body interface{}) {
	switch body.(type) {
	case *GetNewChallengeReply:
		c.state.challenge = body.(*GetNewChallengeReply).Challenge
		c.state.challengeIssued = true
	case *AuthenticateReply:
		c.state.authenticated = true
		c.state.challenge = ""
		c.state.challengeIssued = false
	}
}

func init() {
	AuthenticationServer.Register(DefaultAuthenticatorCRAMMD5)
}
