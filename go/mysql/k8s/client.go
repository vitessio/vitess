package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/http2"
	"vitess.io/vitess/go/vt/log"
)

const (
	namespaceDefault = "default"
)

// Client is a Kuberntes client.
type Client struct {
	// The URL of the API server.
	Endpoint   string
	Namespace  string
	SetHeaders func(h http.Header) error

	Client *http.Client
}

// NewClient initializes a client from a client config.
func NewClient(config *Config) (*Client, error) {
	if len(config.Contexts) == 0 {
		if config.CurrentContext != "" {
			return nil, fmt.Errorf("no contexts with name %q", config.CurrentContext)
		}

		if n := len(config.Clusters); n == 0 {
			return nil, errors.New("no clusters provided")
		} else if n > 1 {
			return nil, errors.New("multiple clusters but no current context")
		}
		if n := len(config.AuthInfos); n == 0 {
			return nil, errors.New("no users provided")
		} else if n > 1 {
			return nil, errors.New("multiple users but no current context")
		}

		return newClient(config.Clusters[0].Cluster, config.AuthInfos[0].AuthInfo, namespaceDefault)
	}

	var ctx Context
	if config.CurrentContext == "" {
		if n := len(config.Contexts); n == 0 {
			return nil, errors.New("no contexts provided")
		} else if n > 1 {
			return nil, errors.New("multiple contexts but no current context")
		}
		ctx = config.Contexts[0].Context
	} else {
		for _, c := range config.Contexts {
			if c.Name == config.CurrentContext {
				ctx = c.Context
				goto configFound
			}
		}
		return nil, fmt.Errorf("no config named %q", config.CurrentContext)
	configFound:
	}

	if ctx.Cluster == "" {
		return nil, fmt.Errorf("context doesn't have a cluster")
	}
	if ctx.AuthInfo == "" {
		return nil, fmt.Errorf("context doesn't have a user")
	}
	var (
		user    AuthInfo
		cluster Cluster
	)

	for _, u := range config.AuthInfos {
		if u.Name == ctx.AuthInfo {
			user = u.AuthInfo
			goto userFound
		}
	}
	return nil, fmt.Errorf("no user named %q", ctx.AuthInfo)
userFound:

	for _, c := range config.Clusters {
		if c.Name == ctx.Cluster {
			cluster = c.Cluster
			goto clusterFound
		}
	}
	return nil, fmt.Errorf("no cluster named %q", ctx.Cluster)
clusterFound:

	namespace := ctx.Namespace
	if namespace == "" {
		namespace = namespaceDefault
	}

	return newClient(cluster, user, namespace)
}

// NewInClusterClient returns a client that uses the service account bearer token mounted
// into Kubernetes pods.
func NewInClusterClient() (*Client, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if len(host) == 0 || len(port) == 0 {
		return nil, errors.New("unable to load in-cluster configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined")
	}
	namespace, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return nil, err
	}

	cluster := Cluster{
		Server:               "https://" + host + ":" + port,
		CertificateAuthority: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
	}
	user := AuthInfo{TokenFile: "/var/run/secrets/kubernetes.io/serviceaccount/token"}
	return newClient(cluster, user, string(namespace))
}

func load(filepath string, data []byte) (out []byte, err error) {
	if filepath != "" {
		data, err = ioutil.ReadFile(filepath)
	}
	return data, err
}

func newClient(cluster Cluster, user AuthInfo, namespace string) (*Client, error) {
	if cluster.Server == "" {
		// NOTE: kubectl defaults to localhost:8080, but it's probably better to just
		// be strict.
		return nil, fmt.Errorf("no cluster endpoint provided")
	}

	ca, err := load(cluster.CertificateAuthority, cluster.CertificateAuthorityData)
	if err != nil {
		return nil, fmt.Errorf("loading certificate authority: %v", err)
	}

	clientCert, err := load(user.ClientCertificate, user.ClientCertificateData)
	if err != nil {
		return nil, fmt.Errorf("load client cert: %v", err)
	}
	clientKey, err := load(user.ClientKey, user.ClientKeyData)
	if err != nil {
		return nil, fmt.Errorf("load client cert: %v", err)
	}

	// See https://github.com/gtank/cryptopasta
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cluster.InsecureSkipTLSVerify,
	}

	if len(ca) != 0 {
		tlsConfig.RootCAs = x509.NewCertPool()
		if !tlsConfig.RootCAs.AppendCertsFromPEM(ca) {
			return nil, errors.New("certificate authority doesn't contain any certificates")
		}
	}
	if len(clientCert) != 0 {
		cert, err := tls.X509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, fmt.Errorf("invalid client cert and key pair: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	token := user.Token
	if user.TokenFile != "" {
		data, err := ioutil.ReadFile(user.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("load token file: %v", err)
		}
		token = string(data)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig:       tlsConfig,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if err := http2.ConfigureTransport(transport); err != nil {
		return nil, err
	}

	client := &Client{
		Endpoint:  cluster.Server,
		Namespace: namespace,
		Client: &http.Client{
			Transport: transport,
		},
	}

	if token != "" {
		client.SetHeaders = func(h http.Header) error {
			h.Set("Authorization", "Bearer "+token)
			return nil
		}
	}
	if user.Username != "" && user.Password != "" {
		auth := user.Username + ":" + user.Password
		auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		client.SetHeaders = func(h http.Header) error {
			h.Set("Authorization", auth)
			return nil
		}
	}
	return client, nil
}

func (c *Client) client() *http.Client {
	if c.Client == nil {
		return http.DefaultClient
	}
	return c.Client
}

// Get retrieves a v1 resource from path
func (c *Client) Get(namespace, path string) (map[string]interface{}, error) {
	request, err := http.NewRequest(http.MethodGet, c.Endpoint+"/api/v1/"+namespace+path, nil)
	if err := c.SetHeaders(request.Header); err != nil {
		return nil, err
	}
	response, err := c.client().Do(request)
	if err != nil {
		log.Errorf("Error fetching k8s secret: %v", err)
		return nil, err
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("Error reading resonse body: %v", err)
	}
	rval := make(map[string]interface{})
	err = json.Unmarshal(data, rval)
	if err != nil {
		log.Errorf("Error unmarshalling response body: %v", err)
		return nil, err
	}
	return rval, err
}
