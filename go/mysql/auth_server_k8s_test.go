package mysql

import (
	"flag"
	"path/filepath"
	"testing"

	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"keights.io/api/core/v1"
)

type MockSecretGetter struct {
}

func (m *MockSecretGetter) Get(getOptions metav1.GetOptions) (*v1.Secret, error) {
	return &v1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind: "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vtgate-test-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"MysqlNativePassword": []byte("*EA5F8D65608CF3A37FE8134BF3B14129511E52CF"),
			"UserData":            []byte("vtgate-test-user"),
		},
	}, nil
}

func NewMockAuthServer() *AuthServerK8s {
	return &AuthServerK8s{
		Method:  MysqlNativePassword,
		Entries: make(map[string][]*AuthServerK8sEntry),
		Getter:  &MockSecretGetter{},
	}
}

// Locally, you need to set KUBERNETES_SERVICE_HOST='localhost' in order for this test to work. However, this will break other tests.
// TODO: Unset required environment variables after running k8s tests.

func TestGetK8sServer(t *testing.T) {

	th := &testHandler{}

	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}

	authServer := NewMockAuthServer()
	if authServer == nil {
		t.Fatalf("AuthServer not created correctly")
	}
	RegisterAuthServerImpl("k8s", authServer)
	username := "vtgate-test-user"

	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("Good communication requires active listening: %v", err)
	}

	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: username,
		Pass:  "scalewithvitess",
	}

	_, err = Connect(context.Background(), params)
	// target is localhost, should not work from tcp connection
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
}
