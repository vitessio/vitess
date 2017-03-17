package mysqlconn

import (
	"crypto/rand"
	"crypto/sha1"

	log "github.com/golang/glog"
)

// AuthServer is the interface that servers must implement to validate
// users and passwords. It has two modes:
//
// 1. using salt the way MySQL native auth does it. In that case, the
// password is not sent in the clear, but the salt is used to hash the
// password both on the client and server side, and the result is sent
// and compared.
//
// 2. sending the user / password in the clear (using MySQL Cleartext
// method). The server then gets access to both user and password, and
// can authenticate using any method. If SSL is not used, it means the
// password is sent in the clear. That may not be suitable for some
// use cases.
type AuthServer interface {
	// UseClearText returns true is Cleartext auth is used.
	// - If it is not set, Salt() and ValidateHash() are called.
	// The server starts up in mysql_native_password mode.
	// (but ValidateClearText can also be called, if client
	// switched to Cleartext).
	// - If it is set, ValidateClearText() is called.
	// The server starts up in mysql_clear_password mode.
	UseClearText() bool

	// Salt returns the salt to use for a connection.
	// It should be 20 bytes of data.
	Salt() ([]byte, error)

	// ValidateHash validates the data sent by the client matches
	// what the server computes.  It also returns the user data.
	ValidateHash(salt []byte, user string, authResponse []byte) (string, error)

	// ValidateClearText validates a user / password is correct.
	// It also returns the user data.
	ValidateClearText(user, password string) (string, error)
}

// authServers is a registry of AuthServer implementations.
var authServers = make(map[string]AuthServer)

// RegisterAuthServerImpl registers an implementations of AuthServer.
func RegisterAuthServerImpl(name string, authServer AuthServer) {
	if _, ok := authServers[name]; ok {
		log.Fatalf("AuthServer named %v already exists", name)
	}
	authServers[name] = authServer
}

// GetAuthServer returns an AuthServer by name, or log.Fatalf.
func GetAuthServer(name string) AuthServer {
	authServer, ok := authServers[name]
	if !ok {
		log.Fatalf("no AuthServer name %v registered", name)
	}
	return authServer
}

// newSalt returns a 20 character salt.
func newSalt() ([]byte, error) {
	salt := make([]byte, 20)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	// Salt must be a legal UTF8 string.
	for i := 0; i < len(salt); i++ {
		salt[i] &= 0x7f
		if salt[i] == '\x00' || salt[i] == '$' {
			salt[i]++
		}
	}

	return salt, nil
}

// scramblePassword computes the hash of the password using 4.1+ method.
func scramblePassword(salt, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(salt + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)
	// outer Hash
	crypt.Reset()
	crypt.Write(salt)
	crypt.Write(hash)
	scramble := crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}
