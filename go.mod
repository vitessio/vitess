module vitess.io/vitess

go 1.12

require (
	cloud.google.com/go v0.45.1
	github.com/GeertJohan/go.rice v1.0.0
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/aws/aws-sdk-go v0.0.0-20180223184012-ebef4262e06a
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.10+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/corpix/uarand v0.1.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible
	github.com/go-ini/ini v1.12.0 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.0-20170215233205-553a64147049
	github.com/gorilla/websocket v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.9.0 // indirect
	github.com/hashicorp/consul v1.4.0
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-rootcerts v0.0.0-20160503143440-6bb64b370b90 // indirect
	github.com/hashicorp/go-uuid v1.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/hashicorp/memberlist v0.1.4 // indirect
	github.com/hashicorp/serf v0.0.0-20161207011743-d3a67ab21bc8 // indirect
	github.com/icrowley/fake v0.0.0-20180203215853-4178557ae428
	github.com/jmespath/go-jmespath v0.0.0-20160202185014-0b12d6b521d8 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/klauspost/compress v1.4.1 // indirect
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/klauspost/crc32 v1.2.0 // indirect
	github.com/klauspost/pgzip v1.2.0
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/krishicks/yaml-patch v0.0.10
	github.com/mattn/go-runewidth v0.0.1 // indirect
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/mitchellh/go-testing-interface v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/olekukonko/tablewriter v0.0.0-20160115111002-cca8bbc07984
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v1.2.0
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pires/go-proxyproto v0.0.0-20191211124218-517ecdf5bb2b
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/common v0.7.0
	github.com/prometheus/procfs v0.0.5 // indirect
	github.com/satori/go.uuid v0.0.0-20160713180306-0aa62d5ddceb // indirect
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/tchap/go-patricia v0.0.0-20160729071656-dd168db6051b
	github.com/tinylib/msgp v1.1.1 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/ugorji/go v1.1.7 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/z-division/go-zookeeper v0.0.0-20190128072838-6d7457066b9b
	go.etcd.io/bbolt v1.3.2 // indirect
	golang.org/x/crypto v0.0.0-20191205180655-e7c4368fe9dd
	golang.org/x/lint v0.0.0-20190409202823-959b441ac422
	golang.org/x/net v0.0.0-20190926025831-c00fd9afed17
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sys v0.0.0-20191218084908-4a24b4065292 // indirect
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20191219041853-979b82bfef62
	google.golang.org/api v0.9.0
	google.golang.org/genproto v0.0.0-20190926190326-7ee9db18f195 // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.17.0
	gopkg.in/asn1-ber.v1 v1.0.0-20150924051756-4e86f4367175 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/ini.v1 v1.51.0 // indirect
	gopkg.in/ldap.v2 v2.5.0
	gopkg.in/yaml.v2 v2.2.7 // indirect
	honnef.co/go/tools v0.0.1-2019.2.3
)
