module vitess.io/vitess

go 1.12

require (
	cloud.google.com/go v0.43.0
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/aws/aws-sdk-go v0.0.0-20180223184012-ebef4262e06a
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/etcd v0.0.0-20170626015032-703663d1f6ed
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/ghodss/yaml v0.0.0-20161207003320-04f313413ffd // indirect
	github.com/go-ini/ini v1.12.0
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.0-20170215233205-553a64147049
	github.com/googleapis/gax-go v1.0.3
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e
	github.com/gorilla/websocket v0.0.0-20160912153041-2d1e4548da23
	github.com/grpc-ecosystem/go-grpc-middleware v0.0.0-20190118093823-f849b5445de4
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20180418170936-39de4380c2e0
	github.com/grpc-ecosystem/grpc-gateway v0.0.0-20161128002007-199c40a060d1
	github.com/hashicorp/consul v1.4.0
	github.com/hashicorp/go-cleanhttp v0.5.0
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-rootcerts v0.0.0-20160503143440-6bb64b370b90
	github.com/hashicorp/go-uuid v1.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/hashicorp/memberlist v0.1.4 // indirect
	github.com/hashicorp/serf v0.0.0-20161207011743-d3a67ab21bc8
	github.com/jmespath/go-jmespath v0.0.0-20160202185014-0b12d6b521d8
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/jtolds/gls v4.2.1+incompatible
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/klauspost/compress v0.0.0-20180801095237-b50017755d44
	github.com/klauspost/cpuid v1.2.0
	github.com/klauspost/crc32 v1.2.0
	github.com/klauspost/pgzip v1.2.0
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-runewidth v0.0.1
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/go-testing-interface v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.1.2
	github.com/olekukonko/tablewriter v0.0.0-20160115111002-cca8bbc07984
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v0.0.0-20160824210600-b984ec7fa9ff
	github.com/pkg/errors v0.8.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/client_golang v0.9.2
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/prometheus/common v0.0.0-20181126121408-4724e9255275
	github.com/prometheus/procfs v0.0.0-20181204211112-1dc9a6cbc91a
	github.com/satori/go.uuid v0.0.0-20160713180306-0aa62d5ddceb // indirect
	github.com/sergi/go-diff v0.0.0-20170409071739-feef008d51ad
	github.com/smartystreets/assertions v0.0.0-20190116191733-b6c0e53d7304
	github.com/smartystreets/goconvey v0.0.0-20181108003508-044398e4856c
	github.com/stretchr/testify v1.3.0
	github.com/tchap/go-patricia v0.0.0-20160729071656-dd168db6051b
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible
	github.com/ugorji/go v1.1.7 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/yudai/gojsondiff v0.0.0-20170626131258-081cda2ee950
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82
	github.com/yudai/pp v2.0.1+incompatible // indirect
	github.com/z-division/go-zookeeper v0.0.0-20190128072838-6d7457066b9b
	go.uber.org/atomic v1.4.0 // indirect
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4
	golang.org/x/net v0.0.0-20190724013045-ca1201d0de80
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sys v0.0.0-20190812172437-4e8604ab3aff
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20190812233024-afc3694995b6 // indirect
	google.golang.org/api v0.7.0
	google.golang.org/appengine v1.6.1
	google.golang.org/genproto v0.0.0-20190801165951-fa694d86fc64 // indirect
	google.golang.org/grpc v1.21.1
	gopkg.in/asn1-ber.v1 v1.0.0-20150924051756-4e86f4367175
	gopkg.in/ldap.v2 v2.5.0
	gopkg.in/yaml.v2 v2.2.1
	honnef.co/go/tools v0.0.1-2019.2.2 // indirect
)
