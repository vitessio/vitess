module vitess.io/vitess

go 1.12

require (
	cloud.google.com/go v0.43.0
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/aws/aws-sdk-go v0.0.0-20180223184012-ebef4262e06a
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/etcd v0.0.0-20170626015032-703663d1f6ed
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/ghodss/yaml v0.0.0-20161207003320-04f313413ffd // indirect
	github.com/go-ini/ini v1.12.0 // indirect
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.0-20170215233205-553a64147049
	github.com/gorilla/websocket v0.0.0-20160912153041-2d1e4548da23
	github.com/grpc-ecosystem/go-grpc-middleware v0.0.0-20190118093823-f849b5445de4
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20180418170936-39de4380c2e0
	github.com/grpc-ecosystem/grpc-gateway v0.0.0-20161128002007-199c40a060d1 // indirect
	github.com/hashicorp/consul v1.4.0
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-rootcerts v0.0.0-20160503143440-6bb64b370b90 // indirect
	github.com/hashicorp/go-uuid v1.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/hashicorp/memberlist v0.1.4 // indirect
	github.com/hashicorp/serf v0.0.0-20161207011743-d3a67ab21bc8 // indirect
	github.com/jmespath/go-jmespath v0.0.0-20160202185014-0b12d6b521d8 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/klauspost/compress v0.0.0-20180801095237-b50017755d44 // indirect
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/klauspost/crc32 v1.2.0 // indirect
	github.com/klauspost/pgzip v1.2.0
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.1 // indirect
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/mitchellh/go-testing-interface v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/olekukonko/tablewriter v0.0.0-20160115111002-cca8bbc07984
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v0.0.0-20160824210600-b984ec7fa9ff
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/platinummonkey/go-concurrency-limits v0.1.7
	github.com/prometheus/client_golang v0.9.2
	github.com/satori/go.uuid v0.0.0-20160713180306-0aa62d5ddceb // indirect
	github.com/stretchr/testify v1.4.0
	github.com/tchap/go-patricia v0.0.0-20160729071656-dd168db6051b
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/ugorji/go v1.1.7 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/z-division/go-zookeeper v0.0.0-20190128072838-6d7457066b9b
	go.uber.org/atomic v1.4.0 // indirect
	golang.org/x/crypto v0.0.0-20190829043050-9756ffdc2472
	golang.org/x/lint v0.0.0-20190409202823-959b441ac422
	golang.org/x/net v0.0.0-20190827160401-ba9fcec4b297
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sys v0.0.0-20190830142957-1e83adbbebd0 // indirect
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20190830154057-c17b040389b9
	google.golang.org/api v0.7.0
	google.golang.org/genproto v0.0.0-20190801165951-fa694d86fc64 // indirect
	google.golang.org/grpc v1.21.1
	gopkg.in/DataDog/dd-trace-go.v1 v1.17.0
	gopkg.in/asn1-ber.v1 v1.0.0-20150924051756-4e86f4367175 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/ldap.v2 v2.5.0
	honnef.co/go/tools v0.0.0-20190418001031-e561f6794a2a
)
