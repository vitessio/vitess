module vitess.io/vitess

go 1.13

require (
	cloud.google.com/go v0.45.1
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/go-autorest/autorest v0.10.0 // indirect
	github.com/GeertJohan/go.rice v1.0.0
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/TylerBrock/colorjson v0.0.0-20180527164720-95ec53f28296
	github.com/armon/consul-api v0.0.0-20180202201655-eb2c6b5be1b6
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878
	github.com/aws/aws-sdk-go v1.28.8
	github.com/buger/jsonparser v0.0.0-20200322175846-f7e751efca13
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/codegangsta/inject v0.0.0-20150114235600-33e0aa1cb7c0 // indirect
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.10+incompatible
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/corpix/uarand v0.1.1 // indirect
	github.com/cyberdelia/go-metrics-graphite v0.0.0-20161219230853-39f87cc3b432
	github.com/evanphx/json-patch v4.5.0+incompatible
	github.com/go-martini/martini v0.0.0-20170121215854-22fa46961aab
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.4.0
	github.com/googleapis/gnostic v0.2.0 // indirect
	github.com/gorilla/websocket v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/consul/api v1.5.0
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/hashicorp/serf v0.9.2 // indirect
	github.com/howeyc/gopass v0.0.0-20190910152052-7cb4b85ec19c
	github.com/icrowley/fake v0.0.0-20180203215853-4178557ae428
	github.com/imdario/mergo v0.3.6 // indirect
	github.com/klauspost/compress v1.4.1 // indirect
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/klauspost/pgzip v1.2.4
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/krishicks/yaml-patch v0.0.10
	github.com/magiconair/properties v1.8.1
	github.com/manifoldco/promptui v0.7.0
	github.com/martini-contrib/auth v0.0.0-20150219114609-fa62c19b7ae8
	github.com/martini-contrib/gzip v0.0.0-20151124214156-6c035326b43f
	github.com/martini-contrib/render v0.0.0-20150707142108-ec18f8345a11
	github.com/mattn/go-sqlite3 v1.14.0
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/mitchellh/go-testing-interface v1.14.0 // indirect
	github.com/mitchellh/mapstructure v1.2.3 // indirect
	github.com/montanaflynn/stats v0.6.3
	github.com/olekukonko/tablewriter v0.0.5-0.20200416053754-163badb3bac6
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing/opentracing-go v1.1.0
	github.com/oxtoacart/bpool v0.0.0-20190530202638-03653db5a59c // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pborman/uuid v1.2.0
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pires/go-proxyproto v0.0.0-20191211124218-517ecdf5bb2b
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.4.1
	github.com/prometheus/common v0.9.1
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/samuel/go-zookeeper v0.0.0-20200724154423-2164a8ac840e
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sjmudd/stopwatch v0.0.0-20170613150411-f380bf8a9be1
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.4.0
	github.com/tchap/go-patricia v0.0.0-20160729071656-dd168db6051b
	github.com/tebeka/selenium v0.9.9
	github.com/tinylib/msgp v1.1.1 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/ugorji/go v1.1.7 // indirect
	github.com/z-division/go-zookeeper v0.0.0-20190128072838-6d7457066b9b
	golang.org/x/crypto v0.0.0-20200220183623-bac4c82f6975
	golang.org/x/lint v0.0.0-20190409202823-959b441ac422
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20191219041853-979b82bfef62
	google.golang.org/api v0.9.0
	google.golang.org/genproto v0.0.0-20190926190326-7ee9db18f195 // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.17.0
	gopkg.in/asn1-ber.v1 v1.0.0-20181015200546-f715ec2f112d // indirect
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/ini.v1 v1.51.0 // indirect
	gopkg.in/ldap.v2 v2.5.0
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gotest.tools v2.2.0+incompatible
	honnef.co/go/tools v0.0.1-2019.2.3
	k8s.io/apiextensions-apiserver v0.17.3
	k8s.io/apimachinery v0.17.3
	k8s.io/client-go v0.17.3
	k8s.io/utils v0.0.0-20191114184206-e782cd3c129f
	sigs.k8s.io/yaml v1.1.0
)
