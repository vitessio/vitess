module vitess.io/vitess

go 1.20

require (
	cloud.google.com/go/storage v1.29.0
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20230106234847-43070de90fa1
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/HdrHistogram/hdrhistogram-go v0.9.0 // indirect
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/aquarapid/vaultlib v0.5.1
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/aws/aws-sdk-go v1.44.192
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/corpix/uarand v0.1.1 // indirect
	github.com/dave/jennifer v1.6.0
	github.com/evanphx/json-patch v5.6.0+incompatible
	github.com/fsnotify/fsnotify v1.6.0
	github.com/go-sql-driver/mysql v1.7.0
	github.com/golang/glog v1.0.0
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.9
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.3.0
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/consul/api v1.18.0
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/icrowley/fake v0.0.0-20180203215853-4178557ae428
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.16.5
	github.com/klauspost/pgzip v1.2.5
	github.com/krishicks/yaml-patch v0.0.10
	github.com/magiconair/properties v1.8.7
	github.com/mattn/go-sqlite3 v1.14.16 // indirect
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/montanaflynn/stats v0.7.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing/opentracing-go v1.2.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pires/go-proxyproto v0.6.2
	github.com/pkg/errors v0.9.1
	github.com/planetscale/pargzip v0.0.0-20201116224723-90c7fc03ea8a
	github.com/planetscale/vtprotobuf v0.4.0
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/common v0.39.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/sjmudd/stopwatch v0.1.1
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.6.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.15.0
	github.com/stretchr/testify v1.8.1
	github.com/tchap/go-patricia v2.3.0+incompatible
	github.com/tidwall/gjson v1.12.1
	github.com/tinylib/msgp v1.1.8 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82
	github.com/z-division/go-zookeeper v1.0.0
	go.etcd.io/etcd/api/v3 v3.5.7
	go.etcd.io/etcd/client/pkg/v3 v3.5.7
	go.etcd.io/etcd/client/v3 v3.5.7
	golang.org/x/crypto v0.5.0 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/net v0.7.0
	golang.org/x/oauth2 v0.4.0
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/term v0.5.0
	golang.org/x/text v0.7.0
	golang.org/x/time v0.3.0
	golang.org/x/tools v0.5.0
	google.golang.org/api v0.109.0
	google.golang.org/genproto v0.0.0-20230131230820-1c016267d619 // indirect
	google.golang.org/grpc v1.52.3
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.2.0
	google.golang.org/grpc/examples v0.0.0-20210430044426-28078834f35b
	google.golang.org/protobuf v1.28.1
	gopkg.in/DataDog/dd-trace-go.v1 v1.47.0
	gopkg.in/asn1-ber.v1 v1.0.0-20181015200546-f715ec2f112d // indirect
	gopkg.in/ldap.v2 v2.5.1
	gotest.tools v2.2.0+incompatible
	sigs.k8s.io/yaml v1.3.0
)

require (
	github.com/Shopify/toxiproxy/v2 v2.5.0
	github.com/bndr/gotabulate v1.1.2
	github.com/google/safehtml v0.1.0
	github.com/hashicorp/go-version v1.6.0
	github.com/kr/pretty v0.3.1
	github.com/kr/text v0.2.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/nsf/jsondiff v0.0.0-20210926074059-1e845ec5d249
	github.com/spf13/jwalterweatherman v1.1.0
	github.com/xlab/treeprint v1.2.0
	golang.org/x/exp v0.0.0-20230131160201-f062dba9d201
	golang.org/x/sync v0.1.0
	modernc.org/sqlite v1.20.3
)

require (
	cloud.google.com/go v0.109.0 // indirect
	cloud.google.com/go/compute v1.18.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.10.0 // indirect
	github.com/DataDog/datadog-agent/pkg/obfuscate v0.42.0 // indirect
	github.com/DataDog/datadog-agent/pkg/remoteconfig/state v0.42.0 // indirect
	github.com/DataDog/datadog-go/v5 v5.2.0 // indirect
	github.com/DataDog/go-tuf v0.3.0--fix-localmeta-fork // indirect
	github.com/DataDog/sketches-go v1.4.1 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/andybalholm/cascadia v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/cyphar/filepath-securejoin v0.2.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.14.1 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.1 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.4.0 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.9 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/onsi/gomega v1.23.0 // indirect
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.3 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/secure-systems-lab/go-securesystemslib v0.4.0 // indirect
	github.com/spf13/afero v1.9.3 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/subosito/gotenv v1.4.2 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	go.uber.org/zap v1.23.0 // indirect
	go4.org/intern v0.0.0-20220617035311-6925f38cc365 // indirect
	go4.org/unsafe/assume-no-moving-gc v0.0.0-20220617031537-928513b29760 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	inet.af/netaddr v0.0.0-20220811202034-502d2d690317 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.40.0 // indirect
	modernc.org/ccgo/v3 v3.16.13 // indirect
	modernc.org/libc v1.22.2 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.5.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.1.0 // indirect
)
