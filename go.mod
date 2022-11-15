module vitess.io/vitess

go 1.19

require (
	cloud.google.com/go/storage v1.10.0
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20211102141018-f7be0cbad29c
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/DataDog/datadog-go v2.2.0+incompatible
	github.com/HdrHistogram/hdrhistogram-go v0.9.0 // indirect
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/aquarapid/vaultlib v0.5.1
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/aws/aws-sdk-go v1.34.2
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/codegangsta/inject v0.0.0-20150114235600-33e0aa1cb7c0 // indirect
	github.com/corpix/uarand v0.1.1 // indirect
	github.com/dave/jennifer v1.4.1
	github.com/evanphx/json-patch v4.9.0+incompatible
	github.com/fsnotify/fsnotify v1.4.9
	github.com/go-martini/martini v0.0.0-20170121215854-22fa46961aab
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/mock v1.5.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3
	github.com/google/go-cmp v0.5.8
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.3.0
	github.com/googleapis/gnostic v0.4.1 // indirect
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/consul/api v1.10.1
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/serf v0.9.7 // indirect
	github.com/howeyc/gopass v0.0.0-20190910152052-7cb4b85ec19c
	github.com/icrowley/fake v0.0.0-20180203215853-4178557ae428
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.13.0
	github.com/klauspost/pgzip v1.2.4
	github.com/krishicks/yaml-patch v0.0.10
	github.com/magiconair/properties v1.8.5
	github.com/mattn/go-sqlite3 v1.14.14
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/mitchellh/go-testing-interface v1.14.0 // indirect
	github.com/montanaflynn/stats v0.6.3
	github.com/olekukonko/tablewriter v0.0.5-0.20200416053754-163badb3bac6
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing/opentracing-go v1.1.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pires/go-proxyproto v0.6.1
	github.com/pkg/errors v0.9.1
	github.com/planetscale/pargzip v0.0.0-20201116224723-90c7fc03ea8a
	github.com/planetscale/vtprotobuf v0.3.0
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.29.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/sjmudd/stopwatch v0.0.0-20170613150411-f380bf8a9be1
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	github.com/spyzhov/ajson v0.4.2
	github.com/stretchr/testify v1.7.1
	github.com/tchap/go-patricia v2.2.6+incompatible
	github.com/tebeka/selenium v0.9.9
	github.com/tidwall/gjson v1.12.1
	github.com/tinylib/msgp v1.1.1 // indirect
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82
	github.com/z-division/go-zookeeper v0.0.0-20190128072838-6d7457066b9b
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/pkg/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	golang.org/x/crypto v0.0.0-20220507011949-2cf3adece122 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211
	golang.org/x/text v0.3.8
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	golang.org/x/tools v0.1.12
	google.golang.org/api v0.45.0
	google.golang.org/genproto v0.0.0-20210701191553-46259e63a0a9 // indirect
	google.golang.org/grpc v1.45.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/grpc/examples v0.0.0-20210430044426-28078834f35b
	google.golang.org/protobuf v1.28.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.17.0
	gopkg.in/asn1-ber.v1 v1.0.0-20181015200546-f715ec2f112d // indirect
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/ldap.v2 v2.5.0
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gotest.tools v2.2.0+incompatible
	honnef.co/go/tools v0.0.1-2020.1.4
	k8s.io/apiextensions-apiserver v0.18.19
	k8s.io/apimachinery v0.20.6
	k8s.io/client-go v0.20.6
	k8s.io/code-generator v0.18.19
	sigs.k8s.io/yaml v1.2.0
)

require (
	github.com/bndr/gotabulate v1.1.2
	github.com/openark/golib v0.0.0-20210531070646-355f37940af8
	golang.org/x/exp v0.0.0-20221023144134-a1e5550cf13e
)

require (
	cloud.google.com/go v0.81.0 // indirect
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/andybalholm/cascadia v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/cyphar/filepath-securejoin v0.2.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.9.0 // indirect
	github.com/felixge/httpsnoop v1.0.1 // indirect
	github.com/frankban/quicktest v1.14.3 // indirect
	github.com/go-logr/logr v0.2.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-hclog v0.12.0 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20190702010315-6dee0af9227d // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/mattn/go-runewidth v0.0.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/onsi/ginkgo v1.12.1 // indirect
	github.com/onsi/gomega v1.10.3 // indirect
	github.com/pelletier/go-toml v1.9.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.62.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	k8s.io/api v0.20.6 // indirect
	k8s.io/gengo v0.0.0-20200413195148-3a45101e95ac // indirect
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.4.0 // indirect
	k8s.io/kube-openapi v0.0.0-20201113171705-d219536bb9fd // indirect
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.0.3 // indirect
)
