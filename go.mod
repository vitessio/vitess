module github.com/planetscale/vitess

go 1.12

require (
	cloud.google.com/go/storage v1.6.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/DataDog/datadog-go v3.5.0+incompatible // indirect
	github.com/GeertJohan/go.rice v1.0.0
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/aws/aws-sdk-go v1.29.29
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/coreos/etcd v3.3.19+incompatible
	github.com/coreos/go-etcd v2.0.0+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/go-ini/ini v1.55.0 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.3.5
	github.com/golang/snappy v0.0.1
	github.com/google/btree v1.0.0 // indirect
	github.com/gorilla/websocket v1.4.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/consul/api v1.4.0
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/klauspost/compress v1.10.3 // indirect
	github.com/klauspost/pgzip v1.2.3
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/olekukonko/tablewriter v0.0.4
	github.com/opentracing-contrib/go-grpc v0.0.0-20191001143057-db30781987df
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v1.2.0
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pires/go-proxyproto v0.0.0-20200315140437-f0371d3cede2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/common v0.9.1
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da // indirect
	github.com/stretchr/testify v1.5.1
	github.com/tchap/go-patricia v2.3.0+incompatible
	github.com/tinylib/msgp v1.1.2 // indirect
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/youtube/vitess v2.1.1+incompatible // indirect
	github.com/z-division/go-zookeeper v1.0.0
	golang.org/x/crypto v0.0.0-20200320181102-891825fb96df
	golang.org/x/net v0.0.0-20200320220750-118fecf932d8
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/api v0.20.0
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/grpc v1.28.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.22.0
	gopkg.in/ldap.v2 v2.5.1
	k8s.io/api v0.17.4 // indirect
	k8s.io/apimachinery v0.17.4
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/utils v0.0.0-20200320200009-4a6ff033650d // indirect
	vitess.io/vitess v2.1.1+incompatible
)
