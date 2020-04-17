module github.com/influxdata/influxdb

go 1.12

require (
	collectd.org v0.3.0
	github.com/BurntSushi/toml v0.3.1
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/apache/arrow v0.0.0-20191024131854-af6fa24be0db // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20191024131854-af6fa24be0db
	github.com/aws/aws-sdk-go v1.25.16 // indirect
	github.com/bmizerany/pat v0.0.0-20170815010413-6226ea591a40
	github.com/boltdb/bolt v1.3.1
	github.com/cespare/xxhash v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dgryski/go-bitstream v0.0.0-20180413035011-3522498ce2c8
	github.com/glycerine/go-unsnap-stream v0.0.0-20180323001048-9f0cb55181dd // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.1.1
	github.com/golang/protobuf v1.3.3
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/go-cmp v0.4.0
	github.com/googleapis/gax-go v1.0.3 // indirect
	github.com/goreleaser/archive v1.1.3 // indirect
	github.com/goreleaser/goreleaser v0.94.0 // indirect
	github.com/influxdata/flux v0.65.0
	github.com/influxdata/influxql v1.0.1
	github.com/influxdata/line-protocol v0.0.0-20190220025226-a3afd890113f // indirect
	github.com/influxdata/roaring v0.0.0-20180809181101-fc520f41fab6
	github.com/influxdata/usage-client v0.0.0-20160829180054-6d3895376368
	github.com/jsternberg/markdownfmt v0.0.0-20180204232022-c2a5702991e3 // indirect
	github.com/jsternberg/zap-logfmt v1.0.0
	github.com/jwilder/encoding v0.0.0-20170811194829-b4e1701a28ef
	github.com/klauspost/compress v1.4.0 // indirect
	github.com/klauspost/cpuid v0.0.0-20170728055534-ae7887de9fa5 // indirect
	github.com/klauspost/crc32 v0.0.0-20161016154125-cb6bfca970f6 // indirect
	github.com/klauspost/pgzip v0.0.0-20170402124221-0bf5dcad4ada
	github.com/masterminds/semver v1.4.2 // indirect
	github.com/mattn/go-isatty v0.0.4
	github.com/mattn/go-zglob v0.0.1 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/paulbellamy/ratecounter v0.2.0
	github.com/peterh/liner v0.0.0-20180619022028-8c1271fcf47f
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.0.0
	github.com/retailnext/hllpp v0.0.0-20180308014038-101a6d2f8b52
	github.com/segmentio/kafka-go v0.2.2 // indirect
	github.com/shirou/gopsutil v2.19.12+incompatible
	github.com/shurcooL/go v0.0.0-20190704215121-7189cc372560 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/spf13/cast v1.3.0
	github.com/tinylib/msgp v1.0.2
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/willf/bitset v1.1.3 // indirect
	github.com/xlab/treeprint v0.0.0-20180616005107-d6fb6747feb6
	go.uber.org/zap v1.9.1
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/sys v0.0.0-20200212091648-12a6c2dcc1e4
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	gonum.org/v1/gonum v0.6.0 // indirect
	google.golang.org/grpc v1.27.1
	gopkg.in/fatih/pool.v2 v2.0.0
	gopkg.in/src-d/go-git.v4 v4.8.1 // indirect
)

replace github.com/influxdata/flux => /work/goinflux/src/github.com/influxdata/flux
