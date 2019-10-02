module github.com/sapcc/maria-back-me-up

go 1.12

require (
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/aws/aws-sdk-go v1.23.21
	github.com/coreos/etcd v3.3.15+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/docker/docker v1.13.1
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/fsnotify/fsnotify v1.4.7
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/gorilla/mux v1.7.3
	github.com/labstack/echo v3.3.10+incompatible
	github.com/labstack/gommon v0.3.0 // indirect
	github.com/namsral/flag v1.7.4-pre
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.7.0
	github.com/sapcc/atlas v0.0.0-20190912211211-bd14cd3121cf // indirect
	github.com/siddontang/go-mysql v0.0.0-20190913034749-818e04340bba
	github.com/sirupsen/logrus v1.4.2
	google.golang.org/grpc v1.23.1 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/docker/docker => github.com/docker/engine v0.0.0-20190717161051-705d9623b7c1
