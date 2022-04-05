module github.com/sapcc/maria-back-me-up

go 1.16

require (
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/aws/aws-sdk-go v1.23.21
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/go-mysql-org/go-mysql v1.3.0
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/gorilla/sessions v1.2.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/jmoiron/sqlx v1.3.4 // indirect
	github.com/labstack/echo v3.3.10+incompatible
	github.com/labstack/gommon v0.3.0 // indirect
	github.com/namsral/flag v1.7.4-pre
	github.com/ncw/swift v1.0.50
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/parser v0.0.0-20210415081931-48e7f467fd74
	github.com/pkg/errors v0.9.1
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.7.0 // indirect
	github.com/robfig/cron/v3 v3.0.1
	github.com/sirupsen/logrus v1.4.2
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	gopkg.in/square/go-jose.v2 v2.4.0 // indirect
	gopkg.in/yaml.v2 v2.3.0
	k8s.io/api v0.17.3
	k8s.io/apimachinery v0.17.3
	k8s.io/client-go v0.17.0
)

replace github.com/docker/docker => github.com/docker/engine v0.0.0-20190717161051-705d9623b7c1
