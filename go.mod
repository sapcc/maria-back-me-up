module github.com/sapcc/maria-back-me-up

go 1.12

require (
	github.com/aws/aws-sdk-go v1.23.21
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/gorilla/sessions v1.2.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/labstack/echo v3.3.10+incompatible
	github.com/labstack/gommon v0.3.0 // indirect
	github.com/namsral/flag v1.7.4-pre
	github.com/ncw/swift v1.0.50
	github.com/pingcap/errors v0.11.0
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.7.0 // indirect
	github.com/robfig/cron/v3 v3.0.1
	github.com/siddontang/go-mysql v0.0.0-20190913034749-818e04340bba
	github.com/sirupsen/logrus v1.4.2
	github.com/slack-go/slack v0.6.6 // indirect
	golang.org/x/crypto v0.0.0-20190820162420-60c769a6c586 // indirect
	golang.org/x/net v0.0.0-20191004110552-13f9640d40b9
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	gopkg.in/square/go-jose.v2 v2.4.0 // indirect
	gopkg.in/yaml.v2 v2.2.8
	k8s.io/api v0.17.3
	k8s.io/apimachinery v0.17.3
	k8s.io/client-go v0.17.0
	k8s.io/utils v0.0.0-20191114184206-e782cd3c129f // indirect
)

replace github.com/docker/docker => github.com/docker/engine v0.0.0-20190717161051-705d9623b7c1
