package verification

import (
	"context"
	"reflect"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/database"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
)

var (
	logger *logrus.Entry
)

type Manager struct {
	cfg           config.Config
	verifications []*Verification
}

func init() {
	logger = log.WithFields(logrus.Fields{"component": "verification"})
}

func NewManager(c config.Config) (m *Manager, err error) {
	verifications := make([]*Verification, 0)
	sts := make([]*Status, 0)
	k8sm, err := k8s.New(c.Namespace)
	if err != nil {
		return
	}
	db, err := database.NewDatabase(c, nil)
	if err != nil {
		return
	}
	keys := reflect.ValueOf(c.Storages).MapKeys()
	svc := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		svc[i] = keys[i].String()
	}
	for _, stg := range svc {
		sm, err := storage.NewManager(c.Storages, stg, db.GetLogPosition().Format)
		if err != nil {
			return m, err
		}
		v := NewVerification(c.ServiceName, stg, sm, c.Verification, db, k8sm)
		if err != nil {
			return m, err
		}
		verifications = append(verifications, v)
		sts = append(sts, v.status)
	}

	prometheus.MustRegister(NewMetricsCollector(sts))
	return &Manager{
		cfg:           c,
		verifications: verifications,
	}, err
}

func (m *Manager) Start(ctx context.Context) {
	for _, v := range m.verifications {
		logger.Debugf("Starting verification service %s", v.serviceName)
		go v.Start(ctx)
	}
}
