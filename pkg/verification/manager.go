package verification

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
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
	k8sm, err := k8s.NewMaria(c.Namespace)
	if err != nil {
		return
	}
	logger.Debugf("Starting verification service %s", c.VerificationService)
	sm, err := storage.NewManager(c.StorageService, "", c.BackupService.MariaDB.LogBin)
	if err != nil {
		return
	}
	svc := sm.GetStorageServices()
	for _, stg := range svc {
		v, err := NewVerification(c.ServiceName, stg, c.StorageService, c.VerificationService, c.BackupService.MariaDB, k8sm)
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

func (m *Manager) Stop() {

}
