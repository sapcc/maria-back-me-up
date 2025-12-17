// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package verification

import (
	"context"
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/database"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
)

var (
	logger      *logrus.Entry
	metricsOnce sync.Once
)

// Manager struct for the verification process
type Manager struct {
	cfg           config.Config
	verifications []*Verification
}

func init() {
	logger = log.WithFields(logrus.Fields{"component": "verification"})
}

// NewManager creates a verification manager instance
func NewManager(c config.Config) (m *Manager, err error) {
	verifications := make([]*Verification, 0)
	sts := make([]*Status, 0)
	k8sm, err := k8s.New(c.Namespace)
	if err != nil {
		return
	}
	db, err := database.NewDatabase(c, nil, k8sm)
	if err != nil {
		return
	}

	stgM, err := storage.NewManager(c.Storages, c.ServiceName, c.Backup.RestoreDir, db.GetLogPosition().Format)
	if err != nil {
		return
	}

	for st, svc := range stgM.GetStorageServices() {
		if !svc.Verify() {
			continue
		}
		v := NewVerification(c.ServiceName, stgM.GetStorage(st), c.Verification, db, k8sm)
		if err != nil {
			return m, err
		}
		verifications = append(verifications, v)
		sts = append(sts, v.status)
	}

	if len(verifications) == 0 {
		return nil, errors.New("no verifications created")
	}

	metricsOnce.Do(func() {
		prometheus.MustRegister(NewMetricsCollector(sts))
	})
	return &Manager{
		cfg:           c,
		verifications: verifications,
	}, err
}

// Start a verification routine per storage
func (m *Manager) Start(ctx context.Context) {
	var eg errgroup.Group
	for _, v := range m.verifications {
		logger.Debugf("starting verification for %s backup", v.storage.GetStorageServiceName())
		eg.Go(func() error {
			err := v.Start(ctx)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		logger.Errorf("error verify backup: %s.", err.Error())
	}
}
