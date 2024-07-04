/**
 * Copyright 2024 SAP SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package verification

import (
	"context"
	"fmt"

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
	logger *logrus.Entry
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
		return nil, fmt.Errorf("no verifications created")
	}

	prometheus.MustRegister(NewMetricsCollector(sts))
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
