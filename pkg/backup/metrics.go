/**
 * Copyright 2019 SAP SE
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

package backup

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	// UpdateStatus holds current status of backups
	UpdateStatus struct {
		sync.RWMutex `yaml:"-"`
		IncBackup    map[string]int
		FullBackup   map[string]int
		Restarts     prometheus.Counter
	}

	// MetricsCollector collects metrics fort the backup and verification status
	MetricsCollector struct {
		backup    *prometheus.Desc
		updateSts *UpdateStatus
	}
)

func NewUpdateStatus() UpdateStatus {
	return UpdateStatus{
		FullBackup: make(map[string]int),
		IncBackup:  make(map[string]int),
		Restarts:   prometheus.NewCounter(prometheus.CounterOpts{Name: "maria_backup_restarts", Help: "Number of times the backup process was restarted"}),
	}
}

// Describe implements the exporter interface function
func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.backup
}

// Collect implements the exporter interface function
func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	c.updateSts.RLock()
	defer c.updateSts.RUnlock()
	for storage, up := range c.updateSts.FullBackup {
		ch <- prometheus.MustNewConstMetric(
			c.backup,
			prometheus.GaugeValue,
			float64(up),
			"full_backup",
			storage,
		)
	}
	for storage, up := range c.updateSts.IncBackup {
		ch <- prometheus.MustNewConstMetric(
			c.backup,
			prometheus.GaugeValue,
			float64(up),
			"inc_backup",
			storage,
		)
	}
	ch <- c.updateSts.Restarts
}

// NewMetricsCollector create a prometheus collector instance
func NewMetricsCollector(u *UpdateStatus) *MetricsCollector {
	m := MetricsCollector{
		updateSts: u,
		backup: prometheus.NewDesc(
			"maria_backup_status",
			"backup status of mariadb",
			[]string{"kind", "storage"},
			prometheus.Labels{}),
	}

	return &m
}
