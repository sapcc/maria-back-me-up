// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

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

// NewUpdateStatus returns a UpdateStatus with an initialized counter metric
func NewUpdateStatus() UpdateStatus {
	return UpdateStatus{
		FullBackup: make(map[string]int),
		IncBackup:  make(map[string]int),
		Restarts:   prometheus.NewCounter(prometheus.CounterOpts{Name: "maria_backup_errors", Help: "Number of times the backup process was restarted due to an error"}),
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
