// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package verification

import (
	"github.com/prometheus/client_golang/prometheus"
)

type (
	// MetricsCollector struct for prometheus metrics
	MetricsCollector struct {
		verify *prometheus.Desc
		status []*Status
	}
)

// Describe implements the exporter interface function
func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.verify
}

// Collect implements the exporter interface function
func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range c.status {
		s.RLock()
		defer s.RUnlock()
		ch <- prometheus.MustNewConstMetric(
			c.verify,
			prometheus.GaugeValue,
			float64(s.VerifyRestore),
			"verify_restore",
			s.BackupService,
			s.StorageService,
		)
		ch <- prometheus.MustNewConstMetric(
			c.verify,
			prometheus.GaugeValue,
			float64(s.VerifyDiff),
			"verify_diff",
			s.BackupService,
			s.StorageService,
		)
		ch <- prometheus.MustNewConstMetric(
			c.verify,
			prometheus.GaugeValue,
			float64(s.VerifyChecksum),
			"verify_checksum",
			s.BackupService,
			s.StorageService,
		)
	}

}

// NewMetricsCollector creates a metricsCollector instance
func NewMetricsCollector(s []*Status) *MetricsCollector {
	m := MetricsCollector{
		status: s,
		verify: prometheus.NewDesc(
			"maria_backup_verify_status",
			"verify status of mariadb",
			[]string{"kind", "service", "storage"},
			prometheus.Labels{}),
	}

	return &m
}
