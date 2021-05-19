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
