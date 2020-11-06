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
	updateStatus struct {
		sync.RWMutex `yaml:"-"`
		IncBackup    map[string]int
		FullBackup   map[string]int
	}

	MetricsCollector struct {
		backup    *prometheus.Desc
		verify    *prometheus.Desc
		updateSts *updateStatus
	}
)

func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.backup
}

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
}

func NewMetricsCollector(u *updateStatus) *MetricsCollector {
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
