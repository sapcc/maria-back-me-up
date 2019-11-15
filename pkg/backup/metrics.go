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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
)

type (
	updateStatus struct {
		sync.RWMutex `yaml:"-"`
		up           int
		incBackup    time.Time
		fullBackup   time.Time
		VerifyBackup int `yaml:"verify_backup"`
		VerifyTables int `yaml:"verify_tables"`
	}

	MetricsCollector struct {
		upGauge   *prometheus.Desc
		backup    *prometheus.Desc
		cfg       config.MariaDB
		updateSts *updateStatus
	}
)

var (
	incBackupUp  int
	fullBackupUp int
)

func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.upGauge
}

func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	c.updateSts.RLock()
	defer c.updateSts.RUnlock()
	ch <- prometheus.MustNewConstMetric(
		c.backup,
		prometheus.GaugeValue,
		float64(c.updateSts.fullBackup.Unix()),
		"full_backup",
	)
	ch <- prometheus.MustNewConstMetric(
		c.backup,
		prometheus.GaugeValue,
		float64(c.updateSts.incBackup.Unix()),
		"inc_backup",
	)
	ch <- prometheus.MustNewConstMetric(
		c.backup,
		prometheus.GaugeValue,
		float64(c.updateSts.VerifyBackup),
		"verify_backup",
	)
	ch <- prometheus.MustNewConstMetric(
		c.backup,
		prometheus.GaugeValue,
		float64(c.updateSts.VerifyTables),
		"verify_tables",
	)
	ch <- prometheus.MustNewConstMetric(
		c.upGauge,
		prometheus.GaugeValue,
		float64(c.updateSts.up),
	)
}

func NewMetricsCollector(c config.MariaDB, u *updateStatus) *MetricsCollector {
	m := MetricsCollector{
		updateSts: u,
		cfg:       c,
		upGauge: prometheus.NewDesc(
			"backup_running",
			"Shows if mariadb backup is running",
			nil,
			prometheus.Labels{}),
		backup: prometheus.NewDesc(
			"maria_backup_status",
			"backup status of mariadb",
			[]string{"kind"},
			prometheus.Labels{}),
	}

	return &m
}
