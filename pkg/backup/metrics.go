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
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
)

type (
	updateStatus struct {
		sync.RWMutex `yaml:"-"`
		incBackup    map[string]int
		fullBackup   map[string]int
		VerifyBackup int    `yaml:"verify_backup"`
		VerifyTables int    `yaml:"verify_tables"`
		VerifyError  string `yaml:"verify_error"`
	}

	MetricsCollector struct {
		backup    *prometheus.Desc
		verify    *prometheus.Desc
		cfg       config.MariaDB
		updateSts *updateStatus
	}
)

func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.backup
}

func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	c.updateSts.RLock()
	defer c.updateSts.RUnlock()
	for storage, up := range c.updateSts.fullBackup {
		ch <- prometheus.MustNewConstMetric(
			c.backup,
			prometheus.GaugeValue,
			float64(up),
			"full_backup",
			storage,
		)
	}
	for storage, up := range c.updateSts.incBackup {
		ch <- prometheus.MustNewConstMetric(
			c.backup,
			prometheus.GaugeValue,
			float64(up),
			"inc_backup",
			storage,
		)
	}
	ch <- prometheus.MustNewConstMetric(
		c.verify,
		prometheus.GaugeValue,
		float64(c.updateSts.VerifyBackup),
		"verify_backup",
	)
	ch <- prometheus.MustNewConstMetric(
		c.verify,
		prometheus.GaugeValue,
		float64(c.updateSts.VerifyTables),
		"verify_tables",
	)
}

func NewMetricsCollector(c config.MariaDB, u *updateStatus) *MetricsCollector {
	fmt.Println(c, u)
	m := MetricsCollector{
		updateSts: u,
		cfg:       c,
		backup: prometheus.NewDesc(
			"maria_backup_status",
			"backup status of mariadb",
			[]string{"kind", "storage"},
			prometheus.Labels{}),
		verify: prometheus.NewDesc(
			"maria_verify_status",
			"verify status of mariadb",
			[]string{"kind"},
			prometheus.Labels{}),
	}

	return &m
}
