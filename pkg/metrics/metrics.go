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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
)

type MetricsCollector struct {
	upGauge    *prometheus.Desc
	targets    *prometheus.Desc
	backup     *prometheus.Desc
	errorCount *prometheus.Desc
	cfg        config.MariaDB
}

var (
	errors       []string
	incBackupUp  int
	fullBackupUp int
	results      []backup.Update
)

func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.upGauge
}

func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	s, err := backup.HealthCheck(c.cfg)
	if err == nil || !s.Ok {
		ch <- prometheus.MustNewConstMetric(
			c.upGauge,
			prometheus.GaugeValue,
			float64(0),
		)
	} else {
		ch <- prometheus.MustNewConstMetric(
			c.upGauge,
			prometheus.GaugeValue,
			float64(1),
		)
	}
	for key, value := range s.Details {
		ch <- prometheus.MustNewConstMetric(
			c.targets,
			prometheus.GaugeValue,
			float64(value),
			key,
		)
	}
	ch <- prometheus.MustNewConstMetric(
		c.errorCount,
		prometheus.GaugeValue,
		float64(len(errors)),
	)
	ch <- prometheus.MustNewConstMetric(
		c.backup,
		prometheus.GaugeValue,
		float64(fullBackupUp),
		"full_backup",
	)
	ch <- prometheus.MustNewConstMetric(
		c.backup,
		prometheus.GaugeValue,
		float64(incBackupUp),
		"inc_backup",
	)
}

/*
func (c *MetricsCollector) startUpdateHandler() {
	for {
		select {
		case u, ok := <-c.updatec:
			if !ok {
				return
			}
			up := 1
			for r := range u.Backup {
				if u.Err != nil {
					errors = append(errors, u.Err.Error())
					up = 0
				}
				if r == 1 {
					incBackupUp = up
				} else {
					fullBackupUp = up
				}
			}
		}
	}
}
*/
func NewMetricsCollector(c config.MariaDB) *MetricsCollector {
	m := MetricsCollector{
		cfg: c,
		targets: prometheus.NewDesc(
			"maria_health_status",
			"Health status of mariadb",
			[]string{"module"},
			prometheus.Labels{}),
		upGauge: prometheus.NewDesc(
			"maria_up",
			"Shows if mariadb is running",
			nil,
			prometheus.Labels{}),
		errorCount: prometheus.NewDesc(
			"error_count",
			"Shows number of errors accured in the past",
			nil,
			prometheus.Labels{}),
		backup: prometheus.NewDesc(
			"maria_backup_status",
			"backup status of mariadb",
			[]string{"kind"},
			prometheus.Labels{}),
	}
	//go m.startUpdateHandler()
	return &m
}
