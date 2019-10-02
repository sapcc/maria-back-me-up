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
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/health"
)

type MetricsCollector struct {
	upGauge *prometheus.Desc
	targets *prometheus.Desc
	mariadb *health.Maria
}

func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.upGauge
}

func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	s, err := c.mariadb.Check()
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
	for key, value := range c.mariadb.Status.Details {
		ch <- prometheus.MustNewConstMetric(
			c.targets,
			prometheus.GaugeValue,
			float64(value),
			key,
		)
	}
}

func NewMetricsCollector(c config.MariaDB) *MetricsCollector {
	return &MetricsCollector{
		mariadb: health.NewMaria(c),
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
	}
}
