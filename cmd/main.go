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

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/labstack/echo"
	"github.com/namsral/flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/restore"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	log "github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/metrics"
	"github.com/sirupsen/logrus"
)

var opts config.Options

func init() {
	flag.StringVar(&opts.ConfigFilePath, "CONFIG_FILE", "./etc/config/config.yaml", "Path to the config file")
	flag.Parse()
	log.SetFormatter(
		&logrus.TextFormatter{
			DisableColors: false,
			FullTimestamp: true,
		})
	log.SetLevel(logrus.Level(5))
}

func main() {
	// Echo instance
	e := echo.New()
	cfg, err := config.GetConfig(opts)
	if err != nil {
		log.Fatal("cannot load config file")
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGSTOP)

	u := make(chan []string, 50)
	errCh := make(chan error, 0)
	m := backup.NewMaria(cfg)

	prometheus.MustRegister(metrics.NewMetricsCollector(cfg.MariaDB))
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	restore.NewMaria(cfg, e)

	go m.RunBackup(ctx, u, errCh)
	// Start server
	go func() {
		if err := e.Start(":1323"); err != nil {
			e.Logger.Info("shutting down the server")
		}
	}()

	defer func() {
		signal.Stop(c)
	}()

	select {
	case <-c:
		if err := e.Shutdown(ctx); err != nil {
			e.Logger.Fatal(err)
		}
		cancel()
	case <-ctx.Done():
	}
}
