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

	"github.com/namsral/flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	log "github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/metrics"
	"github.com/sapcc/maria-back-me-up/pkg/route"
	"github.com/sapcc/maria-back-me-up/pkg/server"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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
	cfg, err := config.GetConfig(opts)
	if err != nil {
		log.Fatal("cannot load config file")
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGSTOP)

	m, err := backup.NewManager(cfg)
	if err != nil {
		log.Fatal("cannot create backup handler: ", err.Error())
	}

	prometheus.MustRegister(metrics.NewMetricsCollector(cfg.MariaDB))

	//_ = restore.NewRestore(cfg, s3, b)
	//if err != nil {
	//	log.Fatal("cannot create restore handler", err.Error())
	//}

	//v, err := backup.NewVerifier("test")
	//v.CreateMariaService()
	e := route.Init(m)
	var eg errgroup.Group
	s := server.NewServer(e)
	eg.Go(func() error {
		return s.Start()
	})
	eg.Go(func() error {
		return m.StartBackup()
	})
	eg.Go(func() error {
		return m.StartVerifyBackup(ctx)
	})

	go func() {
		if err = eg.Wait(); err != nil {
			log.Fatal(err)
		}
	}()

	defer func() {
		signal.Stop(c)
	}()

	select {
	case <-c:
		s.Stop(ctx)
		m.StopBackup()
		cancel()
	case <-ctx.Done():
	}
}
