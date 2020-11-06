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
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	log "github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/route"
	"github.com/sapcc/maria-back-me-up/pkg/server"
	"github.com/sirupsen/logrus"
)

var opts config.Options

func init() {
	flag.StringVar(&opts.ConfigFilePath, "CONFIG_FILE", "./etc/config/config.yaml", "Path to the config file")
	flag.StringVar(&opts.ClientID, "OAUTH_CLIENT_ID", "id", "Oauth provider client id")
	flag.StringVar(&opts.ClientSecret, "OAUTH_CLIENT_SECRET", "secret", "Oauth provider client secret")
	flag.StringVar(&opts.CookieSecret, "OAUTH_COOKIE_SECRET", "secret", "OAuth session cookie secret")
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
		log.Fatal("cannot load config file", err.Error())
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGSTOP)

	m, err := backup.NewManager(cfg)
	if err != nil {
		log.Fatal("cannot create backup handler: ", err.Error())
	}

	api := route.InitAPI(m, opts)
	metrics := route.InitMetrics(m)

	s1 := server.NewServer(metrics)
	s2 := server.NewServer(api)
	go s1.Start(constants.PORT_METRICS)
	go s2.Start(constants.PORT)

	if err = m.Start(); err != nil {
		log.Fatal(err)
	}

	defer func() {
		signal.Stop(c)
	}()

	select {
	case <-c:
		s1.Stop(ctx)
		s2.Stop(ctx)
		m.Stop()
		cancel()
	case <-ctx.Done():
	}
}
