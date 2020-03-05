package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/route"
	"github.com/sapcc/maria-back-me-up/pkg/server"
	"github.com/sapcc/maria-back-me-up/pkg/verification"
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

	v, err := verification.NewManager(cfg)
	if err != nil {
		log.Fatal("cannot create verification manager: ", err.Error())
	}

	metrics := route.InitVerificationMetrics()

	s := server.NewServer(metrics)
	go s.Start(constants.PORT_METRICS)

	v.Start(ctx)

	defer func() {
		signal.Stop(c)
	}()

	select {
	case <-c:
		s.Stop(ctx)
		cancel()
	case <-ctx.Done():
	}
}
