// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package route

import (
	"github.com/labstack/echo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sapcc/maria-back-me-up/pkg/api"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
)

// InitAPI inits the api routes
func InitAPI(m *backup.Manager, opts config.Options) (*echo.Echo, error) {
	e := echo.New()

	if m.GetConfig().Backup.OAuth.Middleware {
		if err := api.InitOAuth(m, opts); err != nil {
			return nil, err
		}
		e.GET("auth/callback", api.HandleOAuth2Callback(opts))
	}

	e.Static("/static", "static")

	i := e.Group("/")
	i.Use(api.Oauth(m.GetConfig().Backup.OAuth, opts), api.Restore(m))
	i.GET("", api.GetRoot(m))
	i.GET("backup", api.GetBackup(m))
	i.GET("restore", api.GetRestore(m))
	i.POST("restore", api.PostRestore(m))

	gb := e.Group("/api")
	gb.Use(api.Oauth(m.GetConfig().Backup.OAuth, opts), api.Restore(m))
	gb.GET("/backup/status", api.GetBackupStatus(m))
	gb.GET("/backup/stop", api.StartStopBackup(m))
	gb.GET("/backup/start", api.StartStopBackup(m))
	gb.GET("/backup/inc/create", api.CreateIncBackup(m))
	gb.POST("/restore/download", api.PostRestoreDownload(m))

	return e, nil
}

// InitMetrics inits the metrics routes
func InitMetrics(m *backup.Manager) *echo.Echo {
	e := echo.New()
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	gh := e.Group("/health")
	gh.GET("/readiness", api.GetReadiness(m))
	return e
}

// InitVerificationMetrics inits the verficiation metrics routes
func InitVerificationMetrics() *echo.Echo {
	e := echo.New()
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	return e
}
