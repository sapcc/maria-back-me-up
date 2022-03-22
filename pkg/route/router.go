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

	if m.GetConfig().Backup.OAuth.Enabled {
		if err := api.InitOAuth(m, opts); err != nil {
			return nil, err
		}
		e.GET("auth/callback", api.HandleOAuth2Callback(opts))
	} else {

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
