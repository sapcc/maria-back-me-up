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

package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
)

type jsonResponse struct {
	Time   string `json:"time"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func GetGackup(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		if c.Path() == "/backup/stop" {
			m.Stop()
			return c.JSON(http.StatusOK, "Stopped")
		} else if c.Path() == "/backup/start" {
			go m.Start()
			return c.JSON(http.StatusOK, "Started")
		}
		return
	}
}

func GetRestore(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var backupPath string
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

		ts := c.QueryParam("time")
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return c.JSON(http.StatusBadRequest, "Time has to be RFC3339 conform "+err.Error())
		}

		s, err := backup.HealthCheck(m.GetConfig().MariaDB)
		if err != nil || !s.Ok {
			return c.JSON(http.StatusInternalServerError, "Cannot do restore, mariadb is not healthy")
		}
		backupPath, err = m.Storage.GetBackupByTimestamp(t)
		if err != nil {
			return sendJSONResponse(c, "Restore Error", err.Error())
		}

		if err = sendJSONResponse(c, "Stopping backup...", ""); err != nil {
			return
		}
		m.Stop()
		if c.Path() == "/restore/soft" {
			if err = sendJSONResponse(c, "Starting restore...", ""); err != nil {
				return
			}

			if err = m.RestoreBackup(backupPath); err != nil {
				return sendJSONResponse(c, "Restore Error!", err.Error())
			}

			return sendJSONResponse(c, "Restore finished!", "")
		} else if c.Path() == "/restore/hard" {
			if err = sendJSONResponse(c, "Starting hard restore...", ""); err != nil {
				return
			}
			if err = m.HardRestoreBackup(backupPath); err != nil {
				return sendJSONResponse(c, "Hard Restore Error!", err.Error())
			}
			if err = sendJSONResponse(c, "Hard Restore finished!", ""); err != nil {
				return
			}
		}
		go m.Start()
		return nil
	}
}

func GetReadiness(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		if c.Path() == "/health/readiness" {
			m.Health.Lock()
			defer m.Health.Unlock()
			if m.Health.Ready {
				return c.String(http.StatusOK, "READY")
			}
			return c.String(http.StatusInternalServerError, "NOT READY")
		}
		return
	}
}

func sendJSONResponse(c echo.Context, s string, errs string) (err error) {
	st := jsonResponse{
		Time:   time.Now().String(),
		Status: s,
		Error:  errs,
	}
	if err := json.NewEncoder(c.Response()).Encode(st); err != nil {
		return err
	}
	c.Response().Flush()
	return
}
