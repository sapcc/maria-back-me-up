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
	"fmt"
	"html/template"
	"net/http"
	"path"
	"time"

	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

type TemplateRenderer struct {
	templates *template.Template
}

type jsonResponse struct {
	Time   string `json:"time"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func prettify(ts time.Time) string {
	return "link" + ts.Format("01-02-2006_15_04_05")
}

func verifyBackup(v []storage.Verify, t time.Time) string {
	var duration time.Duration
	for _, k := range v {
		if t.Before(k.Time) {
			if k.Time.Sub(t) < duration {
				if k.Tables == 1 {
					return "green"
				}
				if k.Backup == 1 {
					return "orange"
				}
			}
			duration = k.Time.Sub(t)
		}
	}
	return "grey"
}

var funcMap = template.FuncMap{
	"prettify":     prettify,
	"verifyBackup": verifyBackup,
}

func GetRoot(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var tmpl = template.New("index.html").Funcs(funcMap)
		t, err := tmpl.ParseFiles(constants.INDEX)
		backups, err := m.Storage.GetAllBackups()
		if err != nil {
			return fmt.Errorf("Error fetching backup list: %s", err.Error())
		}

		return t.Execute(c.Response(), backups)
	}
}

func PostRestore(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		c.Response().WriteHeader(http.StatusOK)
		params, err := c.FormParams()
		if err != nil {
			return
		}

		if len(params["backup"]) == 0 {
			return fmt.Errorf("No Backup selected")
		}
		p := params["backup"][0]
		d, f := path.Split(p)
		backupPath, err := m.Storage.DownloadBackupFrom(d, f)
		if err != nil {
			return sendJSONResponse(c, "Restore Error", err.Error())
		}
		if err = sendJSONResponse(c, "Stopping backup...", ""); err != nil {
			return
		}
		m.Stop()
		time.Sleep(time.Duration(1 * time.Second))
		s, err := backup.HealthCheck(m.GetConfig().MariaDB)
		if err != nil || !s.Ok {
			if err = sendJSONResponse(c, "Database not healthy. Trying hard restore!", ""); err != nil {
				return
			}
			if err = sendJSONResponse(c, "Starting hard restore...", ""); err != nil {
				return
			}
			if err = m.Restore(backupPath, constants.HARDRESTORE); err != nil {
				return sendJSONResponse(c, "Hard Restore Error!", err.Error())
			}
		} else {
			if err = sendJSONResponse(c, "Starting restore...", ""); err != nil {
				return
			}

			if err = m.Restore(backupPath, constants.SOFTRESTORE); err != nil {
				return sendJSONResponse(c, "Restore Error!", err.Error())
			}
		}

		go m.Start()
		return sendJSONResponse(c, "Restore finished!", "")
	}
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

func GetReadiness(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		if c.Path() == "/health/readiness" {
			m.Health.Lock()
			defer m.Health.Unlock()
			if m.Health.Ready {
				return c.String(http.StatusOK, "READY")
			}
			return c.String(http.StatusInternalServerError, "RESTORE IN PROCESS")
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
