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
	"strings"
	"time"

	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/maria"
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

func getKeyPath(key string) string {
	s := strings.Split(key, "/")
	return fmt.Sprintf("Full Dump: %s", s[1])
}

func getServiceName(key string) string {
	s := strings.Split(key, "/")
	return fmt.Sprintf("Service: %s", s[0])
}

func getVerifyBackupState(v []storage.Verify, t time.Time, err bool) string {
	var duration time.Duration
	duration = time.Duration(1000 * time.Hour)
	verifyState := "#6c757d" // grey
	verifyError := "Verfication not completed..."
	for _, k := range v {
		if t.Before(k.Time) {
			if k.Time.Sub(t) < duration {
				if k.VerifyRestore == 1 && k.VerifyDiff == 1 {
					verifyState = "#ffc107" // orange
					if k.VerifyError != "" {
						verifyError = k.VerifyError
					} else {
						verifyError = "Restor + MySQL Diff successful! Table checksum was not executed."
					}
				}
				if k.VerifyChecksum == 1 {
					verifyState = "#28a745" // green
					verifyError = "MySQL Checksum successful"
				}
				if k.VerifyRestore == 0 || k.VerifyDiff == 0 {
					verifyState = "#dc3545" // red
					if k.VerifyError != "" {
						verifyError = k.VerifyError
					}
				}
			}
			duration = k.Time.Sub(t)
		}
	}
	if err {
		return verifyError
	}
	return verifyState
}

var funcMap = template.FuncMap{
	"getKeyPath":           getKeyPath,
	"getVerifyBackupState": getVerifyBackupState,
	"getServiceName":       getServiceName,
}

func GetBackup(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		s := c.QueryParam("storage")
		if err != nil {
			return sendJSONResponse(c, "Error parsing storage key", err.Error())
		}
		var tmpl = template.New("backup.html").Funcs(funcMap)
		t, err := tmpl.ParseFiles(constants.BACKUP)
		backups, err := m.Storage.ListFullBackups(s)
		if err != nil {
			return sendJSONResponse(c, "Error fetching backup list", err.Error())
		}

		return t.Execute(c.Response(), backups)
	}
}

func GetRoot(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var tmpl = template.New("index.html").Funcs(funcMap)
		t, err := tmpl.ParseFiles(constants.INDEX)
		if err != nil {
			return fmt.Errorf("Error parsing index: %s", err.Error())
		}
		s := m.Storage.GetStorageServices()
		return t.Execute(c.Response(), s)
	}
}

func GetRestore(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		k := c.QueryParam("key")
		s := c.QueryParam("storage")
		if err != nil {
			return sendJSONResponse(c, "Error parsing storage key", err.Error())
		}
		var tmpl = template.New("restore.html").Funcs(funcMap)
		t, err := tmpl.ParseFiles(constants.RESTORE)
		incBackups, err := m.Storage.ListIncBackupsFor(s, k)
		if err != nil {
			return sendJSONResponse(c, "Error fetching backup list", err.Error())
		}
		return t.Execute(c.Response(), incBackups)
	}
}

func PostLatestRestore(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		c.Response().WriteHeader(http.StatusOK)

		p, err := m.Storage.DownloadLatestBackup("")
		if err != nil {
			return sendJSONResponse(c, "Restore Error", err.Error())
		}
		m.Stop()
		if err = m.Restore(p); err != nil {
			sendJSONResponse(c, "Error during restore!", err.Error())
		}
		go m.Start()
		return sendJSONResponse(c, "Restore finished!", "")
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
			return sendJSONResponse(c, "No Backup selected", "")
		}
		if len(params["storage"]) == 0 {
			return sendJSONResponse(c, "No Storage selected", "")
		}
		if m.GetConfig().OAuth.Enabled {
			session, err := store.Get(c.Request(), sessionCookieName)
			if err != nil {
				return sendJSONResponse(c, "Cannot read session cookie", err.Error())
			}

			if session.Values["user"] == nil {
				return sendJSONResponse(c, "No session user provided", "")
			}
			user := session.Values["user"].(string)
			if user == "" {
				return sendJSONResponse(c, "Cannot read user info", "")
			}
			log.Info("RESTORE TRIGGERED BY USER: " + user)
		}
		p := params["backup"][0]
		if p == "" {
			return sendJSONResponse(c, "Error parsing backup param", err.Error())
		}
		path, binlog := path.Split(p)
		st := params["storage"][0]
		if st == "" {
			return sendJSONResponse(c, "Error parsing storage param", err.Error())
		}
		backupPath, err := m.Storage.DownloadBackupFrom(st, path, binlog)

		sendJSONResponse(c, "Stopping backup...", "")
		m.Stop()
		time.Sleep(time.Duration(1 * time.Second))

		s, err := maria.HealthCheck(m.GetConfig().MariaDB)
		if err != nil || !s.Ok {
			sendJSONResponse(c, "Database not healthy. Trying to restore!", "")
		}
		sendJSONResponse(c, "Starting restore...", "")

		if err = m.Restore(backupPath); err != nil {
			sendJSONResponse(c, "Error during restore!", err.Error())
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
