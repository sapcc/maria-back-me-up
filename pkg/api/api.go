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
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/websocket"
)

type jsonResponse struct {
	Time   string `json:"time"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

var logger *logrus.Entry

func init() {
	logger = log.WithFields(logrus.Fields{"component": "api"})
}

func getKeyPath(key string) string {
	s := strings.Split(key, "/")
	return fmt.Sprintf("Full Dump: %s", s[1])
}

func getVerifyBackupState(v storage.Backup, t time.Time, withErr bool) string {
	var verifyState string
	// no verify yet available
	if v.VerifySuccess == nil && v.VerifyFail == nil {
		return calcVerifyState(nil, withErr)
	}

	if v.VerifySuccess != nil && v.VerifyFail != nil {
		// if successful verify is the latest status...everything is green
		if v.VerifySuccess.Time.After(v.VerifyFail.Time) {
			if t.After(v.VerifySuccess.Time) {
				return calcVerifyState(nil, withErr)
			}
			return calcVerifyState(v.VerifySuccess, withErr)
		}
		// latest verify: fail
		if v.VerifyFail.Time.After(v.VerifySuccess.Time) {
			if t.After(v.VerifyFail.Time) {
				return calcVerifyState(nil, withErr)
			}
			// if backup is before a green verify, mark it as sucessful
			if t.Before(v.VerifySuccess.Time) {
				return calcVerifyState(v.VerifySuccess, withErr)
			}
			return calcVerifyState(v.VerifyFail, withErr)
		}
		return calcVerifyState(v.VerifyFail, withErr)
	}

	if v.VerifySuccess != nil {
		if t.After(v.VerifySuccess.Time) {
			return calcVerifyState(nil, withErr)
		}
		return calcVerifyState(v.VerifySuccess, withErr)
	}

	if v.VerifyFail != nil {
		if t.After(v.VerifyFail.Time) {
			return calcVerifyState(nil, withErr)
		}
		return calcVerifyState(v.VerifyFail, withErr)
	}

	return verifyState
}

var funcMap = template.FuncMap{
	"getKeyPath":           getKeyPath,
	"getVerifyBackupState": getVerifyBackupState,
}

// GetRoot renders the index.html
func GetRoot(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var tmpl = template.New("index.html").Funcs(funcMap)
		t, err := tmpl.ParseFiles(constants.INDEX)
		if err != nil {
			return fmt.Errorf("Error parsing index: %s", err.Error())
		}
		s := m.Storage.GetStorageServicesKeys()
		d := map[string]interface{}{
			"storages": s,
			"config":   m.GetConfig(),
		}
		return t.Execute(c.Response(), d)
	}
}

// GetBackup renders the backup.html
func GetBackup(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		s := c.QueryParam("storage")
		if err != nil {
			return sendJSONResponse(c, "Error parsing storage key", err.Error())
		}
		var tmpl = template.New("backup.html").Funcs(funcMap)
		t, err := tmpl.ParseFiles(constants.BACKUP)
		if err != nil {
			return sendJSONResponse(c, "Error parsing template file", err.Error())
		}
		var backups backupSlice
		backups, err = m.Storage.GetFullBackups(s)
		sort.Stable(backups)
		d := map[string]interface{}{
			"backups": backups,
			"service": m.GetConfig().ServiceName,
		}
		if err != nil {
			return sendJSONResponse(c, "Error fetching backup list", err.Error())
		}

		return t.Execute(c.Response(), d)
	}
}

// GetRestore renders the restore.html
func GetRestore(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		k := c.QueryParam("key")
		s := c.QueryParam("storage")
		if err != nil {
			return sendJSONResponse(c, "Error parsing storage key", err.Error())
		}
		var tmpl = template.New("restore.html").Funcs(funcMap)
		t, err := tmpl.ParseFiles(constants.RESTORE)
		if err != nil {
			return sendJSONResponse(c, "Error parsing templating", err.Error())
		}
		var incBackups incBackupSlice
		incBackups, err = m.Storage.GetIncBackupsFromDump(s, k)
		sort.Stable(incBackups)
		d := map[string]interface{}{
			"incBackups": incBackups,
			"service":    m.GetConfig().ServiceName,
		}
		if err != nil {
			return sendJSONResponse(c, "Error fetching backup list", err.Error())
		}
		return t.Execute(c.Response(), d)
	}
}

// PostRestoreDownload handles restore download requests
func PostRestoreDownload(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		os.RemoveAll(path.Join(m.GetConfig().Backup.RestoreDir, m.GetConfig().ServiceName))
		res := c.Response()
		params, err := c.FormParams()
		res.WriteHeader(http.StatusOK)
		res.Header().Set("Content-Disposition", "attachment; filename=backup.tar")
		p := params["backup"][0]
		if p == "" {
			return sendJSONResponse(c, "Error parsing backup param", err.Error())
		}
		path, binlog := path.Split(p)
		st := params["storage"][0]
		if st == "" {
			return sendJSONResponse(c, "Error parsing storage param", err.Error())
		}
		backupPath, err := m.Storage.DownloadBackupWithLogPosition(st, path, binlog)
		if err != nil {
			return sendJSONResponse(c, "Error downloading backup", err.Error())
		}
		defer os.RemoveAll(backupPath)
		pr, err := storage.ZipFolderPath(backupPath)
		if err != nil {
			return sendJSONResponse(c, "Error downloading backup", err.Error())
		}
		err = c.Stream(http.StatusOK, "application/x-gzip", pr)
		if err != nil {
			return sendJSONResponse(c, "Error streaming backup file", err.Error())
		}
		return
	}
}

// PostRestore handles backup restore requests
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
		if err = checkHeaderUserData(c, m.GetConfig().Backup.OAuth); err != nil {
			return
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
		backupPath, err := m.Storage.DownloadBackupWithLogPosition(st, path, binlog)
		if err != nil {
			return sendJSONResponse(c, "Error downloading backup", err.Error())
		}

		_ = sendJSONResponse(c, "Stopping backup...", "")
		m.Stop()
		time.Sleep(time.Duration(1 * time.Second))

		s, err := m.Db.HealthCheck()
		if err != nil || !s.Ok {
			_ = sendJSONResponse(c, "Database not healthy. Trying to restore!", "")
		}
		_ = sendJSONResponse(c, "Starting restore...", "")

		if err = m.Restore(backupPath); err != nil {
			_ = sendJSONResponse(c, "Error during restore!", err.Error())
		}
		var eg errgroup.Group
		eg.Go(m.Start)
		if err = eg.Wait(); err != nil {
			_ = sendJSONResponse(c, "Error starting a backup cycle", err.Error())
		}

		return sendJSONResponse(c, "Restore finished!", "")
	}
}

// GetBackupStatus handles backup status requests
func GetBackupStatus(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		websocket.Handler(func(ws *websocket.Conn) {
			defer ws.Close()
			ticker := time.NewTicker(10 * time.Second)
			for {
				d := map[string]interface{}{
					"active": m.GetBackupActive(),
					"health": m.GetHealthStatus(),
				}
				s, _ := json.Marshal(d)
				err := websocket.Message.Send(ws, string(s))
				if err != nil {
					log.Debug("cant write status to websocket")
					ticker.Stop()
					return
				}
				select {
				case <-c.Request().Context().Done():
					return
				case <-ticker.C:
					continue
				}
			}
		}).ServeHTTP(c.Response(), c.Request())
		return
	}
}

// StartStopBackup handles stop backup requests
func StartStopBackup(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		if c.Path() == "/api/backup/stop" {
			ctx := m.Stop()
			<-ctx.Done()
			return c.JSON(http.StatusOK, "Stopped")

		} else if c.Path() == "/api/backup/start" {
			var eg errgroup.Group
			eg.Go(m.Start)
			if err = eg.Wait(); err != nil {
				c.Error(err)
				return
			}
			return c.JSON(http.StatusOK, "Started")
		}
		return
	}
}

// CreateIncBackup handles create inc backup requests
func CreateIncBackup(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		if err := m.CreateIncBackup(); err != nil {
			return c.JSON(http.StatusInternalServerError, "Cannot trigger an incremental backup: "+err.Error())
		}
		return c.JSON(http.StatusOK, "Ok")
	}
}

// GetReadiness handles readiness pod requests
func GetReadiness(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		if c.Path() == "/health/readiness" {
			m.Health.Lock()
			defer m.Health.Unlock()
			if m.Health.Ready {
				if m.GetConfig().SideCar != nil && !*m.GetConfig().SideCar {
					return c.String(http.StatusOK, "Backup in progress")
				}
				return c.String(http.StatusOK, "READY")
			}
			if m.GetConfig().SideCar != nil && !*m.GetConfig().SideCar {
				return c.String(http.StatusOK, "Restore in progress")
			}
			return c.String(http.StatusInternalServerError, "Restore in progress")
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

func checkHeaderUserData(c echo.Context, cfg config.OAuth) (err error) {
	if cfg.Middleware {
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
		log.Warn("restore triggered by user: " + user)
	}
	if cfg.SAPID {
		user := c.Request().Header.Get("X-Auth-Request-User")
		if user == "" {
			return sendJSONResponse(c, "Cannot read user info", "")
		}
		log.Warn("restore triggered by user: " + user)
	}
	return
}
