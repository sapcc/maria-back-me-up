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

package restore

import (
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/health"
	"github.com/sapcc/maria-back-me-up/pkg/writer"
)

type Maria struct {
	cfg    config.Config
	health *health.Maria
	docker *client.Client
	writer *writer.S3
}

func NewMaria(c config.Config, e *echo.Echo) *Maria {
	s3, _ := writer.NewS3(c.S3)
	m := &Maria{
		cfg:    c,
		writer: s3,
	}
	e.GET("/recover", m.restore)

	return m
}

func (m *Maria) restore(c echo.Context) error {
	ts := c.QueryParam("time")

	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return c.String(http.StatusBadRequest, "Time has to be RFC3339 conform "+err.Error())
	}

	err = m.restoreBackup(t)
	if err != nil {
		c.String(http.StatusNotFound, err.Error())
	}
	return err
}

func prepareIncBackup(cfg config.Config) {
}

func (m *Maria) restoreBackup(t time.Time) (err error) {
	p, err := m.writer.GetBackupByTimestamp(t)
	if err != nil {
		return
	}
	untar := exec.Command("tar", "-zxvf", path.Join(p, "dump.tar"))
	untar.CombinedOutput()
	myloaderCmd := exec.Command(
		"myloader",
		"--port="+strconv.Itoa(m.cfg.MariaDB.Port),
		"--host="+m.cfg.MariaDB.Host,
		"--user="+m.cfg.MariaDB.User,
		"--password="+m.cfg.MariaDB.Password,
		"--directory="+path.Join(p, "dump"),
		"--overwrite-tables",
	)

	myloaderCmd.CombinedOutput()
	return
}

func IsEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}
