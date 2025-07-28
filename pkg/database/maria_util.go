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

package database

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	dberror "github.com/sapcc/maria-back-me-up/pkg/error"
)

func mariaHealthCheck(c config.DatabaseConfig) (status Status, err error) {
	status = Status{
		Ok:      true,
		Details: make(map[string]string, 0),
	}

	dbs := strings.Join(c.Databases, " -B ")
	args := strings.Split(fmt.Sprintf("-c -B -q %s -u%s -p%s -h%s -P%s", dbs, c.User, c.Password, c.Host, strconv.Itoa(c.Port)), " ")
	args = append(args, "--skip-write-binlog")
	args = append(args, "--skip-ssl")
	cmd := exec.Command(
		"mariadb-check",
		args...,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		status.Ok = false
		if strings.Contains(string(out), "1049") {
			return status, &dberror.DatabaseMissingError{}
		}
		if strings.Contains(string(out), "2002") {
			return status, &dberror.DatabaseConnectionError{}
		}

		return status, fmt.Errorf("mariadb-check failed with %s", string(out))
	}
	outa := strings.Fields(string(out))
	if len(outa) == 0 {
		return status, &dberror.DatabaseNoTablesError{}
	}
	for i := 0; i < len(outa)-1; i++ {
		if i%2 == 0 {
			if outa[i+1] != "OK" {
				status.Ok = false
				status.Details[outa[i]] = outa[i+1]
			}
		}
	}

	return
}

// purgeBinlogBefore purges all binlog files which are older than `minutes`
func purgeBinlogsBefore(c config.DatabaseConfig, minutes int) (err error) {
	conn, err := client.Connect(fmt.Sprintf("%s:%s", c.Host, strconv.Itoa(c.Port)), c.User, c.Password, "")
	if err != nil {
		return
	}

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection: %v", err)
		}
	}()

	if err = conn.Ping(); err != nil {
		return
	}

	result, err := conn.Execute("SELECT NOW() FROM DUAL;")
	if err != nil {
		return
	}

	serverTimeString, err := result.GetString(0, 0)
	if err != nil {
		return
	}

	serverTime, err := time.Parse("2006-01-02 15:04:05", serverTimeString)
	if err != nil {
		return
	}
	purgeBeforeTimeString := serverTime.Add(-time.Duration(minutes) * time.Minute).Format("2006-01-02 15:04:05")

	_, err = conn.Execute(fmt.Sprintf("PURGE BINARY LOGS BEFORE '%s'", purgeBeforeTimeString))
	if err != nil {
		return
	}
	return
}

func purgeBinlogsTo(c config.DatabaseConfig, logName string) (err error) {
	conn, cerr := client.Connect(fmt.Sprintf("%s:%s", c.Host, strconv.Itoa(c.Port)), c.User, c.Password, "")
	if cerr != nil {
		return
	}

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection: %v", err)
		}
	}()

	if err = conn.Ping(); err != nil {
		return
	}

	_, err = conn.Execute(fmt.Sprintf("PURGE BINARY LOGS TO '%s'", logName))
	if err != nil {
		return
	}
	return
}

func resetSlave(c config.DatabaseConfig) (err error) {
	conn, err := client.Connect(fmt.Sprintf("%s:%s", c.Host, strconv.Itoa(c.Port)), c.User, c.Password, "")
	if err != nil {
		return fmt.Errorf("connection for slave reset failed: %s", err.Error())
	}

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection: %v", err)
		}
	}()

	if err = conn.Ping(); err != nil {
		return
	}

	_, err = conn.Execute("RESET SLAVE")
	if err != nil {
		return
	}
	return
}
