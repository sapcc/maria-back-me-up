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
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	dberror "github.com/sapcc/maria-back-me-up/pkg/error"
	"github.com/siddontang/go-mysql/client"
	"k8s.io/apimachinery/pkg/util/wait"
)

func mariaHealthCheck(c config.DatabaseConfig) (status Status, err error) {
	status = Status{
		Ok:      true,
		Details: make(map[string]string, 0),
	}

	dbs := strings.Join(c.Databases, " -B ")
	args := strings.Split(fmt.Sprintf("-c -B %s -u%s -p%s -h%s -P%s", dbs, c.User, c.Password, c.Host, strconv.Itoa(c.Port)), " ")
	args = append(args, "--skip-write-binlog")
	cmd := exec.Command(
		"mysqlcheck",
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

		return status, fmt.Errorf("mysqlcheck failed with %s", string(out))
	}
	outa := strings.Fields(string(out))
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

func pingMariaDB(c config.DatabaseConfig) (err error) {
	var out []byte
	if out, err = exec.Command("mysqladmin",
		"status",
		"-u"+c.User,
		"-p"+c.Password,
		"-h"+c.Host,
		"-P"+strconv.Itoa(c.Port),
	).CombinedOutput(); err != nil {
		return fmt.Errorf("mysqladmin status error: %s", string(out))
	}
	return
}

func purgeBinlogsTo(c config.DatabaseConfig, log string) (err error) {
	conn, err := client.Connect(fmt.Sprintf("%s:%s", c.Host, strconv.Itoa(c.Port)), c.User, c.Password, "")
	if err = conn.Ping(); err != nil {
		return
	}

	_, err = conn.Execute(fmt.Sprintf("PURGE BINARY LOGS TO '%s'", log))
	if err != nil {
		return
	}
	return
}

func getMariaCheckSumForTable(c config.DatabaseConfig, verifyTables []string) (cs Checksum, err error) {
	cs.TablesChecksum = make(map[string]int64)
	cf := wait.ConditionFunc(func() (bool, error) {
		err = pingMariaDB(c)
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err = wait.Poll(5*time.Second, 1*time.Minute, cf); err != nil {
		return
	}

	conn, err := client.Connect(fmt.Sprintf("%s:%s", c.Host, strconv.Itoa(c.Port)), c.User, c.Password, "")
	if err != nil {
		return
	}
	if err = conn.Ping(); err != nil {
		return
	}

	defer conn.Close()

	rs, err := conn.Execute(fmt.Sprintf("CHECKSUM TABLE %s", strings.Join(verifyTables, ", ")))
	if err != nil {
		return
	}

	for r := 0; r < rs.RowNumber(); r++ {
		var tn string
		var s int64
		tn, err = rs.GetStringByName(r, "Table")
		s, err = rs.GetIntByName(r, "Checksum")
		if err != nil {
			return
		}
		cs.TablesChecksum[tn] = s
	}

	return
}
