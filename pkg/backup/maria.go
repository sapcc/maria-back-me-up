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
package backup

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/siddontang/go-mysql/client"
	"k8s.io/apimachinery/pkg/util/wait"
)

type (
	Status struct {
		Ok      bool
		Details map[string]string
	}

	DatabaseMissingError struct {
		message string
	}

	checksum struct {
		Table string `yaml:"table"`
		Sum   int64  `yaml:"sum"`
	}
)

func (d *DatabaseMissingError) Error() string {
	return "Database not available"
}

func HealthCheck(c config.MariaDB) (status Status, err error) {
	status = Status{
		Ok:      true,
		Details: make(map[string]string, 0),
	}

	dbs := strings.Join(c.Databases, " -B ")
	args := strings.Split(fmt.Sprintf("-c -B %s -u%s -p%s -h%s -P%s", dbs, c.User, c.Password, c.Host, strconv.Itoa(c.Port)), " ")
	cmd := exec.Command(
		"mysqlcheck",
		args...,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		status.Ok = false
		if strings.Contains(string(out), "1049") {
			return status, &DatabaseMissingError{}
		}
		return status, fmt.Errorf("mysqlcheck failed with %s", err)
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

func PingMariaDB(c config.MariaDB) (err error) {
	if err = exec.Command("mysqladmin",
		"status",
		"-u"+c.User,
		"-p"+c.Password,
		"-h"+c.Host,
		"-P"+strconv.Itoa(c.Port),
	).Run(); err != nil {
		return fmt.Errorf("mysqladmin status error: %s", err.Error())
	}

	return
}

func getCheckSumForTable(c config.MariaDB) (cs map[string]int64, err error) {
	cs = make(map[string]int64)
	cf := wait.ConditionFunc(func() (bool, error) {
		err = PingMariaDB(c)
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

	rs, err := conn.Execute(fmt.Sprintf("CHECKSUM TABLE %s", strings.Join(c.VerifyTables, ", ")))
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
		cs[tn] = s
	}

	return
}

func runMysqlDiff(c1, c2 config.MariaDB) (out []byte, err error) {
	//mysqldiff --server1=root:pw@localhost:3306 --server2=root:pw@db_backup:3306 test:test
	s1 := fmt.Sprintf("%s:%s@%s:%s", c1.User, c1.Password, c1.Host, strconv.Itoa(c1.Port))
	s2 := fmt.Sprintf("%s:%s@%s:%s", c2.User, c2.Password, c2.Host, strconv.Itoa(c2.Port))
	dbs := make([]string, 0)
	for _, db := range c1.Databases {
		dbs = append(dbs, fmt.Sprintf("%s:%s", db, db))
	}
	e := exec.Command("mysqldiff",
		"--server1="+s1,
		"--server2="+s2,
		"--difftype=differ",
	)
	e.Args = append(e.Args, dbs...)

	out, err = e.CombinedOutput()
	return
}
