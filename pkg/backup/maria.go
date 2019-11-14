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

type Status struct {
	Ok      bool
	Details map[string]int
}

type checksum struct {
	Table string `yaml:"table"`
	Sum   int64  `yaml:"sum"`
}

func HealthCheck(c config.MariaDB) (status Status, err error) {
	status = Status{
		Ok:      true,
		Details: make(map[string]int, 0),
	}
	dbs := strings.Join(c.Databases, " -B ")
	args := strings.Split(fmt.Sprintf("-c -B %s -u%s -p%s -h%s -P%s", dbs, c.User, c.Password, c.Host, strconv.Itoa(c.Port)), " ")
	cmd := exec.Command(
		"mysqlcheck",
		args...,
	)

	out, err := cmd.Output()
	if err != nil {
		status.Ok = false
		return status, fmt.Errorf("mysqlcheck failed with %s", err)
	}
	outa := strings.Fields(string(out))
	for i := 0; i < len(outa)-1; i++ {
		if i%2 == 0 {
			if outa[i+1] == "OK" {
				status.Details[outa[i]] = 1
			} else {
				status.Ok = false
				status.Details[outa[i]] = 0
			}
		}
	}

	return
}

func getCheckSumForTable(c config.MariaDB) (cs map[string]int64, err error) {
	cs = make(map[string]int64)
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := HealthCheck(c)
		if err != nil || !s.Ok {
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
