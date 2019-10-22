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

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

type Status struct {
	Ok      bool
	Details map[string]int
}

func HealthCheck(c config.MariaDB) (status Status, err error) {
	status = Status{
		Ok:      true,
		Details: make(map[string]int, 0),
	}
	cmd := exec.Command("mysqlcheck",
		"-A",
		"-u"+c.User,
		"-p"+c.Password,
		"-h"+c.Host,
		"-P"+strconv.Itoa(c.Port),
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
