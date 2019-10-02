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
package health

import (
	"os/exec"
	"strconv"
	"strings"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
)

type Maria struct {
	cfg    config.MariaDB
	Status Status
}

type Status struct {
	Ok      bool
	Details map[string]int
}

func NewMaria(c config.MariaDB) *Maria {
	return &Maria{
		cfg: c,
		Status: Status{
			Ok:      true,
			Details: make(map[string]int, 0),
		},
	}
}

func (m *Maria) Check() (status Status, err error) {
	cmd := exec.Command("mysqlcheck",
		"-A",
		"-u"+m.cfg.User,
		"-p"+m.cfg.Password,
		"-h"+m.cfg.Host,
		"-P"+strconv.Itoa(m.cfg.Port),
	)

	out, err := cmd.Output()
	if err != nil {
		log.Error("mysqlcheck failed with %s\n", err)
		return
	}
	outa := strings.Fields(string(out))
	for i := 0; i < len(outa)-1; i++ {
		if i%2 == 0 {
			if outa[i+1] == "OK" {
				m.Status.Details[outa[i]] = 1
			} else {
				m.Status.Ok = false
				m.Status.Details[outa[i]] = 0
			}
		}
	}

	return m.Status, err
}
