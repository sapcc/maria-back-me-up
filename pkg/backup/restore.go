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
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/maria"
	"k8s.io/apimachinery/pkg/util/wait"
)

type Restore struct {
	cfg    config.BackupService
	backup *Backup
}

func NewRestore(c config.BackupService) (r *Restore) {
	return &Restore{
		cfg: c,
	}
}

func (r *Restore) VerifyRestore(backupPath string) (err error) {
	if err = r.waitMariaDbUp(5 * time.Minute); err != nil {
		return fmt.Errorf("Timed out waiting for verfiy mariadb to boot. Cant perform verification")
	}
	if err = r.restoreDump(backupPath); err != nil {
		return
	}

	if err = r.restoreIncBackup(backupPath); err != nil {
		return
	}

	return
}

func (r *Restore) restore(backupPath string) (err error) {
	if err = r.restartMariaDB(); err != nil {
		//Cant shutdown database. Lets try restore anyway
		log.Error(fmt.Errorf("Timed out trying to shutdown database"))
	}
	err = r.waitMariaDbUp(5 * time.Minute)
	if err != nil {
		log.Error(fmt.Errorf("Timed out waiting for mariadb to boot. Delete data dir"))
		r.deleteMariaDBDatabases()
	} else {
		r.dropMariaDBDatabases()
	}

	if err = r.waitMariaDbUp(1 * time.Minute); err != nil {
		return fmt.Errorf("Timed out waiting for mariadb to boot. Cant perform restore")
	}

	if err = r.restoreDump(backupPath); err != nil {
		return
	}

	if err = r.restoreIncBackup(backupPath); err != nil {
		return
	}

	if sts, err := maria.HealthCheck(r.cfg.MariaDB); err != nil || !sts.Ok {
		return fmt.Errorf("Mariadb health check failed after restore. Tables corrupted: %s", sts.Details)
	}
	return
}

func (r *Restore) restoreDump(backupPath string) (err error) {
	log.Debug("Restore path: ", backupPath)
	if err = os.MkdirAll(
		filepath.Join(backupPath, "dump"), os.ModePerm); err != nil {
		return
	}
	log.Debug("tar path: ", path.Join(backupPath, "dump"))
	if err = exec.Command(
		"tar",
		"-xvf", path.Join(backupPath, "dump.tar"),
		"-C", path.Join(backupPath, "dump"),
	).Run(); err != nil {
		return
	}

	b, err := exec.Command(
		"myloader",
		"--port="+strconv.Itoa(r.cfg.MariaDB.Port),
		"--host="+r.cfg.MariaDB.Host,
		"--user="+r.cfg.MariaDB.User,
		"--password="+r.cfg.MariaDB.Password,
		"--directory="+path.Join(backupPath, "dump"),
		"--overwrite-tables",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("myloader error: %s", string(b))
	}
	log.Debug("myloader restore finished")
	return
}

func (r *Restore) restoreIncBackup(p string) (err error) {
	var binlogFiles []string
	filepath.Walk(p, func(p string, f os.FileInfo, err error) error {
		if f.IsDir() && f.Name() == "dump" {
			return filepath.SkipDir
		}

		if !f.IsDir() && strings.Contains(f.Name(), r.cfg.MariaDB.LogBin) {
			binlogFiles = append(binlogFiles, p)
		}
		return nil
	})
	if len(binlogFiles) == 0 {
		return
	}
	log.Debug("start mysqlbinlog", binlogFiles)
	binlogCMD := exec.Command(
		"mysqlbinlog", binlogFiles...,
	)
	mysqlPipe := exec.Command(
		"mysql",
		"--binary-mode",
		"-u"+r.cfg.MariaDB.User,
		"-p"+r.cfg.MariaDB.Password,
		"-h"+r.cfg.MariaDB.Host,
		"-P"+strconv.Itoa(r.cfg.MariaDB.Port),
	)
	pipe, _ := binlogCMD.StdoutPipe()
	defer pipe.Close()
	mysqlPipe.Stdin = pipe
	//mysqlPipe.Stdout = os.Stdout
	if err = mysqlPipe.Start(); err != nil {
		return
	}
	if err = binlogCMD.Run(); err != nil {
		return fmt.Errorf("mysqlbinlog error: %s", err.Error())
	}
	return mysqlPipe.Wait()
}

func (r *Restore) dropMariaDBDatabases() {
	for _, d := range r.cfg.MariaDB.Databases {
		log.Debug("dropping database: ", d)
		if err := exec.Command("mysqladmin",
			"-u"+r.cfg.MariaDB.User,
			"-p"+r.cfg.MariaDB.Password,
			"-h"+r.cfg.MariaDB.Host,
			"-P"+strconv.Itoa(r.cfg.MariaDB.Port),
			"drop", d,
			"--force",
		).Run(); err != nil {
			log.Error(fmt.Errorf("mysqladmin drop table error: %s", err.Error()))
		}
	}

}

func (r *Restore) deleteMariaDBDatabases() (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		for _, d := range r.cfg.MariaDB.Databases {
			log.Debug("deleting database: ", d)
			if err = os.RemoveAll(filepath.Join(r.cfg.MariaDB.DataDir, d)); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if r.cfg.MariaDB.DataDir != "" {
		return wait.Poll(1*time.Second, 30*time.Second, cf)
	}
	return
}

func (r *Restore) restartMariaDB() (err error) {
	cmd := exec.Command("mysqladmin",
		"shutdown",
		"-u"+r.cfg.MariaDB.User,
		"-p"+r.cfg.MariaDB.Password,
		"-h"+r.cfg.MariaDB.Host,
		"-P"+strconv.Itoa(r.cfg.MariaDB.Port),
	)
	cf := wait.ConditionFunc(func() (bool, error) {
		err = cmd.Run()
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	return wait.Poll(5*time.Second, 30*time.Second, cf)
}

func (r *Restore) waitMariaDBHealthy(timeout time.Duration) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := maria.HealthCheck(r.cfg.MariaDB)
		if err != nil {
			return false, nil
		} else if !s.Ok {
			return false, fmt.Errorf("Tables corrupt: %s", s.Details)
		}
		return true, nil
	})
	return wait.Poll(5*time.Second, timeout, cf)
}

func (r *Restore) waitMariaDbUp(timeout time.Duration) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		err := maria.PingMariaDB(r.cfg.MariaDB)
		if err != nil {
			log.Error("Error Pinging mariadb. error: ", err.Error())
			return false, nil
		}
		log.Debug("Pinging mariadb successful")
		return true, nil
	})
	return wait.Poll(5*time.Second, timeout, cf)
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
