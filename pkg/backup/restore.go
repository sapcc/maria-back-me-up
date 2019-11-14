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

	"github.com/coreos/etcd/client"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"k8s.io/apimachinery/pkg/util/wait"
)

type Restore struct {
	cfg    config.Config
	docker *client.Client
	backup *Backup
}

func NewRestore(c config.Config) (r *Restore) {
	return &Restore{
		cfg: c,
	}
}

func (r *Restore) restore(backupPath string) (err error) {
	if err = r.restartMariaDB(); err != nil {
		//Cant shutdown database. Try to delete datadir anyway.
		log.Error(fmt.Errorf("Timed out trying to shutdown database"))
	}
	if err = r.deleteMariaDBData(); err != nil {
		log.Error(fmt.Errorf("Error trying to delete database data dir: %s", err.Error()))
	}
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := HealthCheck(r.cfg.MariaDB)
		if err != nil || !s.Ok {
			return false, nil
		}
		return true, nil
	})
	if err = wait.Poll(5*time.Second, 5*time.Minute, cf); err != nil {
		return fmt.Errorf("Timed out waiting for mariadb to become healthy")
	}

	//Drop database
	if err = exec.Command("mysqladmin",
		"-u"+r.cfg.MariaDB.User,
		"-p"+r.cfg.MariaDB.Password,
		"-h"+r.cfg.MariaDB.Host,
		"-P"+strconv.Itoa(r.cfg.MariaDB.Port),
		"drop", r.cfg.MariaDB.Database,
		"--force",
	).Run(); err != nil {
		log.Error(fmt.Errorf("mysqladmin drop table error: %s", err.Error()))
	}

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
	//mysql -h 127.0.0.1 -u root -p -N -e "SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST where user != 'system user' and user !='root';" | awk '{print "KILL "$1";"}'| mysql -h 127.0.0.1 -u root -p

	if err = exec.Command(
		"myloader",
		"--port="+strconv.Itoa(r.cfg.MariaDB.Port),
		"--host="+r.cfg.MariaDB.Host,
		"--user="+r.cfg.MariaDB.User,
		"--password="+r.cfg.MariaDB.Password,
		"--directory="+path.Join(backupPath, "dump"),
		"--overwrite-tables",
	).Run(); err != nil {
		return
	}
	log.Debug("myloader restore finished")

	return r.restoreIncBackupFromPath(backupPath)
}

func (r *Restore) restoreIncBackupFromPath(p string) (err error) {
	var binlogFiles []string
	filepath.Walk(p, func(p string, f os.FileInfo, err error) error {
		if f.IsDir() && f.Name() == "dump" {
			return filepath.SkipDir
		}

		if !f.IsDir() && strings.Contains(f.Name(), "mysqld-bin") {
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
		return
	}
	return mysqlPipe.Wait()
}

func (r *Restore) deleteMariaDBData() (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		if err = os.RemoveAll(r.cfg.MariaDB.DataDir); err != nil {
			return false, nil
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
