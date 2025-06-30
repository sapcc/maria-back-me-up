/**
 * Copyright 2024 SAP SE
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
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/wait"
)

type slowQueryLogState string

const (
	slowQueryLogON  slowQueryLogState = "ON"
	slowQueryLogOFF slowQueryLogState = "OFF"
)

type (
	// MariaDB database struct
	MariaDB struct {
		cfg               config.Config
		storage           *storage.Manager
		kub               *k8s.Database
		logPosition       LogPosition
		flushTimer        *time.Timer
		slowQueryLogState slowQueryLogState
	}
	metadata struct {
		Status binlog `yaml:"SHOW MASTER STATUS"`
	}
	binlog struct {
		Log  string `yaml:"Log"`
		Pos  uint32 `yaml:"Pos"`
		GTID string `yaml:"GTID"`
	}
)

// NewMariaDB creates a mariadb databse instance
func NewMariaDB(c config.Config, sm *storage.Manager, k *k8s.Database) (Database, error) {
	return &MariaDB{
		cfg:               c,
		storage:           sm,
		kub:               k,
		slowQueryLogState: slowQueryLogON,
	}, nil
}

// CreateFullBackup creates a mariadb full backup
func (m *MariaDB) CreateFullBackup(path string) (bp LogPosition, err error) {
	if path == "" {
		return bp, errors.New("no path given")
	}
	defer func() {
		if err = m.setSlowQueryLog(slowQueryLogON); err != nil {
			log.Error(fmt.Errorf("error enabling slow_query_log: %s", err.Error()))
		}
	}()

	if err = m.setSlowQueryLog(slowQueryLogOFF); err != nil {
		log.Error(fmt.Errorf("error disabling slow_query_log: %s", err.Error()))
		// dont stop fullbackup because of toggling slow_query_log
	}

	switch m.cfg.Database.DumpTool {
	case config.Mysqldump:
		bp, err = m.createMysqlDump(path)
		if err != nil {
			return bp, err
		}
	case config.MyDumper:
		bp, err = m.createMyDump(path)
		if err != nil {
			return bp, err
		}
	default:
		return bp, fmt.Errorf("unsupported dump tool: %s", m.cfg.Database.DumpTool.String())
	}
	return
}

// GetLogPosition returns the current mariadb log position
func (m *MariaDB) GetLogPosition() LogPosition {
	return m.logPosition
}

// GetConfig returns the database config
func (m *MariaDB) GetConfig() config.DatabaseConfig {
	return m.cfg.Database
}

// Restore runs a restore on the mariadb
func (m *MariaDB) Restore(path string) (err error) {
	withIP := false

	if m.kub != nil {
		withIP = true
		old := m.cfg.Database.Host
		ip, err := m.kub.GetPodIP(fmt.Sprintf("app=%s-mariadb", m.cfg.ServiceName))
		if err != nil {
			return err
		}
		m.cfg.Database.Host = ip
		defer func() {
			m.cfg.Database.Host = old
		}()
	}
	if err = m.restartMariaDB(); err != nil {
		// Cant shutdown database. Lets try restore anyway
		log.Error(fmt.Errorf("timed out trying to shutdown database, %s", err.Error()))
	}
	err = m.Up(5*time.Minute, withIP)
	if err != nil {
		log.Error(fmt.Errorf("timed out waiting for mariadb to boot. Delete data dir"))
		if err := m.deleteMariaDBDataDir(); err != nil {
			return fmt.Errorf("cannot delete data dir %s", err.Error())
		}
	} else {
		m.dropMariaDBDatabases()
	}

	if err = m.Up(1*time.Minute, withIP); err != nil {
		return errors.New("timed out waiting for mariadb to boot. Cant perform restore")
	}

	if err = m.restoreDump(path); err != nil {
		return
	}

	if err = m.restoreIncBackup(path); err != nil {
		return
	}

	if sts, err := mariaHealthCheck(m.cfg.Database); err != nil || !sts.Ok {
		return fmt.Errorf("mariadb health check failed after restore. Tables corrupted: %s", sts.Details)
	}
	if err = resetSlave(m.cfg.Database); err != nil {
		log.Error(fmt.Errorf("cannot call RESET slave: %s", err.Error()))
	}
	return
}

// VerifyRestore runs a restore on the mariadb for verification
func (m *MariaDB) VerifyRestore(path string) (err error) {
	if err = m.Up(10*time.Minute, false); err != nil {
		return errors.New("timed out waiting for verfiy mariadb to boot. Cant perform verification")
	}
	if err = m.restoreDump(path); err != nil {
		return
	}

	if err = m.restoreIncBackup(path); err != nil {
		return
	}

	return
}

// GetCheckSumForTable returns tables checksums
func (m *MariaDB) GetCheckSumForTable(verifyTables []string, withIP bool) (cs Checksum, err error) {
	cs.TablesChecksum = make(map[string]int64)
	conn, err := client.Connect(fmt.Sprintf("%s:%s", m.cfg.Database.Host, strconv.Itoa(m.cfg.Database.Port)), m.cfg.Database.User, m.cfg.Database.Password, "")
	if err != nil {
		return
	}

	defer conn.Close()

	if err = conn.Ping(); err != nil {
		return
	}

	_ = m.FlushIncBackup()
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

// HealthCheck returns the mariadb databases healthcheck
func (m *MariaDB) HealthCheck() (status Status, err error) {
	return mariaHealthCheck(m.cfg.Database)
}

func (m *MariaDB) createMyDump(toPath string) (bp LogPosition, err error) {
	log.Debug("running mydumper...")
	mydumperCmd := exec.Command(
		"mydumper",
		"--port="+strconv.Itoa(m.cfg.Database.Port),
		"--host="+m.cfg.Database.Host,
		"--user="+m.cfg.Database.User,
		"--password="+m.cfg.Database.Password,
		"--outputdir="+toPath,
		// `--regex=^(?!(mysql\.))`,
		"--compress",
		"--trx-consistency-only",
		"--compress-protocol",
		"--rows=50000",
		"--threads=8",
	)

	err = mydumperCmd.Run()
	if err != nil {
		return
	}
	myBp, err := getMyDumpBinlog(toPath)
	if err != nil {
		return
	}
	bp.Pos = myBp.Pos
	bp.Name = myBp.Name
	log.Debug("Uploading full backup")
	return bp, m.storage.WriteFolderAll(toPath)
}

func (m *MariaDB) createMysqlDump(toPath string) (bp LogPosition, err error) {
	var myBp mysql.Position
	log.Debug("running mariadb-dump...")
	err = os.MkdirAll(toPath, os.ModePerm)
	if err != nil {
		return
	}
	outfile, err := os.Create(filepath.Join(toPath, "dump.sql"))
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, outfile.Close())
	}()

	cmd := exec.Command(
		"mariadb-dump",
		"--port="+strconv.Itoa(m.cfg.Database.Port),
		"--host="+m.cfg.Database.Host,
		"--user="+m.cfg.Database.User,
		"--password="+m.cfg.Database.Password,
		"--skip-ssl",
		"--single-transaction",
		"--quick",
		"--all-databases",
		"--master-data=1",
		"--max_allowed_packet=512M",
	)
	cmd.Stdout = outfile
	err = cmd.Run()
	if err != nil {
		return bp, fmt.Errorf("could not create full backup: %v", err)
	}
	dump, err := os.Open(filepath.Join(toPath, "dump.sql"))
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, dump.Close())
	}()
	scanner := bufio.NewScanner(dump)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "MASTER_LOG_FILE") {
			s := strings.ReplaceAll(scanner.Text(), "'", "")
			myBp, err = getMysqlDumpBinlog(s)
			if err == nil && myBp.Pos != 0 {
				break
			}
		}
	}
	if myBp.Pos == 0 {
		return bp, errors.New("no binlog position found")
	}
	log.Debug("found binlogPosition: ", myBp.Pos)
	bp.Pos = myBp.Pos
	bp.Name = myBp.Name
	log.Debug("Uploading full backup")
	return bp, m.storage.WriteFolderAll(toPath)
}

// StartIncBackup creates an incremental backup
func (m *MariaDB) StartIncBackup(ctx context.Context, mp LogPosition, dir string, ch chan error) (err error) {
	var binlogFile string
	cfg := replication.BinlogSyncerConfig{
		ServerID:             uint32(m.cfg.Database.ServerID),
		Flavor:               "mariadb",
		Host:                 m.cfg.Database.Host,
		Port:                 uint16(m.cfg.Database.Port),
		User:                 m.cfg.Database.User,
		Password:             m.cfg.Database.Password,
		MaxReconnectAttempts: m.cfg.Backup.BinlogMaxReconnectAttempts,
		DumpCommandFlag:      2,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	binlogChan := make(chan storage.StreamEvent, 1)
	defer func() {
		log.Debug("closing binlog syncer")
		close(binlogChan)
		syncer.Close()
		if m.flushTimer != nil {
			m.flushTimer.Stop()
			m.flushTimer = nil
		}
	}()

	// Start sync with specified binlog file and position
	myBp := mysql.Position{Pos: mp.Pos, Name: mp.Name}
	streamer, err := syncer.StartSync(myBp)
	if err != nil {
		return fmt.Errorf("Cannot start binlog stream: %w", err)
	}
	if m.cfg.Backup.IncrementalBackupInMinutes > 0 {
		m.flushTimer = time.AfterFunc(time.Duration(m.cfg.Backup.IncrementalBackupInMinutes)*time.Minute, func() { m.flushLogs("") })
	}
	for {
		ev, inerr := streamer.GetEvent(ctx)
		if inerr != nil {
			if inerr == ctx.Err() {
				return nil
			}
			return fmt.Errorf("Error reading binlog stream: %w", inerr)
		}
		offset := ev.Header.LogPos

		if ev.Header.EventType == replication.ROTATE_EVENT {
			rotateEvent := ev.Event.(*replication.RotateEvent)
			binlogFile = string(rotateEvent.NextLogName)
			log.Debug("Binlog syncer rotation. next log file", offset, string(rotateEvent.NextLogName))
			if ev.Header.Timestamp == 0 || offset == 0 {
				continue
			}
		} else if ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			// FormateDescriptionEvent is the first event in binlog, we will close old writer and create new ones
			if binlogFile != "" {
				close(binlogChan)
				time.Sleep(100 * time.Millisecond)
			}
			binlogChan = make(chan storage.StreamEvent, 1)
			var eg errgroup.Group
			eg.Go(func() error {
				return m.storage.WriteStreamAll(path.Join(dir, binlogFile), "", binlogChan, false)
			})
			go m.handleWriteErrors(ctx, &eg, ch)

			binlogChan <- &storage.ByteEvent{Value: replication.BinLogFileHeader}

		}
		binlogChan <- &storage.BinlogEvent{Value: ev}

		switch ev.Event.(type) {
		case *replication.RowsEvent:
			if m.flushTimer == nil && m.cfg.Backup.IncrementalBackupInMinutes > 0 {
				m.flushTimer = time.AfterFunc(time.Duration(m.cfg.Backup.IncrementalBackupInMinutes)*time.Minute, func() { m.flushLogs(binlogFile) })
			}
		case *replication.QueryEvent:
			if m.flushTimer == nil && m.cfg.Backup.IncrementalBackupInMinutes > 0 {
				m.flushTimer = time.AfterFunc(time.Duration(m.cfg.Backup.IncrementalBackupInMinutes)*time.Minute, func() { m.flushLogs(binlogFile) })
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}

// FlushIncBackup resets the inc backup timer
func (m *MariaDB) FlushIncBackup() (err error) {
	if m.flushTimer != nil {
		if !m.flushTimer.Stop() {
			<-m.flushTimer.C
		}
		m.flushTimer.Reset(0)
	} else {
		return fmt.Errorf("no writes to database happened yet")
	}
	return
}

func (m *MariaDB) handleWriteErrors(ctx context.Context, eg *errgroup.Group, ch chan error) {
	err := eg.Wait()
	if ctx.Err() != nil {
		close(ch)
		return
	}
	ch <- err
}

func (m *MariaDB) flushLogs(binlogFile string) {
	defer func() {
		m.flushTimer = nil
		if err := m.setSlowQueryLog(slowQueryLogON); err != nil {
			log.Error(fmt.Errorf("error enabling slow_query_log: %s", err.Error()))
		}
	}()
	if err := m.setSlowQueryLog(slowQueryLogOFF); err != nil {
		log.Error(fmt.Errorf("error disabling slow_query_log: %s", err.Error()))
		// dont stop because of toggling slow_query_log
	}
	flushLogs := exec.Command(
		"mariadb-admin",
		"flush-logs",
		"--port="+strconv.Itoa(m.cfg.Database.Port),
		"--host="+m.cfg.Database.Host,
		"--user="+m.cfg.Database.User,
		"--password="+m.cfg.Database.Password,
		"--skip-ssl",
	)
	_, err := flushLogs.CombinedOutput()
	if err != nil {
		log.Error("Error flushing binlogs: ", err)
		return
	}

	if !m.cfg.Backup.DisableBinlogPurgeOnRotate {
		if m.cfg.Backup.PurgeBinlogAfterMinutes > 0 {
			err = purgeBinlogsBefore(m.cfg.Database, m.cfg.Backup.PurgeBinlogAfterMinutes)
			if err != nil {
				log.Warn("error purging binlogs: ", err)
			}
		} else if binlogFile != "" {
			err = purgeBinlogsTo(m.cfg.Database, binlogFile)
			if err != nil {
				log.Warn("error purging binlogs: ", err)
			}
		}
	}
}

func (m *MariaDB) restoreDump(backupPath string) (err error) {
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
	switch m.cfg.Database.DumpTool {
	case config.Mysqldump:
		dump, err := os.Open(path.Join(backupPath, "dump", "dump.sql"))

		if err != nil {
			return fmt.Errorf("could not open dump file: %s", err.Error())
		}

		cmd := exec.Command(
			"mariadb",
			"--skip-ssl",
			"--port="+strconv.Itoa(m.cfg.Database.Port),
			"--host="+m.cfg.Database.Host,
			"--user="+m.cfg.Database.User,
			"--password="+m.cfg.Database.Password,
		)
		cmd.Stdin = dump
		b, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s error: %s", config.Mysqldump.String(), string(b))
		}
		log.Debug(fmt.Sprintf("%s restore finished", config.Mysqldump.String()))
	case config.MyDumper:
		b, err := exec.Command(
			"myloader",
			"--port="+strconv.Itoa(m.cfg.Database.Port),
			"--host="+m.cfg.Database.Host,
			"--user="+m.cfg.Database.User,
			"--password="+m.cfg.Database.Password,
			"--directory="+path.Join(backupPath, "dump"),
			"--overwrite-tables",
			"--compress-protocol",
			"--queries-per-transaction=500",
			"--threads=8",
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s error: %s", config.MyDumper.String(), string(b))
		}
		log.Debug(fmt.Sprintf("%s restore finished", config.MyDumper.String()))
	}
	return
}

func (m *MariaDB) restoreIncBackup(p string) (err error) {
	var binlogFiles []string
	err = filepath.Walk(p, func(p string, f os.FileInfo, err error) error {
		if f.IsDir() && f.Name() == "dump" {
			return filepath.SkipDir
		}

		if !f.IsDir() && strings.Contains(f.Name(), m.cfg.Database.LogNameFormat) {
			binlogFiles = append(binlogFiles, p)
		}
		return nil
	})
	if err != nil {
		return
	}
	if len(binlogFiles) == 0 {
		return
	}
	log.Debug("start mysqlbinlog", binlogFiles)
	binlogCMD := exec.Command(
		"mysqlbinlog", binlogFiles...,
	)
	mysqlPipe := exec.Command(
		"mariadb",
		"--skip-ssl",
		"--binary-mode",
		"-u"+m.cfg.Database.User,
		"-p"+m.cfg.Database.Password,
		"-h"+m.cfg.Database.Host,
		"-P"+strconv.Itoa(m.cfg.Database.Port),
	)
	pipe, _ := binlogCMD.StdoutPipe()
	defer pipe.Close()
	mysqlPipe.Stdin = pipe
	// mysqlPipe.Stdout = os.Stdout
	if err = mysqlPipe.Start(); err != nil {
		return
	}
	if err = binlogCMD.Run(); err != nil {
		return fmt.Errorf("mysqlbinlog error: %s", err.Error())
	}
	return mysqlPipe.Wait()
}

/*
func (m *MariaDB) checkBackupDirExistsAndCreate() (p string, err error) {
	if _, err := os.Stat(m.cfg.Backup.BackupDir); os.IsNotExist(err) {
		err = os.MkdirAll(m.cfg.Backup.BackupDir, os.ModePerm)
		return m.cfg.Backup.BackupDir, err
	}
	return
}
*/

// Up checks if the mariadb is up and recieving requests
func (m *MariaDB) Up(timeout time.Duration, withIP bool) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		err = m.pingMariaDB(withIP)
		if err != nil {
			log.Error("Error pinging mariadb. error: ", err.Error())
			return false, nil
		}
		log.Debug("Pinging mariadb successful")
		return true, nil
	})
	return wait.Poll(5*time.Second, timeout, cf)
}

/*
func (m *MariaDB) waitMariaDBHealthy(timeout time.Duration) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := mariaHealthCheck(m.cfg.Database)
		if err != nil {
			return false, nil
		} else if !s.Ok {
			return false, fmt.Errorf("Tables corrupt: %s", s.Details)
		}
		return true, nil
	})
	return wait.Poll(5*time.Second, timeout, cf)
}
*/

func (m *MariaDB) deleteMariaDBDataDir() (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		for _, d := range m.cfg.Database.Databases {
			log.Debug("deleting database: ", d)
			if err = os.RemoveAll(filepath.Join(m.cfg.Database.DataDir, d)); err != nil {
				return false, nil
			}
		}
		if err = os.RemoveAll(m.cfg.Database.DataDir); err != nil {
			return false, nil
		}
		return true, nil
	})
	if m.cfg.Database.DataDir != "" {
		return wait.Poll(1*time.Second, 30*time.Second, cf)
	}
	return
}

func (m *MariaDB) pingMariaDB(withIP bool) (err error) {
	var out []byte
	if withIP {
		ip, err := m.kub.GetPodIP(fmt.Sprintf("app=%s-mariadb", m.cfg.ServiceName))
		if err != nil {
			return err
		}
		m.cfg.Database.Host = ip
	}
	if out, err = exec.Command("mariadb-admin",
		"status",
		"-u"+m.cfg.Database.User,
		"-p"+m.cfg.Database.Password,
		"-h"+m.cfg.Database.Host,
		"-P"+strconv.Itoa(m.cfg.Database.Port),
		"--skip-ssl",
	).CombinedOutput(); err != nil {
		var msg string
		if out != nil {
			msg = string(out) // Stdout/Stderr
		} else {
			msg = err.Error() // Error message if command cannot be executed
		}
		return fmt.Errorf("mariadb-admin status error: %s", msg)
	}
	return
}

func (m *MariaDB) dropMariaDBDatabases() {
	for _, d := range m.cfg.Database.Databases {
		log.Debug("dropping database: ", d)
		if err := exec.Command("mariadb-admin",
			"-u"+m.cfg.Database.User,
			"-p"+m.cfg.Database.Password,
			"-h"+m.cfg.Database.Host,
			"-P"+strconv.Itoa(m.cfg.Database.Port),
			"drop", d,
			"--force",
			"--skip-ssl",
		).Run(); err != nil {
			log.Error(fmt.Errorf("mariadb-admin drop table error: %s", err.Error()))
		}
	}

}

func (m *MariaDB) restartMariaDB() (err error) {
	cmd := exec.Command("mariadb-admin",
		"shutdown",
		"-u"+m.cfg.Database.User,
		"-p"+m.cfg.Database.Password,
		"-h"+m.cfg.Database.Host,
		"-P"+strconv.Itoa(m.cfg.Database.Port),
		"--skip-ssl",
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

func (m *MariaDB) setSlowQueryLog(state slowQueryLogState) (err error) {
	conn, err := client.Connect(fmt.Sprintf("%s:%s", m.cfg.Database.Host, strconv.Itoa(m.cfg.Database.Port)), m.cfg.Database.User, m.cfg.Database.Password, "")
	if err != nil {
		return
	}
	defer conn.Close()
	if m.slowQueryLogState != state {
		_, err = conn.Execute(fmt.Sprintf("set global slow_query_log = '%s'", state))
		if err == nil {
			m.slowQueryLogState = state
		}
	}
	return
}

func getMyDumpBinlog(p string) (mp mysql.Position, err error) {
	meta := metadata{}
	yamlBytes, err := os.ReadFile(path.Join(p, "/metadata"))
	if err != nil {
		return mp, fmt.Errorf("read config file: %s", err.Error())
	}
	//turn string to valid yaml
	yamlCorrect := strings.ReplaceAll(string(yamlBytes), "\t", "  ")
	r, _ := regexp.Compile(`([a-zA-Z])[\:]([^\s])`)
	err = yaml.Unmarshal([]byte(r.ReplaceAllString(yamlCorrect, `$1: $2`)), &meta)
	if err != nil {
		return mp, fmt.Errorf("parse config file: %s", err.Error())
	}
	mp.Name = meta.Status.Log
	mp.Pos = meta.Status.Pos

	return
}

func getMysqlDumpBinlog(s string) (mp mysql.Position, err error) {
	var rex = regexp.MustCompile(`(\w+)=([^;,]*)`)
	data := rex.FindAllStringSubmatch(s, -1)
	res := make(map[string]string)
	for _, kv := range data {
		k := kv[1]
		v := kv[2]
		res[k] = v
	}
	logPos, ok := res["MASTER_LOG_POS"]
	if !ok {
		return
	}

	pos, err := strconv.ParseInt(logPos, 10, 32)
	mp.Name = res["MASTER_LOG_FILE"]
	mp.Pos = uint32(pos)
	return
}

// GetDatabaseDiff returns the database diff between two mariadbs
func (m *MariaDB) GetDatabaseDiff(c1, c2 config.DatabaseConfig) (out []byte, err error) {
	// mysqldiff --server1=root:pw@localhost:3306 --server2=root:pw@db_backup:3306 test:test
	s1 := fmt.Sprintf("%s:%s@%s:%s", c1.User, c1.Password, c1.Host, strconv.Itoa(c1.Port))
	s2 := fmt.Sprintf("%s:%s@%s:%s", c2.User, c2.Password, c2.Host, strconv.Itoa(c2.Port))
	dbs := make([]string, 0)
	for _, db := range c1.Databases {
		dbs = append(dbs, fmt.Sprintf("%s:%s", db, db))
	}
	e := exec.Command("mysqldiff",
		"--skip-table-options",
		"--server1="+s1,
		"--server2="+s2,
		"--difftype=differ",
	)
	e.Args = append(e.Args, dbs...)

	out, err = e.CombinedOutput()
	return
}

//TODO: additional backup - verification
// source: https://medium.com/tensult/mydumper-myloader-and-my-experience-of-migrating-to-aws-rds-ff74fc9c1add
/* Check the tables count in each database
SELECT table_schema, COUNT(*) as tables_count FROM information_schema.tables group by table_schema;
# Check the triggers count in each database
select trigger_schema, COUNT(*) as triggers_count
from information_schema.triggers group by trigger_schema;
# Check the routines count in each database
select routine_schema, COUNT(*) as routines_count
from information_schema.routines group by routine_schema;
# Check the events count in each database
select event_schema, COUNT(*) as events_count
from information_schema.events group by event_schema;
*/
