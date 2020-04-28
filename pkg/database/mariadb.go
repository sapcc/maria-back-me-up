package database

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/wait"
)

type (
	MariaDB struct {
		cfg         config.Config
		storage     *storage.Manager
		logPosition LogPosition
		flushTimer  *time.Timer
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

func NewMariaDB(c config.Config, sm *storage.Manager) (Database, error) {
	return &MariaDB{
		cfg:     c,
		storage: sm,
	}, nil
}

func (m *MariaDB) CreateFullBackup(path string) (bp LogPosition, err error) {
	if m.cfg.Database.DumpTool == nil || *m.cfg.Database.DumpTool == "mysqldump" {
		bp, err = m.createMysqlDump(path)
		if err != nil {
			return bp, fmt.Errorf("error creating mysqlDump: %w", err)
		}
	} else {
		bp, err = m.createMyDump(path)
		if err != nil {
			return bp, fmt.Errorf("error creating mysqlDump: %w", err)
		}
	}
	return
}

func (m *MariaDB) GetLogPosition() LogPosition {
	return m.logPosition
}

func (m *MariaDB) GetConfig() config.DatabaseConfig {
	return m.cfg.Database
}

func (m *MariaDB) Restore(path string) (err error) {
	if err = m.restartMariaDB(); err != nil {
		//Cant shutdown database. Lets try restore anyway
		log.Error(fmt.Errorf("Timed out trying to shutdown database, %s", err.Error()))
	}
	err = m.waitMariaDbUp(5 * time.Minute)
	if err != nil {
		log.Error(fmt.Errorf("Timed out waiting for mariadb to boot. Delete data dir"))
		m.deleteMariaDBDatabases()
	} else {
		m.dropMariaDBDatabases()
	}

	if err = m.waitMariaDbUp(1 * time.Minute); err != nil {
		return fmt.Errorf("Timed out waiting for mariadb to boot. Cant perform restore")
	}

	if err = m.restoreDump(path); err != nil {
		return
	}

	if err = m.restoreIncBackup(path); err != nil {
		return
	}

	if sts, err := mariaHealthCheck(m.cfg.Database); err != nil || !sts.Ok {
		return fmt.Errorf("Mariadb health check failed after restore. Tables corrupted: %s", sts.Details)
	}
	return
}

func (m *MariaDB) VerifyRestore(path string) (err error) {
	if err = m.waitMariaDbUp(5 * time.Minute); err != nil {
		return fmt.Errorf("Timed out waiting for verfiy mariadb to boot. Cant perform verification")
	}
	if err = m.restoreDump(path); err != nil {
		return
	}

	if err = m.restoreIncBackup(path); err != nil {
		return
	}

	return
}

func (m *MariaDB) GetCheckSumForTable(verifyTables []string) (cs Checksum, err error) {
	cs.TablesChecksum = make(map[string]int64)
	cf := wait.ConditionFunc(func() (bool, error) {
		err = pingMariaDB(m.cfg.Database)
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err = wait.Poll(5*time.Second, 1*time.Minute, cf); err != nil {
		return
	}

	conn, err := client.Connect(fmt.Sprintf("%s:%s", m.cfg.Database.Host, strconv.Itoa(m.cfg.Database.Port)), m.cfg.Database.User, m.cfg.Database.Password, "")
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
		//`--regex=^(?!(mysql\.))`,
		"--compress",
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
	log.Debug("running mysqldump...")
	err = os.MkdirAll(toPath, os.ModePerm)
	outfile, err := os.Create(filepath.Join(toPath, "dump.sql"))
	defer outfile.Close()
	cmd := exec.Command(
		"mysqldump",
		"--port="+strconv.Itoa(m.cfg.Database.Port),
		"--host="+m.cfg.Database.Host,
		"--user="+m.cfg.Database.User,
		"--password="+m.cfg.Database.Password,
		"--all-databases",
		"--master-data=1",
	)
	cmd.Stdout = outfile
	cmd.Run()
	dump, err := os.Open(filepath.Join(toPath, "dump.sql"))
	defer dump.Close()
	scanner := bufio.NewScanner(dump)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "MASTER_LOG_FILE") {
			s := strings.ReplaceAll(scanner.Text(), "'", "")
			myBp, err = getMysqlDumpBinlog(s)
			if err != nil {
				return bp, err
			}
		}
	}
	bp.Pos = myBp.Pos
	bp.Name = myBp.Name
	log.Debug("Uploading full backup")
	return bp, m.storage.WriteFolderAll(toPath)
}

func (m *MariaDB) StartIncBackup(ctx context.Context, mp LogPosition, dir string, ch chan error) (err error) {
	var binlogFile string
	cfg := replication.BinlogSyncerConfig{
		ServerID:             999,
		Flavor:               "mariadb",
		Host:                 m.cfg.Database.Host,
		Port:                 uint16(m.cfg.Database.Port),
		User:                 m.cfg.Database.User,
		Password:             m.cfg.Database.Password,
		MaxReconnectAttempts: 5,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	binlogReader, binlogWriter := io.Pipe()
	defer func() {
		log.Debug("closing binlog syncer")
		binlogWriter.Close()
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
	m.flushTimer = time.AfterFunc(time.Duration(m.cfg.Backup.IncrementalBackupInMinutes)*time.Minute, func() { m.flushLogs("") })
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
				binlogWriter.Close()
				time.Sleep(100 * time.Millisecond)
			}
			binlogReader, binlogWriter = io.Pipe()
			var eg errgroup.Group
			eg.Go(func() error {
				return m.storage.WriteStreamAll(path.Join(dir, binlogFile), "", binlogReader)
			})
			go m.handleWriteErrors(ctx, &eg, ch)

			binlogWriter.Write(replication.BinLogFileHeader)

		}
		binlogWriter.Write(ev.RawData)

		switch ev.Event.(type) {
		case *replication.RowsEvent:
			if m.flushTimer == nil {
				m.flushTimer = time.AfterFunc(time.Duration(m.cfg.Backup.IncrementalBackupInMinutes)*time.Minute, func() { m.flushLogs(binlogFile) })
			}
		case *replication.QueryEvent:
			if m.flushTimer == nil {
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

func (m *MariaDB) handleWriteErrors(ctx context.Context, eg *errgroup.Group, ch chan error) {
	err := eg.Wait()
	if ctx.Err() != nil {
		close(ch)
		return
	}
	ch <- err
}

func (m *MariaDB) flushLogs(binlogFile string) (err error) {
	defer func() {
		m.flushTimer = nil
	}()
	flushLogs := exec.Command(
		"mysqladmin",
		"flush-logs",
		"--port="+strconv.Itoa(m.cfg.Database.Port),
		"--host="+m.cfg.Database.Host,
		"--user="+m.cfg.Database.User,
		"--password="+m.cfg.Database.Password,
	)
	_, err = flushLogs.CombinedOutput()
	if err != nil {
		log.Error("Error flushing binlogs: ", err)
		return
	}

	if binlogFile != "" {
		return purgeBinlogsTo(m.cfg.Database, binlogFile)
	}
	return
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
	if m.cfg.Database.DumpTool == nil || *m.cfg.Database.DumpTool == "mysqldump" {
		dump, err := os.Open(path.Join(backupPath, "dump", "dump.sql"))
		cmd := exec.Command(
			"mysql",
			"--port="+strconv.Itoa(m.cfg.Database.Port),
			"--host="+m.cfg.Database.Host,
			"--user="+m.cfg.Database.User,
			"--password="+m.cfg.Database.Password,
		)
		cmd.Stdin = dump
		b, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("myloader error: %s", string(b))
		}
	} else {
		b, err := exec.Command(
			"myloader",
			"--port="+strconv.Itoa(m.cfg.Database.Port),
			"--host="+m.cfg.Database.Host,
			"--user="+m.cfg.Database.User,
			"--password="+m.cfg.Database.Password,
			"--directory="+path.Join(backupPath, "dump"),
			"--overwrite-tables",
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("myloader error: %s", string(b))
		}
	}

	log.Debug("myloader restore finished")
	return
}

func (m *MariaDB) restoreIncBackup(p string) (err error) {
	var binlogFiles []string
	filepath.Walk(p, func(p string, f os.FileInfo, err error) error {
		if f.IsDir() && f.Name() == "dump" {
			return filepath.SkipDir
		}

		if !f.IsDir() && strings.Contains(f.Name(), m.cfg.Database.LogNameFormat) {
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
		"-u"+m.cfg.Database.User,
		"-p"+m.cfg.Database.Password,
		"-h"+m.cfg.Database.Host,
		"-P"+strconv.Itoa(m.cfg.Database.Port),
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

func (m *MariaDB) checkBackupDirExistsAndCreate() (p string, err error) {
	if _, err := os.Stat(m.cfg.Backup.BackupDir); os.IsNotExist(err) {
		err = os.MkdirAll(m.cfg.Backup.BackupDir, os.ModePerm)
		return m.cfg.Backup.BackupDir, err
	}
	return
}

func (m *MariaDB) waitMariaDbUp(timeout time.Duration) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		err := pingMariaDB(m.cfg.Database)
		if err != nil {
			log.Error("Error Pinging mariadb. error: ", err.Error())
			return false, nil
		}
		log.Debug("Pinging mariadb successful")
		return true, nil
	})
	return wait.Poll(5*time.Second, timeout, cf)
}

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

func (m *MariaDB) deleteMariaDBDatabases() (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		for _, d := range m.cfg.Database.Databases {
			log.Debug("deleting database: ", d)
			if err = os.RemoveAll(filepath.Join(m.cfg.Database.DataDir, d)); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if m.cfg.Database.DataDir != "" {
		return wait.Poll(1*time.Second, 30*time.Second, cf)
	}
	return
}

func (m *MariaDB) dropMariaDBDatabases() {
	for _, d := range m.cfg.Database.Databases {
		log.Debug("dropping database: ", d)
		if err := exec.Command("mysqladmin",
			"-u"+m.cfg.Database.User,
			"-p"+m.cfg.Database.Password,
			"-h"+m.cfg.Database.Host,
			"-P"+strconv.Itoa(m.cfg.Database.Port),
			"drop", d,
			"--force",
		).Run(); err != nil {
			log.Error(fmt.Errorf("mysqladmin drop table error: %s", err.Error()))
		}
	}

}

func (m *MariaDB) restartMariaDB() (err error) {
	cmd := exec.Command("mysqladmin",
		"shutdown",
		"-u"+m.cfg.Database.User,
		"-p"+m.cfg.Database.Password,
		"-h"+m.cfg.Database.Host,
		"-P"+strconv.Itoa(m.cfg.Database.Port),
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

func getMyDumpBinlog(p string) (mp mysql.Position, err error) {
	meta := metadata{}
	yamlBytes, err := ioutil.ReadFile(path.Join(p, "/metadata"))
	if err != nil {
		return mp, fmt.Errorf("read config file: %s", err.Error())
	}
	//turn string to valid yaml
	yamlCorrect := strings.ReplaceAll(string(yamlBytes), "\t", "  ")
	r, _ := regexp.Compile("([a-zA-Z])[\\:]([^\\s])")
	err = yaml.Unmarshal([]byte(r.ReplaceAllString(yamlCorrect, `$1: $2`)), &meta)
	if err != nil {
		return mp, fmt.Errorf("parse config file: %s", err.Error())
	}
	mp.Name = meta.Status.Log
	mp.Pos = meta.Status.Pos

	return
}

func getMysqlDumpBinlog(s string) (mp mysql.Position, err error) {
	var rex = regexp.MustCompile("(\\w+)=([^;,]*)")
	data := rex.FindAllStringSubmatch(s, -1)
	res := make(map[string]string)
	for _, kv := range data {
		k := kv[1]
		v := kv[2]
		res[k] = v
	}
	pos, err := strconv.ParseInt(res["MASTER_LOG_POS"], 10, 32)
	mp.Name = res["MASTER_LOG_FILE"]
	mp.Pos = uint32(pos)
	return
}

func (m *MariaDB) GetDatabaseDiff(c1, c2 config.DatabaseConfig) (out []byte, err error) {
	//mysqldiff --server1=root:pw@localhost:3306 --server2=root:pw@db_backup:3306 test:test
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
