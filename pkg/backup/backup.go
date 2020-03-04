package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/maria"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type (
	Backup struct {
		cfg        config.Config
		storage    *storage.Manager
		flushTimer *time.Timer
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

func NewBackup(c config.Config, sm *storage.Manager) (m *Backup, err error) {
	if err != nil {
		return
	}

	m = &Backup{
		cfg:     c,
		storage: sm,
	}

	return
}

func (b *Backup) createMysqlDump(toPath string) (err error) {
	mydumperCmd := exec.Command(
		"mydumper",
		"--port="+strconv.Itoa(b.cfg.BackupService.MariaDB.Port),
		"--host="+b.cfg.BackupService.MariaDB.Host,
		"--user="+b.cfg.BackupService.MariaDB.User,
		"--password="+b.cfg.BackupService.MariaDB.Password,
		"--outputdir="+toPath,
		//"--regex='^(?!(mysql))'",
		"--compress",
	)

	err = mydumperCmd.Run()
	if err != nil {
		return
	}
	log.Debug("Uploading full backup")
	return b.storage.WriteFolderAll(toPath)
}

func (b *Backup) runBinlog(ctx context.Context, mp mysql.Position, dir string, ch chan error) (err error) {
	var binlogFile string
	cfg := replication.BinlogSyncerConfig{
		ServerID:             999,
		Flavor:               "mariadb",
		Host:                 b.cfg.BackupService.MariaDB.Host,
		Port:                 uint16(b.cfg.BackupService.MariaDB.Port),
		User:                 b.cfg.BackupService.MariaDB.User,
		Password:             b.cfg.BackupService.MariaDB.Password,
		MaxReconnectAttempts: 5,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	binlogReader, binlogWriter := io.Pipe()
	defer func() {
		log.Debug("closing binlog syncer")
		binlogWriter.Close()
		syncer.Close()
		if b.flushTimer != nil {
			b.flushTimer.Stop()
			b.flushTimer = nil
		}
	}()

	// Start sync with specified binlog file and position
	streamer, err := syncer.StartSync(mp)
	if err != nil {
		return fmt.Errorf("Cannot start binlog stream: %w", err)
	}
	b.flushTimer = time.AfterFunc(time.Duration(b.cfg.BackupService.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs("") })
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
				return b.storage.WriteStreamAll(path.Join(dir, binlogFile), "", binlogReader)
			})
			go b.handleWriteErrors(ctx, &eg, ch)

			binlogWriter.Write(replication.BinLogFileHeader)

		}
		binlogWriter.Write(ev.RawData)

		switch ev.Event.(type) {
		case *replication.RowsEvent:
			if b.flushTimer == nil {
				b.flushTimer = time.AfterFunc(time.Duration(b.cfg.BackupService.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs(binlogFile) })
			}
		case *replication.QueryEvent:
			if b.flushTimer == nil {
				b.flushTimer = time.AfterFunc(time.Duration(b.cfg.BackupService.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs(binlogFile) })
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

func (b *Backup) handleWriteErrors(ctx context.Context, eg *errgroup.Group, ch chan error) {
	err := eg.Wait()
	if ctx.Err() != nil {
		close(ch)
		return
	}
	ch <- err
}

func (b *Backup) flushLogs(binlogFile string) (err error) {
	defer func() {
		b.flushTimer = nil
	}()
	flushLogs := exec.Command(
		"mysqladmin",
		"flush-logs",
		"--port="+strconv.Itoa(b.cfg.BackupService.MariaDB.Port),
		"--host="+b.cfg.BackupService.MariaDB.Host,
		"--user="+b.cfg.BackupService.MariaDB.User,
		"--password="+b.cfg.BackupService.MariaDB.Password,
	)
	_, err = flushLogs.CombinedOutput()
	if err != nil {
		log.Error("Error flushing binlogs: ", err)
		return
	}

	if binlogFile != "" {
		return maria.PurgeBinlogsTo(b.cfg.BackupService.MariaDB, binlogFile)
	}
	return
}

func (b *Backup) checkBackupDirExistsAndCreate() (p string, err error) {
	if _, err := os.Stat(b.cfg.BackupService.BackupDir); os.IsNotExist(err) {
		err = os.MkdirAll(b.cfg.BackupService.BackupDir, os.ModePerm)
		return b.cfg.BackupService.BackupDir, err
	}
	return
}
