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

	"github.com/docker/docker/client"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/sync/errgroup"
)

const (
	fullBackup = 0
	incBackup  = 2
)

type (
	Backup struct {
		cfg        config.Config
		docker     *client.Client
		storage    storage.Storage
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

func NewBackup(c config.Config, s storage.Storage) (m *Backup, err error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return
	}

	m = &Backup{
		cfg:     c,
		docker:  cli,
		storage: s,
	}

	return
}

func (b *Backup) createMysqlDump(toPath string) (err error) {
	mydumperCmd := exec.Command(
		"mydumper",
		"--port="+strconv.Itoa(b.cfg.MariaDB.Port),
		"--host="+b.cfg.MariaDB.Host,
		"--user="+b.cfg.MariaDB.User,
		"--password="+b.cfg.MariaDB.Password,
		"--outputdir="+toPath,
		//"--regex='^(?!(mysql))'",
		"--compress",
	)

	err = mydumperCmd.Run()
	if err != nil {
		return
	}
	log.Debug("Uploading full backup")
	if err = b.storage.WriteFolder(toPath); err != nil {
		return
	}
	log.Debug("Done uploading full backup")
	return
}

func (b *Backup) runBinlog(ctx context.Context, mp mysql.Position, dir string, c chan time.Time) (err error) {
	var binlogFile string
	cfg := replication.BinlogSyncerConfig{
		ServerID: 999,
		Flavor:   "mariadb",
		Host:     b.cfg.MariaDB.Host,
		Port:     uint16(b.cfg.MariaDB.Port),
		User:     b.cfg.MariaDB.User,
		Password: b.cfg.MariaDB.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	pr, pw := io.Pipe()
	defer func() {
		pw.Close()
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
	b.flushTimer = time.AfterFunc(time.Duration(b.cfg.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs() })
	for {
		ev, inerr := streamer.GetEvent(ctx)
		if inerr != nil {
			if inerr == ctx.Err() {
				return nil
			}
			return fmt.Errorf("Error reading binlog stream: %w", err)
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
			// FormateDescriptionEvent is the first event in binlog, we will close old one and create a new log file
			if binlogFile != "" {
				pw.Close()
				time.Sleep(100 * time.Millisecond)
			}
			pr, pw = io.Pipe()
			var eg errgroup.Group
			eg.Go(func() error {
				return b.storage.WriteStream(path.Join(dir, binlogFile), "", pr)
			})
			go func() error {
				if err = eg.Wait(); err != nil {
					return err
				}
				if ctx.Err() == nil {
					c <- time.Now()
				} else {
					c <- time.Now()
					close(c)
				}
				return nil
			}()

			pw.Write(replication.BinLogFileHeader)

		}
		pw.Write(ev.RawData)

		switch ev.Event.(type) {
		case *replication.RowsEvent:
			if b.flushTimer == nil {
				b.flushTimer = time.AfterFunc(time.Duration(b.cfg.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs() })
			}
		case *replication.QueryEvent:
			if b.flushTimer == nil {
				b.flushTimer = time.AfterFunc(time.Duration(b.cfg.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs() })
			}
		}

		select {
		case <-ctx.Done():
			if b.flushTimer != nil {
				b.flushTimer.Stop()
				b.flushTimer = nil
			}
			log.Info("stop binlog streaming")
			return
		default:
			continue
		}
	}
}

func (b *Backup) flushLogs() (err error) {
	defer func() {
		b.flushTimer = nil
	}()
	flushLogs := exec.Command(
		"mysqladmin",
		"flush-logs",
		"--port="+strconv.Itoa(b.cfg.MariaDB.Port),
		"--host="+b.cfg.MariaDB.Host,
		"--user="+b.cfg.MariaDB.User,
		"--password="+b.cfg.MariaDB.Password,
	)
	_, err = flushLogs.CombinedOutput()
	if err != nil {
		log.Error("Error flushing binlogs: ", err)
		return
	}
	return
}

func (b *Backup) checkBackupDirExistsAndCreate() (p string, err error) {
	if _, err := os.Stat(b.cfg.BackupDir); os.IsNotExist(err) {
		err = os.MkdirAll(b.cfg.BackupDir, os.ModePerm)
		return b.cfg.BackupDir, err
	}
	return
}
