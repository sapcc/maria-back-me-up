package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/docker/docker/client"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
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
		updateSts  *updateStatus
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

func NewBackup(c config.Config, s storage.Storage, us *updateStatus) (m *Backup, err error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return
	}

	m = &Backup{
		cfg:       c,
		docker:    cli,
		storage:   s,
		updateSts: us,
	}

	return
}

func (b *Backup) createMysqlDump(toPath string) (err error) {
	for _, v := range b.storage.GetRemoteStorageServices() {
		b.updateSts.fullBackup[v] = 0
	}
	errs := 0
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
	for i := range b.storage.GetRemoteStorageServices() {
		if err := b.storage.WriteFolder(i, toPath); err != nil {
			log.Error(err)
			errs++
		} else {
			b.updateSts.Lock()
			b.updateSts.fullBackup[b.storage.GetRemoteStorageServices()[i]] = 1
			b.updateSts.Unlock()
		}
	}
	if errs == 2 {
		return errors.New("Writing to both storages failed")
	}

	log.Debug("Done uploading full backup")
	return
}

func (b *Backup) runBinlog(ctx context.Context, mp mysql.Position, dir string, ch chan error) (err error) {
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
	binlogReader, binlogWriter := io.Pipe()
	copyReader, copyWriter := io.Pipe()
	//copy binlog writer for second storage
	copiedReader := io.TeeReader(binlogReader, copyWriter)
	defer func() {
		log.Debug("closing syncer")
		binlogWriter.Close()
		copyWriter.Close()
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
	b.flushTimer = time.AfterFunc(time.Duration(b.cfg.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs("") })
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
			// FormateDescriptionEvent is the first event in binlog, we will close old writer and create new ones
			if binlogFile != "" {
				binlogWriter.Close()
				copyWriter.Close()
				time.Sleep(100 * time.Millisecond)
			}
			binlogReader, binlogWriter = io.Pipe()
			copyReader, copyWriter = io.Pipe()
			copiedReader = io.TeeReader(binlogReader, copyWriter)
			var eg errgroup.Group
			eg.Go(func() error {
				return b.storage.WriteStream(0, path.Join(dir, binlogFile), "", copyReader)
			})
			eg.Go(func() error {
				return b.storage.WriteStream(1, path.Join(dir, binlogFile), "", copiedReader)
			})
			go func() error {
				return b.handleWriteErrors(ctx, &eg, ch)
			}()

			binlogWriter.Write(replication.BinLogFileHeader)

		}
		binlogWriter.Write(ev.RawData)

		switch ev.Event.(type) {
		case *replication.RowsEvent:
			if b.flushTimer == nil {
				b.flushTimer = time.AfterFunc(time.Duration(b.cfg.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs(binlogFile) })
			}
		case *replication.QueryEvent:
			if b.flushTimer == nil {
				b.flushTimer = time.AfterFunc(time.Duration(b.cfg.IncrementalBackupIntervalInMinutes)*time.Minute, func() { b.flushLogs(binlogFile) })
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

func (b *Backup) handleWriteErrors(ctx context.Context, eg *errgroup.Group, ch chan error) (err error) {
	if errs := eg.Wait(); len(errs) > 0 {
		b.updateSts.Lock()
		defer b.updateSts.Unlock()
		for _, i := range b.storage.GetRemoteStorageServices() {
			b.updateSts.incBackup[i] = 1
		}
		for i := 0; i < len(errs); i++ {
			switch e := errs[i].(type) {
			case *storage.StorageError:
				b.updateSts.incBackup[e.Storage] = 0
			default:
				log.Error(errs[i])
			}
			if i == 1 {
				return err
			}
		}
	}
	if ctx.Err() == nil {
		ch <- nil
	} else {
		ch <- nil
		close(ch)
	}
	return nil
}

func (b *Backup) flushLogs(binlogFile string) (err error) {
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

	if binlogFile != "" {
		return purgeBinlogsTo(b.cfg.MariaDB, binlogFile)
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
