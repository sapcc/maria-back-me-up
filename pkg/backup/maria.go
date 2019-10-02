package backup

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/client"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/health"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/writer"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"gopkg.in/yaml.v2"
)

var (
	binlogCancel context.CancelFunc
)

type (
	Maria struct {
		cfg    config.Config
		health *health.Maria
		docker *client.Client
		writer *writer.S3
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

func NewMaria(c config.Config) *Maria {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	s3, err := writer.NewS3(c.S3)
	if err != nil {
		return &Maria{}
	}
	return &Maria{
		cfg:    c,
		docker: cli,
		health: health.NewMaria(c.MariaDB),
		writer: s3,
	}
}

func (m *Maria) RunBackup(ctx context.Context, ch chan<- []string, errCh chan<- error) {
	_, err := m.checkBackupDirExistsAndCreate()
	if err != nil {
		log.Fatal("Cannot create backup folder", err)
	}
	for c := time.Tick(time.Duration(m.cfg.IntervalInSeconds) * time.Second); ; {
		s, err := m.health.Check()
		//Only run backups if db is healthy
		if err == nil && s.Ok {
			err = m.createMysqlDump(ch, errCh)
			if err != nil {
				log.Error(err)
			}
		}
		select {
		case <-c:
			continue
		case <-ctx.Done():
			log.Info("stop backup")
			binlogCancel()
			return
		}
	}
}

func (m *Maria) createMysqlDump(ch chan<- []string, errCh chan<- error) (err error) {
	// Stop binlog
	if binlogCancel != nil {
		binlogCancel()
	}
	dt := time.Now()
	backup := path.Join(m.cfg.BackupDir, dt.Format(time.RFC3339))
	mydumperCmd := exec.Command(
		"mydumper",
		"--port="+strconv.Itoa(m.cfg.MariaDB.Port),
		"--host="+m.cfg.MariaDB.Host,
		"--user="+m.cfg.MariaDB.User,
		"--password="+m.cfg.MariaDB.Password,
		"--outputdir="+backup,
		//"--regex='^(?!(mysql))'",
		"--compress",
	)

	tarPipe := exec.Command("tar", "-zcf", "test.tar.gz", backup)
	tarPipe.Stdin, _ = mydumperCmd.StdoutPipe()
	err = mydumperCmd.Run()
	err = tarPipe.Start()
	err = tarPipe.Wait()
	log.Info(m.writer.WriteFolder(backup))
	if err != nil {
		return
	}

	ctx := context.Background()
	ctx, binlogCancel = context.WithCancel(ctx)
	bp, err := m.readMetadata(backup)
	if err != nil {
		return
	}
	go m.runBinlog(ctx, bp, dt.Format(time.RFC3339), ch, errCh)
	return
}

func (m *Maria) runBinlog(ctx context.Context, mp mysql.Position, dir string, ch chan<- []string, errCh chan<- error) {
	var filename string
	go m.flushLogs(ctx)
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     m.cfg.MariaDB.Host,
		Port:     uint16(m.cfg.MariaDB.Port),
		User:     m.cfg.MariaDB.User,
		Password: m.cfg.MariaDB.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	pr, pw := io.Pipe()
	// Start sync with specified binlog file and position
	streamer, err := syncer.StartSync(mp)
	defer syncer.Close()
	if err != nil {
		log.Fatal("Cannot start binlog stream", err)
	}
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if err == ctx.Err() {
				return
			}
			errCh <- err
		}
		offset := ev.Header.LogPos

		fmt.Println(ev.Header.EventType)
		if ev.Header.EventType == replication.ROTATE_EVENT {
			rotateEvent := ev.Event.(*replication.RotateEvent)
			if ev.Header.Timestamp == 0 || offset == 0 {
				// fake rotate event
				fmt.Println("FAKE", offset, string(rotateEvent.NextLogName))
				continue
			}
			if filename != "" {
				pw.Close()
				time.Sleep(100 * time.Millisecond)
				pr, pw = io.Pipe()
			}
			filename = string(rotateEvent.NextLogName)
			go m.writer.WriteStream(path.Join(dir, filename), "", pr)
			//binlogPath = path.Join("/var/lib/mysql", filename)
			continue
		} else if ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			if filename != "" {
				pw.Write(replication.BinLogFileHeader)
			}
		} else {
			pw.Write(ev.RawData)
		}
		select {
		case <-ctx.Done():
			log.Info("stop binlog streaming")
			return
		default:
			continue
		}
	}
}

func (m *Maria) flushLogs(ctx context.Context) {
	for c := time.Tick(time.Duration(80) * time.Second); ; {
		flushLogs := exec.Command(
			"mysqladmin",
			"flush-logs",
			"--port="+strconv.Itoa(m.cfg.MariaDB.Port),
			"--host="+m.cfg.MariaDB.Host,
			"--user="+m.cfg.MariaDB.User,
			"--password="+m.cfg.MariaDB.Password,
		)
		_, err := flushLogs.CombinedOutput()
		if err != nil {
			log.Error("Error flushing binlogs: ", err)
		}
		select {
		case <-c:
			continue
		case <-ctx.Done():
			log.Info("stop flush")
			return
		}
	}
}

func (m *Maria) readMetadata(p string) (mp mysql.Position, err error) {
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

func (m *Maria) checkBackupDirExistsAndCreate() (p string, err error) {
	if _, err := os.Stat(m.cfg.BackupDir); os.IsNotExist(err) {
		err = os.MkdirAll(m.cfg.BackupDir, os.ModePerm)
		return m.cfg.BackupDir, err
	}
	return
}
