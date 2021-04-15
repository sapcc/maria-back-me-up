package storage

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type MariaDBStream struct {
	cfg         config.MariaDBStream
	serviceName string
	statusError map[string]string
}

func NewMariaDBStream(c config.MariaDBStream, serviceName string, logBin string) (m *MariaDBStream, err error) {
	return &MariaDBStream{
		cfg:         c,
		serviceName: serviceName,
	}, nil
}

func (m *MariaDBStream) WriteStream(fileName, mimeType string, body io.Reader, tags map[string]string, dlo bool) error {
	return &Error{Storage: m.cfg.ServiceName, message: "method 'WriteStream' is not implemented"}
}

func (m *MariaDBStream) WriteChannelStream(name, mimeType string, body <-chan StreamEvent, tags map[string]string, dlo bool) (err error) {

	//  (Add some retry logic etc to check if the connection can be re-established)
	//  IP or get Pod IP from k8s
	conn, err := client.Connect(strings.Join([]string{m.cfg.Host, strconv.Itoa(m.cfg.Port)}, ":"), m.cfg.User, m.cfg.Password, "")

	if err != nil {
		return fmt.Errorf("Error connecting to target mariadb: %w", err)
	}
	defer conn.Close()

	for {
		value, ok := <-body
		if !ok {
			return
		}
		switch value.(type) {
		case *BinlogEvent:
			event := value.(*BinlogEvent).Value
			switch event.Header.EventType {
			case replication.QUERY_EVENT:
				queryEvent := event.Event.(*replication.QueryEvent)
				err := conn.UseDB(string(queryEvent.Schema))
				if err != nil {
					switch err := errors.Cause(err).(type) {
					case *mysql.MyError:
						if err.Code == 1049 {
							// Unknown database, unset DB. This can be a `create database` query
							conn.UseDB("")
						} else {
							return fmt.Errorf("cannot change schema: %v", err.Error())
						}
					default:
						return fmt.Errorf("cannot change schema: %v", err.Error())
					}
				}

				result, err := conn.Execute(string(queryEvent.Query))
				if err != nil {
					return fmt.Errorf("execution of query failed: %v", err.Error())
				}

				fmt.Printf("Affected rows %v", result.AffectedRows)

			case replication.MARIADB_ANNOTATE_ROWS_EVENT:
				annotateRowsEvent := event.Event.(*replication.MariadbAnnotateRowsEvent)
				result, err := conn.Execute(string(annotateRowsEvent.Query))
				if err != nil {
					return fmt.Errorf("execution of query failed: %v", err.Error())
				}
				fmt.Printf("Affected rows %v", result.AffectedRows)
			default:
				// Only QueryEvent and MARIADB_ANNOTATE_ROWS_EVENT contain queries which must be replicated
				continue
			}
		case *ByteEvent:
			continue
		default:
			return fmt.Errorf("unexpected type %T", value)
		}
	}
}

func (m *MariaDBStream) GetStorageServiceName() (name string) {
	return m.cfg.ServiceName
}

func (m *MariaDBStream) GetStatusError() map[string]string {
	return m.statusError
}

func (m *MariaDBStream) GetStatusErrorByKey(backupKey string) string {
	if st, ok := m.statusError[path.Dir(backupKey)]; ok {
		return st
	}
	return ""
}

func (m *MariaDBStream) GetSupportedStream() StreamType {
	return CHANNEL_STREAM
}

func (m *MariaDBStream) WriteFolder(p string) (err error) {
	log.Debug("Restore path: ", p)

	if m.cfg.DumpTool == nil || *m.cfg.DumpTool == "mysqldump" {
		dump, err := os.Open(path.Join(p, "dump.sql"))

		if err != nil {
			return fmt.Errorf("could not read dump: %s", err.Error())
		}

		cmd := exec.Command(
			"mysql",
			"--port="+strconv.Itoa(m.cfg.Port),
			"--host="+m.cfg.Host,
			"--user="+m.cfg.User,
			"--password="+m.cfg.Password,
		)
		cmd.Stdin = dump
		b, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("mysql error: %s", string(b))
		}
	} else {
		b, err := exec.Command(
			"myloader",
			"--port="+strconv.Itoa(m.cfg.Port),
			"--host="+m.cfg.Host,
			"--user="+m.cfg.User,
			"--password="+m.cfg.Password,
			"--directory="+path.Join(p, "dump"),
			"--overwrite-tables",
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("myloader error: %s", string(b))
		}
	}

	log.Debug("myloader restore finished")
	return
}

func (m *MariaDBStream) DownloadLatestBackup() (path string, err error) {
	return path, &Error{Storage: m.cfg.ServiceName, message: "method 'DownloadLatestBackup' is not implemented"}
}

func (m *MariaDBStream) GetFullBackups() (bl []Backup, err error) {
	return bl, &Error{Storage: m.cfg.ServiceName, message: "method 'ListFullBackups' is not implemented"}
}

func (m *MariaDBStream) GetIncBackupsFromDump(key string) (bl []Backup, err error) {
	return bl, &Error{Storage: m.cfg.ServiceName, message: "method 'ListIncBackupsFor' is not implemented"}
}

func (m *MariaDBStream) DownloadBackupWithLogPosition(fullBackupPath string, binlog string) (path string, err error) {
	return path, &Error{Storage: m.cfg.ServiceName, message: "method 'DownloadBackupFrom' is not implemented"}
}

func (m *MariaDBStream) DownloadBackup(fullBackup Backup) (path string, err error) {
	return path, &Error{Storage: m.cfg.ServiceName, message: "method 'DownloadBackup' is not implemented"}
}
