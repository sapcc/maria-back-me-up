/**
 * Copyright 2021 SAP SE
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

package storage

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"

	// blank import of mysql driver for database/sql
	_ "github.com/siddontang/go-mysql/driver"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

// MariaDBStream struct is ...
type MariaDBStream struct {
	db          *sql.DB
	cfg         config.MariaDBStream
	serviceName string
	statusError map[string]string
}

// NewMariaDBStream creates a new mariadbstream storage object
func NewMariaDBStream(c config.MariaDBStream, serviceName string) (m *MariaDBStream, err error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@%s:%d", c.User, c.Password, c.Host, c.Port))
	if err != nil {
		return nil, fmt.Errorf("error opening db %s: %s", serviceName, err.Error())
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping db %s: %s", serviceName, err.Error())
	}

	return &MariaDBStream{
		db:          db,
		cfg:         c,
		serviceName: serviceName,
	}, nil
}

// WriteStream implements interface
func (m *MariaDBStream) WriteStream(fileName, mimeType string, body io.Reader, tags map[string]string, dlo bool) error {
	return &Error{Storage: m.cfg.Name, message: "method 'WriteStream' is not implemented"}
}

// WriteChannel implements interface
func (m *MariaDBStream) WriteChannel(name, mimeType string, body <-chan StreamEvent, tags map[string]string, dlo bool) (err error) {
	ctx := context.Background()
	for {
		value, ok := <-body
		if !ok {
			ctx.Done()
			return
		}
		switch value.(type) {
		case *BinlogEvent:
			event := value.(*BinlogEvent).Value
			switch event.Header.EventType {
			case replication.QUERY_EVENT:
				queryEvent := event.Event.(*replication.QueryEvent)
				err = replicateQueryEvent(ctx, m.db, queryEvent)
				if err != nil {
					return fmt.Errorf("replication of query failed: %s", err.Error())
				}

			case replication.MARIADB_ANNOTATE_ROWS_EVENT:
				annotateRowsEvent := event.Event.(*replication.MariadbAnnotateRowsEvent)
				_, err := m.db.Exec(string(annotateRowsEvent.Query))
				if err != nil {
					return fmt.Errorf("execution of query failed: %v", err.Error())
				}
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

// GetStorageServiceName implements interface
func (m *MariaDBStream) GetStorageServiceName() (name string) {
	return m.cfg.Name
}

// GetStatusError implements interface
func (m *MariaDBStream) GetStatusError() map[string]string {
	return m.statusError
}

// GetStatusErrorByKey implements interface
func (m *MariaDBStream) GetStatusErrorByKey(backupKey string) string {
	if st, ok := m.statusError[path.Dir(backupKey)]; ok {
		return st
	}
	return ""
}

// WriteFolder implements interface
func (m *MariaDBStream) WriteFolder(p string) (err error) {
	log.Debug("SQL dump path: ", p)

	dump, err := os.Open(path.Join(p, "dump.sql"))
	if err != nil {
		return fmt.Errorf("could not read dump: %s", err.Error())
	}

	switch m.cfg.DumpTool {
	case config.Mysqldump:
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
	case config.MyDumper:
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
	default:
		return fmt.Errorf("unsupported dump tool '%s'", m.cfg.DumpTool)
	}

	log.Debug("myloader restore finished")
	return
}

// replicateQueryEvent replicates the query contained by the event.
// It is not guaranteed that the query uses 'schema.table' syntax.
// The transaction is necessary to ensure all statements are executed on the same db connection.
// In case of a `create [database | schema]` query the `USE SCHEMA` will result in ERROR 1049. Thus unset it with `USE DUMMY`
func replicateQueryEvent(ctx context.Context, db *sql.DB, event *replication.QueryEvent) (err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("cannot get db connection: %s", err.Error())
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "use "+string(event.Schema))
	if err != nil {
		switch err := errors.Cause(err).(type) {
		case *mysql.MyError:
			if err.Code == 1049 {
				// Unknown database, unset DB. This can be a `create database` query
				conn.ExecContext(ctx, "use dummy")
			} else {
				return fmt.Errorf("cannot change schema: %v", err.Error())
			}
		}
	}
	_, err = conn.ExecContext(ctx, string(event.Query))

	if err != nil {
		return fmt.Errorf("execution of query failed: %v", err.Error())
	}
	return
}

// DownloadLatestBackup implements interface
func (m *MariaDBStream) DownloadLatestBackup() (path string, err error) {
	return path, &Error{Storage: m.cfg.Name, message: "method 'DownloadLatestBackup' is not implemented"}
}

// GetFullBackups implements interface
func (m *MariaDBStream) GetFullBackups() (bl []Backup, err error) {
	return bl, &Error{Storage: m.cfg.Name, message: "method 'ListFullBackups' is not implemented"}
}

// GetIncBackupsFromDump implements interface
func (m *MariaDBStream) GetIncBackupsFromDump(key string) (bl []Backup, err error) {
	return bl, &Error{Storage: m.cfg.Name, message: "method 'ListIncBackupsFor' is not implemented"}
}

// DownloadBackupWithLogPosition implements interface
func (m *MariaDBStream) DownloadBackupWithLogPosition(fullBackupPath string, binlog string) (path string, err error) {
	return path, &Error{Storage: m.cfg.Name, message: "method 'DownloadBackupFrom' is not implemented"}
}

// DownloadBackup implements interface
func (m *MariaDBStream) DownloadBackup(fullBackup Backup) (path string, err error) {
	return path, &Error{Storage: m.cfg.Name, message: "method 'DownloadBackup' is not implemented"}
}
