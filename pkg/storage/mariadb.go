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
	"strings"
	"time"

	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pkg/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"

	// blank import of the mysql parser for pingcap/parser
	_ "github.com/pingcap/parser/test_driver"
	// blank import of mysql driver for database/sql
	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// MariaDBStream struct is ...
type MariaDBStream struct {
	// db connection to the target db
	db          *sql.DB
	cfg         config.MariaDBStream
	databases   map[string]struct{}
	sqlParser   *parser.Parser
	serviceName string
	statusError map[string]string
	tx          *sql.Tx
}

type column struct {
	name  string
	value sql.NullString
	skip  bool
}

// NewMariaDBStream creates a new mariadbstream storage object
func NewMariaDBStream(c config.MariaDBStream, serviceName string) (m *MariaDBStream, err error) {

	db, err := openDBConnection(c.User, c.Password, c.Host, c.Port, serviceName)
	if err != nil {
		return m, fmt.Errorf("failed to init MariaDBStream: %s", err.Error())
	}

	databases := initDatabaseMap(c.Databases)

	var sqlParser *parser.Parser
	if c.ParseSchema {
		log.Info("Parsing SQL for schema is enabled")
		sqlParser = parser.New()
	}

	return &MariaDBStream{
		db:          db,
		cfg:         c,
		databases:   databases,
		sqlParser:   sqlParser,
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
			if m.tx != nil {
				err = m.tx.Commit()
				if ok := errors.Is(err, sql.ErrTxDone); !ok {
					return fmt.Errorf("error committing transaction: %s", err.Error())
				}
				m.tx = nil
			}
			ctx.Done()
			return
		}
		switch e := value.(type) {
		case *BinlogEvent:
			err := m.ProcessBinlogEvent(ctx, e.Value)
			if err != nil {
				return fmt.Errorf("replication of binlog event failed: %s", err.Error())
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
	backupPath := path.Join(p, "dump.sql")

	// Should the full dump be filtered to certain DB schemas?
	if m.cfg.Databases != nil {
		log.Debug("Extracting schemas from full backup")
		backupPath, err = m.extractSchemas(backupPath)
		if err != nil {
			return fmt.Errorf("failed to write folder: %s", err.Error())
		}
	}

	dump, err := os.Open(backupPath)
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

// ProcessBinlogEvent processes the QueryEvents
// - Events other than QueryEvent or RowsEvents are ignored
// - Only replicates all schemas or those set in the config
// - If schemas are filtered and the schema is set the event will be ignored
func (m *MariaDBStream) ProcessBinlogEvent(ctx context.Context, event *replication.BinlogEvent) (err error) {

	switch e := event.Event.(type) {
	case *replication.QueryEvent:
		return m.handleQueryEvent(ctx, e)

	case *replication.RowsEvent:
		return m.handleRowsEvent(ctx, e, event.Header.EventType)

	case *replication.IntVarEvent:
		return m.handleIntVarEvent(ctx, e)

	case *replication.MariadbGTIDEvent:
		if m.tx != nil {
			err = m.tx.Commit()
			if err != nil {
				if ok := errors.Is(err, sql.ErrTxDone); !ok {
					return fmt.Errorf("error committing transaction: %s", err.Error())
				}
			}
		}
		m.tx, err = m.db.BeginTx(ctx, nil)
		return err

	case *replication.XIDEvent:
		err = m.tx.Commit()
		if err != nil {
			if ok := errors.Is(err, sql.ErrTxDone); !ok {
				return fmt.Errorf("error committing transaction: %s", err.Error())
			}
		}
		m.tx = nil
		return

	default:
		// Only QueryEvent and ROWS_EVENT contain queries which must be replicated
		return
	}
}

func (m *MariaDBStream) handleIntVarEvent(ctx context.Context, event *replication.IntVarEvent) (err error) {
	var query string

	if event.Type == replication.INSERT_ID_AUTO_INC {
		query = fmt.Sprintf("SET INSERT_ID=%d;", event.Value)
	} else if event.Type == replication.LAST_INSERT_ID {
		query = fmt.Sprintf("SET LAST_INSERT_ID=%d;", event.Value)
	} else {
		return fmt.Errorf("error IntVarEvent has unsupported type")
	}

	_, err = m.tx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error setting insert id: %s", err.Error())
	}
	return
}

func (m *MariaDBStream) handleQueryEvent(ctx context.Context, event *replication.QueryEvent) (err error) {
	schema := string(event.Schema)
	if m.databases != nil && m.cfg.ParseSchema {
		schema, err = m.determineSchema(event)
		if err != nil {
			return fmt.Errorf("schema could not be determined for '%s': %v", string(event.Query), err.Error())
		}
	}

	if !m.canReplicate(schema) {
		// only schemas specified in the config are replicated
		return
	}

	err = m.replicateQuery(ctx, string(event.Query), schema)
	if err != nil {
		return fmt.Errorf("replication of query failed: %s", err.Error())
	}
	return
}

// replicateQueryEvent replicates the query contained by the event.
// It is not guaranteed that the query uses 'schema.table' syntax.
// The transaction is necessary to ensure all statements are executed on the same db connection.
// In case of a `create [database | schema]` query the `USE SCHEMA` will result in ERROR 1049. Thus unset it with `USE DUMMY`
func (m *MariaDBStream) replicateQuery(ctx context.Context, query string, schema string) (err error) {

	if schema != "" {
		_, err = m.tx.ExecContext(ctx, fmt.Sprintf("use %s;", schema))
		if err != nil {
			switch err := errors.Cause(err).(type) {
			case *mysql.MyError:
				if err.Code == 1049 {
					// Unknown database, unset DB. This can be a `create database` query
					m.tx.ExecContext(ctx, "use dummy")
				} else {
					return fmt.Errorf("cannot change schema: %v", err.Error())
				}
			}
		}
	}
	_, err = m.tx.ExecContext(ctx, query)

	if err != nil {
		return fmt.Errorf("execution of query failed: %v", err.Error())
	}
	return
}

// handleRowsEvent replicates WriteRowEvents, DeleteRowsEvents or UpdateRowsEvents with Version V0,V1 or V2
func (m *MariaDBStream) handleRowsEvent(ctx context.Context, event *replication.RowsEvent, eventType replication.EventType) (err error) {

	if event.Version != 1 {
		return fmt.Errorf("unsupported RowsEvent version: %d", event.Version)
	}

	if !m.canReplicate(string(event.Table.Schema)) {
		// only schemas specified in the config are replicated
		return
	}

	var query string
	var args []interface{}
	var columns, updateColumns []column

	for i, row := range event.Rows {
		if eventType == replication.UPDATE_ROWS_EVENTv1 && i%2 == 1 {
			// update action contains one row for the old values and one for the updated values
			// these are handled together
			continue
		}
		if len(event.SkippedColumns) > i {
			columns = getRowColumns(event.Table, row, event.SkippedColumns[0])
		} else {
			columns = getRowColumns(event.Table, row, []int{})
		}

		switch eventType {
		case replication.WRITE_ROWS_EVENTv1:
			query, args, err = m.createInsertQueryFromRow(event.Table, columns)
			if err != nil {
				return fmt.Errorf("error creating insert query: %s", err.Error())
			}

		case replication.DELETE_ROWS_EVENTv1:
			query, args, err = m.createDeleteQueryFromRow(event.Table, columns)
			if err != nil {
				return fmt.Errorf("error creating delete query: %s", err.Error())
			}

		case replication.UPDATE_ROWS_EVENTv1:
			if len(event.SkippedColumns) > 1 {
				updateColumns = getRowColumns(event.Table, event.Rows[i+1], event.SkippedColumns[1])
			} else {
				updateColumns = getRowColumns(event.Table, event.Rows[i+1], []int{})
			}

			query, args, err = m.createUpdateQueryFromRow(event.Table, columns, updateColumns)
			if err != nil {
				return fmt.Errorf("error creating update query: %s", err.Error())
			}

		default:
			return fmt.Errorf("failed to create query: unsupported rows event %s", eventType)
		}

		_, err := m.tx.Exec(query, args...)
		if err != nil {
			return fmt.Errorf("error replicating query: %s", err.Error())
		}
	}

	return nil
}

// createInsertQueryFromRow returns an INSERT query with placeholders for the VALUES clause.
// Args contains the values for the placeholders.
func (m *MariaDBStream) createInsertQueryFromRow(table *replication.TableMapEvent, columns []column) (query string, args []interface{}, err error) {

	columnExpression := ""
	valuePlaceholders := ""
	for _, col := range columns {
		if col.skip {
			continue
		}
		columnExpression += col.name + ","
		valuePlaceholders += "?,"
		args = append(args, col.value)
	}
	columnExpression = strings.TrimSuffix(columnExpression, ",")
	valuePlaceholders = strings.TrimSuffix(valuePlaceholders, ",")

	query = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);", table.Schema, table.Table, columnExpression, valuePlaceholders)
	return query, args, nil
}

// createDeleteQueryFromRow returns an DELETE query with placeholders for the WHERE clause.
// Args contains the values for the placeholders.
func (m *MariaDBStream) createDeleteQueryFromRow(table *replication.TableMapEvent, columns []column) (query string, args []interface{}, err error) {

	whereCondition, args := createWhereCondition(table, columns)
	query = fmt.Sprintf("DELETE FROM %s.%s WHERE %s;", table.Schema, table.Table, whereCondition)
	return query, args, nil
}

// createUpdateQueryFromRow returns an UPDATE query with placeholders for SET and WHERE clause.
// Args contains the values for the placeholders.
func (m *MariaDBStream) createUpdateQueryFromRow(table *replication.TableMapEvent, columns []column, updateColumns []column) (query string, args []interface{}, err error) {

	setColumns := ""
	for i, col := range updateColumns {
		if col.skip {
			continue
		}
		setColumns += string(table.ColumnName[i]) + " = ?, "
		args = append(args, col.value)
	}

	setColumns = strings.TrimSuffix(setColumns, ", ")

	whereCondition, whereArgs := createWhereCondition(table, columns)
	args = append(args, whereArgs...)
	query = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s;", table.Schema, table.Table, setColumns, whereCondition)

	return query, args, nil
}

// extractSchemas filters the full backup dump.sql and only retains statements for schemas specified in the config
func (m *MariaDBStream) extractSchemas(backupPath string) (filteredBackupPath string, err error) {
	filteredBackupPath = filepath.Join(filepath.Dir(backupPath), "dump_filtered.sql")
	file, err := os.Create(filteredBackupPath)
	if err != nil {
		return "", fmt.Errorf("failed to create file for filtered backup: %s", err.Error())
	}
	defer file.Close()
	bytes, err := m.extractBackupMetadata(backupPath)
	if err != nil {
		return "", fmt.Errorf("could not extract metadata from full backup: %s", err.Error())
	}
	_, err = file.Write(bytes)
	if err != nil {
		return "", fmt.Errorf("failed to write backup metadata: %s", err.Error())
	}

	for _, s := range m.cfg.Databases {
		bytes, err := m.extractSchema(backupPath, s)
		if err != nil {
			return "", fmt.Errorf("could not extract schema %s from full backup: %s", s, err.Error())
		}
		_, err = file.Write(bytes)
		if err != nil {
			return "", fmt.Errorf("failed to write schema %s to file: %s", s, err.Error())
		}
	}

	return filteredBackupPath, nil
}

// getBackupMetadata extracts the backup metadata from the full dump
func (m *MariaDBStream) extractBackupMetadata(path string) (bytes []byte, err error) {
	cmd := exec.Command(
		"sed",
		"-n", "/^-- MariaDB dump /,/^-- Current Database: `/p",
		path,
	)
	out, err := cmd.Output()

	if err != nil {
		return nil, fmt.Errorf("could not extract backup metadata from backup: %s", err.Error())
	}
	return out, nil
}

// extractSchema filters the full backup for all statements related to `schema`
func (m *MariaDBStream) extractSchema(path, schema string) (bytes []byte, err error) {
	cmd := exec.Command(
		"sed",
		"-n", fmt.Sprintf("/^-- Current Database: `%s`/,/^-- Current Database: `/p", schema),
		path,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("could not extract schema %s from backup: %s", schema, err.Error())
	}
	return out, nil
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

// openDBConnection open a DB connection and ping the db
// MaxOpenConns = 5, MaxIdleConns = 5, ConnMaxLifetime = 30 Minutes
func openDBConnection(user, password, host string, port int, serviceName string) (db *sql.DB, err error) {
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@%s:%d", user, password, host, port))
	if err != nil {
		return nil, fmt.Errorf("error opening db %s: %s", serviceName, err.Error())
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping db %s: %s", serviceName, err.Error())
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 15)

	return
}

// initDatabaseMap copies the db names from a slice to a map.
// This map is used to lookup the databases that should be replicated
func initDatabaseMap(dbs []string) map[string]struct{} {
	var dbMap map[string]struct{}
	if dbs != nil {
		dbMap = make(map[string]struct{}, len(dbs))
		for _, database := range dbs {
			dbMap[database] = struct{}{}
		}
	}
	return dbMap
}

type schemaExtractor struct {
	schema string
}

func (s *schemaExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if name, ok := in.(*ast.TableName); ok {
		s.schema = name.Schema.O
		return in, true
	}
	return in, false
}

func (s *schemaExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// canReplicate returns if the schema is on the list of schemas to replicate
func (m *MariaDBStream) canReplicate(schema string) (ok bool) {
	if len(m.databases) > 0 {
		if schema == "" {
			log.Warn("Ignoring query with no schema set, filtering not possible, enable ParseSchema flag")
			return false
		}

		if _, ok := m.databases[schema]; !ok {
			// only scheams specified in the config are replicated
			return false
		}
	}
	return true
}

// determineSchema parses the query to determine the schema. If no schema is found use the event schema.
func (m *MariaDBStream) determineSchema(event *replication.QueryEvent) (string, error) {
	node, err := m.sqlParser.ParseOneStmt(string(event.Query), "", "")
	if err != nil {
		return "", err
	}
	v := &schemaExtractor{}
	node.Accept(v)
	if v.schema != "" {
		return v.schema, nil
	}
	return "", fmt.Errorf("could not determine schema")
}

func newNullString(value interface{}, nullable bool) sql.NullString {

	if value == nil && nullable {
		return sql.NullString{
			String: "",
			Valid:  false,
		}
	}

	switch v := value.(type) {
	case string:
		return sql.NullString{
			String: v,
			Valid:  true,
		}
	case []byte:
		return sql.NullString{
			String: string(v),
			Valid:  true,
		}
	default:
		if v == nil {
			return sql.NullString{
				String: "",
				Valid:  true,
			}
		}
		return sql.NullString{
			String: fmt.Sprintf("%v", v),
			Valid:  true,
		}
	}

}

func createWhereCondition(table *replication.TableMapEvent, columns []column) (condition string, args []interface{}) {

	if len(table.PrimaryKey) > 0 {
		for _, j := range table.PrimaryKey {
			condition += string(table.ColumnName[j]) + " = ? and "
			args = append(args, columns[j].value)
		}
	} else {
		for i, col := range columns {
			if col.skip {
				continue
			}
			condition += string(table.ColumnName[i]) + " = ? and "
			args = append(args, col.value)
		}
	}
	condition = strings.TrimSuffix(condition, " and ")
	return
}

func getRowColumns(table *replication.TableMapEvent, row []interface{}, skipColumns []int) (columns []column) {
	for i, col := range row {
		if ok, nullable := table.Nullable(i); ok {
			columns = append(columns, column{name: string(table.ColumnName[i]), value: newNullString(col, nullable), skip: false})
		} else {
			columns = append(columns, column{name: string(table.ColumnName[i]), value: newNullString(col, false), skip: false})
		}
	}

	for _, i := range skipColumns {
		columns[i].skip = true
	}
	return columns
}
