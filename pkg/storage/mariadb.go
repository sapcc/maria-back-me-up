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
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"

	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"

	pcerrors "github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pkg/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"

	// blank import of the mysql parser for pingcap/parser
	_ "github.com/pingcap/parser/test_driver"

	// blank import of mysql driver for database/sql
	_ "github.com/go-sql-driver/mysql"
)

// MariaDBStream struct is ...
type MariaDBStream struct {
	// db connection to the target db
	db            *sql.DB
	cfg           config.MariaDBStream
	logger        *logrus.Entry
	databases     map[string]struct{}
	sqlParser     *parser.Parser
	serviceName   string
	statusError   map[string]string
	retry         *retryHandler
	tableMetadata map[string]map[string]map[int]columnDefinition
}

type column struct {
	name       string
	value      sql.NullString
	skip       bool
	isKeyField bool
}

// schemaFilter defines how to filter out a part of the dump file
type schemaFilter struct {
	start         *regexp.Regexp
	end           *regexp.Regexp
	hasStarted    bool
	canMultiMatch bool
	isDone        bool
}

// NewMariaDBStream creates a new mariadbstream storage object
func NewMariaDBStream(c config.MariaDBStream, serviceName string) (m *MariaDBStream, err error) {

	db, err := openDBConnection(c.User, c.Password, c.Host, c.Port, serviceName)
	if err != nil {
		return m, fmt.Errorf("failed to init MariaDBStream: %s", err.Error())
	}

	databases := initDatabaseMap(c.Databases)

	tableMetadata, err := queryTableMetadata(c, db)
	if err != nil {
		return nil, err
	}
	var sqlParser *parser.Parser
	if c.ParseSchema {
		logger.Info("Parsing SQL for schema is enabled")
		sqlParser = parser.New()
	}

	return &MariaDBStream{
		db:            db,
		cfg:           c,
		logger:        logger.WithField("service", serviceName),
		databases:     databases,
		sqlParser:     sqlParser,
		serviceName:   serviceName,
		retry:         newRetryHandler(db, &c, serviceName),
		tableMetadata: tableMetadata,
	}, nil

}

// Verify implements interface
func (m *MariaDBStream) Verify() bool {
	return false
}

// WriteStream implements interface
func (m *MariaDBStream) WriteStream(fileName, mimeType string, body io.Reader, tags map[string]string, dlo bool) error {
	return &Error{Storage: m.cfg.Name, message: "method 'WriteStream' is not implemented"}
}

// WriteChannel implements interface
func (m *MariaDBStream) WriteChannel(name, mimeType string, body <-chan StreamEvent, tags map[string]string, dlo bool) (err error) {
	ctx := context.Background()

	defer func() error {
		if err := m.retry.commit(ctx); err != nil {
			return fmt.Errorf("error committing transaction: %s", err.Error())
		}
		return nil
	}()

	for {
		value, ok := <-body
		if !ok {
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

// GetTotalIncBackupsFromDump implements interface
func (m *MariaDBStream) GetTotalIncBackupsFromDump(key string) (t int, err error) {
	return
}

// WriteFolder implements interface
func (m *MariaDBStream) WriteFolder(p string) (err error) {

	switch m.cfg.DumpTool {
	case config.Mysqldump:
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
		log.Debug("mysql restore finished")
	case config.MyDumper:
		err = filterMyDumperBackupDir(p, m.cfg.Databases)

		b, err := exec.Command(
			"myloader",
			"--port="+strconv.Itoa(m.cfg.Port),
			"--host="+m.cfg.Host,
			"--user="+m.cfg.User,
			"--password="+m.cfg.Password,
			"--directory="+p,
			"--overwrite-tables",
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("myloader error: %s", string(b))
		}
		log.Debug("myloader restore finished")
	default:
		return fmt.Errorf("unsupported dump tool '%s'", m.cfg.DumpTool)
	}

	cmd := exec.Command(
		"mysqlcheck",
		"--port="+strconv.Itoa(m.cfg.Port),
		"--host="+m.cfg.Host,
		"--user="+m.cfg.User,
		"--password="+m.cfg.Password,
		"--analyze",
		"--databases", strings.Join(m.cfg.Databases, " "),
	)
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mysql error: %s", string(b))
	}
	log.Debug("mysqlcheck finished analyzing tables")
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
		return m.retry.beginTx(ctx)

	case *replication.XIDEvent:
		return m.retry.commit(ctx)

	case *replication.RotateEvent:
		return m.retry.commit(ctx)

	default:
		// Only QueryEvent and ROWS_EVENT contain queries which must be replicated
		return
	}
}

func (m *MariaDBStream) handleIntVarEvent(ctx context.Context, event *replication.IntVarEvent) (err error) {
	var query string

	if event.Type == replication.INSERT_ID {
		query = fmt.Sprintf("SET INSERT_ID=%d;", event.Value)
	} else if event.Type == replication.LAST_INSERT_ID {
		query = fmt.Sprintf("SET LAST_INSERT_ID=%d;", event.Value)
	} else {
		return fmt.Errorf("error IntVarEvent has unsupported type")
	}

	err = m.retry.execContext(ctx, query, nil)
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
		err = m.retry.execContext(ctx, fmt.Sprintf("use %s;", schema), nil)
		if err != nil {
			switch err := errors.Cause(err).(type) {
			case *mysql.MySQLError:
				if err.Number == 1049 {
					// Unknown database, unset DB. This can be a `create database` query
					m.retry.execContext(ctx, "use dummy;", nil)
				} else {
					return fmt.Errorf("cannot change schema: %v", err.Error())
				}
			}
		}
	}
	err = m.retry.execContext(ctx, query, nil)

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
	var hasPrimaryKey bool

	for i, row := range event.Rows {
		if eventType == replication.UPDATE_ROWS_EVENTv1 && i%2 == 1 {
			// update action contains one row for the old values and one for the updated values
			// these are handled together
			continue
		}

		if hasRowMetadata(event) {
			if hasFullRowImage(event) {
				columns, hasPrimaryKey = getRowColumnsTableEvent(event.Table, row, []int{})
			} else {
				columns, hasPrimaryKey = getRowColumnsTableEvent(event.Table, row, event.SkippedColumns[0])
			}
		} else {
			if hasFullRowImage(event) {
				columns, hasPrimaryKey = getRowColumnsTableMetadata(m.tableMetadata[string(event.Table.Schema)][string(event.Table.Table)], row, []int{})
			} else {
				columns, hasPrimaryKey = getRowColumnsTableMetadata(m.tableMetadata[string(event.Table.Schema)][string(event.Table.Table)], row, event.SkippedColumns[0])
			}
		}

		switch eventType {
		case replication.WRITE_ROWS_EVENTv1:
			query, args, err = m.createInsertQueryFromRow(event.Table, columns)
			if err != nil {
				return fmt.Errorf("error creating insert query: %s", err.Error())
			}

		case replication.DELETE_ROWS_EVENTv1:
			query, args, err = m.createDeleteQueryFromRow(event.Table, columns, hasPrimaryKey)
			if err != nil {
				return fmt.Errorf("error creating delete query: %s", err.Error())
			}

		case replication.UPDATE_ROWS_EVENTv1:
			if hasRowMetadata(event) {
				if hasFullRowImage(event) {
					updateColumns, hasPrimaryKey = getRowColumnsTableEvent(event.Table, event.Rows[i+1], []int{})
				} else {
					updateColumns, hasPrimaryKey = getRowColumnsTableEvent(event.Table, event.Rows[i+1], event.SkippedColumns[1])
				}
			} else {
				if hasFullRowImage(event) {
					updateColumns, hasPrimaryKey = getRowColumnsTableMetadata(m.tableMetadata[string(event.Table.Schema)][string(event.Table.Table)], event.Rows[i+1], []int{})
				} else {
					updateColumns, hasPrimaryKey = getRowColumnsTableMetadata(m.tableMetadata[string(event.Table.Schema)][string(event.Table.Table)], event.Rows[i+1], event.SkippedColumns[1])
				}
			}

			query, args, err = m.createUpdateQueryFromRow(event.Table, columns, updateColumns, hasPrimaryKey)
			if err != nil {
				return fmt.Errorf("error creating update query: %s", err.Error())
			}

		default:
			return fmt.Errorf("failed to create query: unsupported rows event %s", eventType)
		}

		err := m.retry.execContext(ctx, query, args)
		if err != nil {
			return fmt.Errorf("error replicating RowsEvent: %s", err.Error())
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
func (m *MariaDBStream) createDeleteQueryFromRow(table *replication.TableMapEvent, columns []column, hasPrimaryKey bool) (query string, args []interface{}, err error) {

	whereCondition, args := createWhereCondition(columns, hasPrimaryKey)
	query = fmt.Sprintf("DELETE FROM %s.%s WHERE %s;", table.Schema, table.Table, whereCondition)
	return query, args, nil
}

// createUpdateQueryFromRow returns an UPDATE query with placeholders for SET and WHERE clause.
// Args contains the values for the placeholders.
func (m *MariaDBStream) createUpdateQueryFromRow(table *replication.TableMapEvent, columns []column, updateColumns []column, hasPrimaryKey bool) (query string, args []interface{}, err error) {

	setColumns := ""
	for _, col := range updateColumns {
		if col.skip {
			continue
		}
		setColumns += col.name + " = ?, "
		args = append(args, col.value)
	}

	setColumns = strings.TrimSuffix(setColumns, ", ")

	whereCondition, whereArgs := createWhereCondition(columns, hasPrimaryKey)
	args = append(args, whereArgs...)
	query = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s;", table.Schema, table.Table, setColumns, whereCondition)

	return query, args, nil
}

// extractSchemas filters the full backup dump.sql and only retains statements for schemas specified in the config
func (m *MariaDBStream) extractSchemas(backupPath string) (filteredBackupPath string, err error) {
	filteredBackupPath = filepath.Join(filepath.Dir(backupPath), "dump_filtered.sql")

	filters := make([]schemaFilter, 0)

	// filter to retrieve dump metadata
	metadataStart := regexp.MustCompile("-- MariaDB dump.*")
	metadataEnd := regexp.MustCompile("-- Current Database:.*")
	metadataFilter := schemaFilter{start: metadataStart, end: metadataEnd, canMultiMatch: false}
	filters = append(filters, metadataFilter)

	// creates a filter for each schema to be restored
	for _, s := range m.cfg.Databases {
		schemaStart := regexp.MustCompile(fmt.Sprintf("-- Current Database: `%s`", s))
		schemaEnd := regexp.MustCompile("-- Current Database:.*")
		schemaFilter := schemaFilter{start: schemaStart, end: schemaEnd, canMultiMatch: true}
		filters = append(filters, schemaFilter)
	}

	// filter the dump file based on the filter provided
	err = m.filterDump(backupPath, filteredBackupPath, filters)
	if err != nil {
		return "", err
	}

	return filteredBackupPath, nil
}

func (m *MariaDBStream) filterDump(path, targetPath string, filters []schemaFilter) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	target, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create file for filtered backup: %s", err.Error())
	}
	defer target.Close()

	maxCapacity := m.cfg.DumpFilterBufferSizeMB * 1024 * 1024

	scanner := bufio.NewScanner(file)

	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {

		line := scanner.Bytes()
		for i, f := range filters {
			if f.isDone {
				continue
			}
			if f.hasStarted {
				if matched := f.end.Match(line); matched {
					if f.canMultiMatch {
						filters[i].hasStarted = false
					} else {
						filters[i].isDone = true
					}
				} else {
					if _, err = target.Write(line); err != nil {
						return err
					}
					if _, err = target.Write([]byte{'\n'}); err != nil {
						return err
					}
				}
			} else {
				matched := f.start.Match(line)
				if matched {
					filters[i].hasStarted = true
					if _, err = target.Write(line); err != nil {
						return err
					}
					if _, err = target.Write([]byte{'\n'}); err != nil {
						return err
					}
				}
			}
		}
	}
	return scanner.Err()
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

	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port))
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

func createWhereCondition(columns []column, hasPrimaryKey bool) (condition string, args []interface{}) {

	if hasPrimaryKey {
		for _, col := range columns {
			if col.isKeyField {
				condition += col.name + " = ? and "
				args = append(args, col.value)
			}
		}
	} else {
		for _, col := range columns {
			if col.skip {
				continue
			}
			condition += col.name + " = ? and "
			args = append(args, col.value)
		}
	}
	condition = strings.TrimSuffix(condition, " and ")
	return
}

// getRowColumnsTableEvent combines the values from the row with the column defintion from the TableMapEvent
func getRowColumnsTableEvent(table *replication.TableMapEvent, row []interface{}, skipColumns []int) (columns []column, hasPrimaryKey bool) {
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

	if len(table.PrimaryKey) > 0 {
		hasPrimaryKey = true

		for _, i := range table.PrimaryKey {
			columns[i].isKeyField = true
		}
	}

	return columns, hasPrimaryKey
}

// getRowColumnsTableMetadata combines the values from the row with the column defintion from the TableMetadata
func getRowColumnsTableMetadata(metadata map[int]columnDefinition, row []interface{}, skipColumns []int) (columns []column, hasPrimaryKey bool) {

	for i, col := range row {
		columns = append(columns, column{name: metadata[i+1].name, value: newNullString(col, metadata[i+1].isNullable), skip: false, isKeyField: metadata[i+1].isKey})
		if metadata[i+1].isKey {
			hasPrimaryKey = true
		}
	}
	for _, i := range skipColumns {
		columns[i].skip = true
	}

	return columns, hasPrimaryKey
}

type columnDefinition struct {
	name       string
	isKey      bool
	isNullable bool
}

// hasRowMetadata returns true if the TableMapEvent of the RowsEvent contains column names
func hasRowMetadata(event *replication.RowsEvent) bool {
	return len(event.Table.ColumnName) > 0
}

// hasFullRowImage returns true if the RowsEvent does not have SkippedColumns
func hasFullRowImage(event *replication.RowsEvent) bool {
	return len(event.SkippedColumns) == 0
}

// queryTableMetadata queries the table information needed to replicate RowsEvents.
// This metadata is only used if the Main Server does not have `binlog_row_metadata=FULL`
func queryTableMetadata(cfg config.MariaDBStream, db *sql.DB) (metadata map[string]map[string]map[int]columnDefinition, err error) {

	metadata = make(map[string]map[string]map[int]columnDefinition)
	for _, schema := range cfg.Databases {
		query := fmt.Sprintf("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_KEY, ORDINAL_POSITION, IS_NULLABLE from information_schema.COLUMNS where TABLE_SCHEMA = '%s';", schema)

		rows, err := db.Query(query)
		if err != nil {
			return metadata, fmt.Errorf("error querying info schema: %s", err.Error())
		}
		defer rows.Close()

		var tableName, columnName, columnKey, nullable string
		var ordinalPosition int

		tableMetadata := make(map[string]map[int]columnDefinition)

		for rows.Next() {
			err := rows.Scan(&tableName, &columnName, &columnKey, &ordinalPosition, &nullable)
			if err != nil {
				return metadata, fmt.Errorf("error parsing info schema results: %s", err.Error())
			}

			isKey := false
			if columnKey != "" {
				isKey = true
			}

			isNullable := false
			if nullable == "YES" {
				isNullable = true
			}

			if _, ok := tableMetadata[tableName]; !ok {
				tableMetadata[tableName] = make(map[int]columnDefinition)
			}

			tableMetadata[tableName][ordinalPosition] = columnDefinition{columnName, isKey, isNullable}
		}
		metadata[schema] = tableMetadata
	}

	return metadata, err
}

// filterMyDumperBackupDir removes unwanted backup files
// keeps `metadata` file of mydumper backup
// keeps all files starting with one of the defined DB names
func filterMyDumperBackupDir(path string, databases []string) error {
	if databases == nil {
		return nil
	}

	pattern := ".*/(metadata|"
	for _, db := range databases {
		pattern += db + "|"
	}
	pattern = strings.TrimSuffix(pattern, "|") + ")"

	files, err := filepath.Glob(fmt.Sprintf("%s/*", path))
	if err != nil {
		return fmt.Errorf("could not list db directory")
	}
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("could not compile regex '%s'", pattern)
	}

	for _, file := range files {
		if !regex.MatchString(file) {
			err := os.Remove(file)
			if err != nil {
				return fmt.Errorf("could not remove file '%s'", file)
			}
		}
	}
	return nil
}

// myQuery holds a query and its optional arguments
type myQuery struct {
	query string
	args  []interface{}
}

// isDBUp tries for 90 seconds to connect back with the DB then quits
func isDBUp(user, password, host string, port int) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		if err = pingMariaDB(user, password, host, port); err != nil {
			log.Warn(fmt.Sprintf("error pinging mariadb: %s", err.Error()))
			return false, nil
		}
		log.Debug("pinging mariadb successful")
		return true, nil
	})
	if err = wait.Poll(5*time.Second, time.Duration(90)*time.Second, cf); err != nil {
		return fmt.Errorf("pinging target mariadb failed. aborting tx retry")
	}
	return nil
}

func pingMariaDB(user, password, host string, port int) (err error) {
	var out []byte
	if out, err = exec.Command("mysqladmin",
		"status",
		"-u"+user,
		"-p"+password,
		"-h"+host,
		"-P"+strconv.Itoa(port),
	).CombinedOutput(); err != nil {
		var msg string
		if out != nil {
			msg = string(out) // Stdout/Stderr
		} else {
			msg = err.Error() // Error message if command cannot be executed
		}
		return fmt.Errorf("mysqladmin status error: %s", msg)
	}
	return
}

// isRetryable returns true if err is a retryable error
func isRetryable(err error) bool {
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	if errors.Is(err, unix.ECONNREFUSED) {
		return true
	}

	switch pcerrors.Cause(err).(type) {
	case *mysql.MySQLError:
		if mysqlErr, ok := pcerrors.Cause(err).(*mysql.MySQLError); ok {
			if mysqlErr.Number == 1053 || mysqlErr.Number == 1927 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

type retryHandler struct {
	db          *sql.DB
	cfg         *config.MariaDBStream
	tx          *sql.Tx
	queries     []myQuery
	serviceName string
}

func newRetryHandler(db *sql.DB, cfg *config.MariaDBStream, serviceName string) (rh *retryHandler) {
	return &retryHandler{db: db, cfg: cfg, tx: nil}
}

func (r *retryHandler) beginTx(ctx context.Context) (err error) {

	if err = r.commit(ctx); err != nil {
		return err
	}

	r.tx, err = r.db.BeginTx(ctx, nil)
	if err == nil {
		return
	}

	if isRetryable(err) {
		if err := isDBUp(r.cfg.User, r.cfg.Password, r.cfg.Host, r.cfg.Port); err != nil {
			return err
		}

		r.tx, err = r.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("cannot create tx: %s", err)
		}
		return nil
	}
	return fmt.Errorf("non-retryable error cannot create tx: %s", err)

}

func (r *retryHandler) commit(ctx context.Context) (err error) {
	if r.tx == nil {
		return
	}

	// reset tx and queries if successful or not
	defer func() {
		r.tx.Rollback()
		r.tx = nil
		r.queries = nil
	}()

	err = r.tx.Commit()
	if err != nil {
		if isRetryable(err) {
			if err = r.retryTx(ctx); err != nil {
				return fmt.Errorf("commit retry error: %s", err.Error())
			}
			if err = r.tx.Commit(); err != nil {
				return fmt.Errorf("error committing retried transaction: %s", err.Error())
			}
		}
		if ok := errors.Is(err, sql.ErrTxDone); !ok {
			return fmt.Errorf("non-retryable error committing transaction: %s", err.Error())
		}
	}
	return
}

func (r *retryHandler) execContext(ctx context.Context, query string, args []interface{}) (err error) {
	if r.tx != nil {
		r.queries = append(r.queries, myQuery{query: query, args: args})
		_, err = r.tx.ExecContext(ctx, query, args...)
		if err == nil {
			return
		}
		r.tx.Rollback()

		if isRetryable(err) {
			if err = r.retryTx(ctx); err != nil {
				log.Info("failed to retry tx:", err.Error())
				return err
			}
			log.Info("retried tx successfully")
			return
		}
		log.Info("non-retryable tx error:", err.Error())
		return
	}

	_, err = r.db.ExecContext(ctx, query, args...)
	if err != nil {
		if isRetryable(err) {
			if err = r.retryQuery(ctx, query, args); err != nil {
				log.Info("failed to retry query:", err.Error())
				return err
			}
			log.Info("retried query successfully")
			return
		}
		log.Info("non-retryable query error: ", err.Error())
		return err
	}
	return nil
}

func (r *retryHandler) retryTx(ctx context.Context) (err error) {
	if err := isDBUp(r.cfg.User, r.cfg.Password, r.cfg.Host, r.cfg.Port); err != nil {
		return fmt.Errorf("db not reachable: %s", err.Error())
	}

	r.tx, err = r.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	for _, q := range r.queries {
		if _, err = r.tx.ExecContext(ctx, q.query, q.args...); err != nil {
			return err
		}
	}
	return
}

func (r *retryHandler) retryQuery(ctx context.Context, query string, args []interface{}) (err error) {
	if err = isDBUp(r.cfg.User, r.cfg.Password, r.cfg.Host, r.cfg.Port); err != nil {
		return fmt.Errorf("db not reachable: %s", err.Error())
	}

	if _, err = r.db.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return
}
