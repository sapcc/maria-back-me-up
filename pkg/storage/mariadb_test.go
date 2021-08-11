package storage

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/parser"
)

func TestWriteRowsEventv1FullMetadataFullImage(t *testing.T) {
	mariaDBStream, mock := setup(t)
	rowsEvent := createRowsTestEvent(1, replication.WRITE_ROWS_EVENTv1, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", ""},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
		{int32(3), "task1", "2021-05-02", "2022-05-02", nil},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("3", "task1", "2021-05-02", "2022-05-02", nil).WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestWriteRowsEventv1FullMetadataMinimalImage(t *testing.T) {
	mariaDBStream, mock := setup(t)
	rowsEvent := createRowsTestEvent(1, replication.WRITE_ROWS_EVENTv1, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", ""},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})
	rowsEvent.Table.NullBitmap = []byte{28}
	rowsEvent.SkippedColumns = [][]int{{}, {}}

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)

	rowsEvent = createRowsTestEvent(1, replication.WRITE_ROWS_EVENTv1, [][]interface{}{
		{int32(1), "task1", nil, nil, ""},
		{int32(1), "task1", nil, nil, nil},
	})
	rowsEvent.Table.NullBitmap = []byte{28}
	rowsEvent.SkippedColumns = [][]int{
		{2, 3},
		{2, 3, 4},
	}
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,description) VALUES (?,?,?);").WithArgs("1", "task1", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,description) VALUES (?,?,?);").WithArgs("1", "task1", nil).WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestDeleteRowsEventV1(t *testing.T) {
	mariaDBStream, mock := setup(t)

	rowsEvent := createRowsTestEvent(1, replication.DELETE_ROWS_EVENTv1, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ?;").WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1FullMetaFullImage(t *testing.T) {
	mariaDBStream, mock := setup(t)

	rowsEvent := createRowsTestEvent(1, replication.UPDATE_ROWS_EVENTv1, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ?;").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ?;").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1FullMetaMinimalImage(t *testing.T) {
	mariaDBStream, mock := setup(t)

	rowsEvent := createRowsTestEvent(1, replication.UPDATE_ROWS_EVENTv1, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
		{nil, nil, nil, nil, "Test Entry New"},
	})

	rowsEvent.SkippedColumns = [][]int{
		{1, 2, 3, 4},
		{0, 1, 2, 3},
	}

	mock.ExpectExec("UPDATE service.task SET description = ? WHERE ask_id = ?;").WithArgs("Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func setup(t *testing.T) (mariaDBStream MariaDBStream, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("test setup failed: %s", err.Error())
		t.FailNow()
	}
	mariaDBStream = MariaDBStream{sqlParser: parser.New(), db: db}
	return
}

func execRowsEventTest(t *testing.T, mock sqlmock.Sqlmock, mariaDBStream MariaDBStream, eventType replication.EventType, rowsEvent replication.RowsEvent) {
	err := mariaDBStream.handleRowsEvent(context.TODO(), &rowsEvent, eventType)
	if err != nil {
		t.Errorf("Failed to replicate %s: %s", eventType.String(), err.Error())
		t.FailNow()
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Failed expectations not met: %s", err.Error())
	}
}

func createRowsTestEvent(version int, eventType replication.EventType, rows [][]interface{}) replication.RowsEvent {
	rowsEvent := replication.RowsEvent{
		Version: version,
		TableID: 1337,
		Table: &replication.TableMapEvent{
			Schema: []byte("service"),
			Table:  []byte("task"),
			ColumnName: [][]byte{
				[]byte("ask_id"),
				[]byte("title"),
				[]byte("start_date"),
				[]byte("due_date"),
				[]byte("description"),
			},
			PrimaryKey: []uint64{0},
			NullBitmap: []byte{28},
		},
		ColumnCount: 5,
		Rows:        rows,
	}

	return rowsEvent
}
