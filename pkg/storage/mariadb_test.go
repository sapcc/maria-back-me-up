/**
 * Copyright 2024 SAP SE
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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/parser"
	"github.com/sapcc/maria-back-me-up/pkg/config"
)

type testEventOptions struct {
	eventType       replication.EventType
	primaryKey      []uint64
	nullBitmap      []byte
	skippedColumns  [][]int
	hasFullMetadata bool
}

func TestWriteQueryEvent(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, true)
	queryEvent := replication.QueryEvent{
		Schema: []byte("service"),
		Query:  []byte("INSERT INTO task (ask_id, title, start_date, due_date, description) VALUES ( '2', 'task1', '2021-05-02', '2022-05-02', 'Test Entry');"),
	}

	mock.ExpectExec("use service;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO task (ask_id, title, start_date, due_date, description) VALUES ( '2', 'task1', '2021-05-02', '2022-05-02', 'Test Entry');").WillReturnResult(sqlmock.NewResult(1, 1))

	execQueryEventTest(t, mock, mariaDBStream, queryEvent, true)
}

func TestWriteQueryEventParseSchema(t *testing.T) {
	mariaDBStream, mock := setup(t, true, true, true)
	queryEvent := replication.QueryEvent{
		Schema: []byte("test"),
		Query:  []byte("INSERT INTO service.task (ask_id, title, start_date, due_date, description) VALUES ( '2', 'task1', '2021-05-02', '2022-05-02', 'Test Entry');"),
	}

	mock.ExpectExec("use service;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO service.task (ask_id, title, start_date, due_date, description) VALUES ( '2', 'task1', '2021-05-02', '2022-05-02', 'Test Entry');").WillReturnResult(sqlmock.NewResult(1, 1))

	execQueryEventTest(t, mock, mariaDBStream, queryEvent, true)
}

func TestWriteQueryEventParseSchemaSkipped(t *testing.T) {
	mariaDBStream, mock := setup(t, true, true, true)
	queryEvent := replication.QueryEvent{
		Schema: []byte("service"),
		Query:  []byte("INSERT INTO test.task (ask_id, title, start_date, due_date, description) VALUES ( '2', 'task1', '2021-05-02', '2022-05-02', 'Test Entry');"),
	}
	// no mock expectations, since this query should be skipped. schema used in query is not in list && parse is enabled
	execQueryEventTest(t, mock, mariaDBStream, queryEvent, true)
}

func TestWriteRowsEventv1FullImage(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, true)
	options := testEventOptions{
		primaryKey:      []uint64{0},
		eventType:       replication.WRITE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: true,
	}
	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", ""},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
		{int32(3), "task1", "2021-05-02", "2022-05-02", nil},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("3", "task1", "2021-05-02", "2022-05-02", nil).WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestWriteRowsEventv1MinimalImage(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, true)

	options := testEventOptions{
		primaryKey:      []uint64{0},
		eventType:       replication.WRITE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", ""},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestWriteRowsEventv1MinimalImageNullCols(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, true)
	options := testEventOptions{
		primaryKey: []uint64{0},
		eventType:  replication.WRITE_ROWS_EVENTv1,
		nullBitmap: []byte{28},
		skippedColumns: [][]int{
			{2, 3},
			{2, 3, 4},
		},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", nil, nil, ""},
		{int32(1), "task1", nil, nil, nil},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,description) VALUES (?,?,?);").WithArgs("1", "task1", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,description) VALUES (?,?,?);").WithArgs("1", "task1", nil).WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestDeleteRowsEventV1FullImageWithPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, true)

	options := testEventOptions{
		primaryKey:      []uint64{0},
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ?;").WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}
func TestDeleteRowsEventV1FullImageNoPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, false)
	// Without Primary Key
	options := testEventOptions{
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ? and title = ? and start_date = ? and due_date = ? and description = ?;").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}

func TestDeleteRowsEventV1MinimalImageWithPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, true)

	options := testEventOptions{
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		skippedColumns:  [][]int{{1, 2, 3, 4}},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ?;").WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}

func TestDeleteRowsEventV1MinimalImageNoPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, false)

	options := testEventOptions{
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		skippedColumns:  [][]int{{1, 2, 3, 4}},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ?;").WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1FullImageWithPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, false)

	options := testEventOptions{
		primaryKey:      []uint64{0},
		eventType:       replication.UPDATE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ?;").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ?;").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}
func TestUpdateRowsEventV1FullImageNoPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, false)

	options := testEventOptions{
		eventType:       replication.UPDATE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ? and title = ? and start_date = ? and due_date = ? and description = ?;").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1", "task1", "2021-05-02", "2022-05-02", "Test Entry Old").WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ? and title = ? and start_date = ? and due_date = ? and description = ?;").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1", "task1", "2021-05-02", "2022-05-02", "Test Entry Old").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1MinimalImageWithPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, true)

	options := testEventOptions{
		primaryKey: []uint64{0},
		eventType:  replication.UPDATE_ROWS_EVENTv1,
		nullBitmap: []byte{28},
		skippedColumns: [][]int{
			{1, 2, 3, 4},
			{0, 1, 2, 3},
		},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
		{nil, nil, nil, nil, "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET description = ? WHERE ask_id = ?;").WithArgs("Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1MinimalImageNoPK(t *testing.T) {
	mariaDBStream, mock := setup(t, false, true, false)

	options := testEventOptions{
		eventType:  replication.UPDATE_ROWS_EVENTv1,
		nullBitmap: []byte{28},
		skippedColumns: [][]int{
			{1, 2, 3, 4},
			{0, 1, 2, 3},
		},
		hasFullMetadata: true,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
		{nil, nil, nil, nil, "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET description = ? WHERE ask_id = ?;").WithArgs("Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func TestWriteRowsEventv1FullImageNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)
	options := testEventOptions{
		eventType:       replication.WRITE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: false,
	}
	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", ""},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
		{int32(3), "task1", "2021-05-02", "2022-05-02", nil},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("3", "task1", "2021-05-02", "2022-05-02", nil).WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestWriteRowsEventv1MinimalImageNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)

	options := testEventOptions{
		eventType:       replication.WRITE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", ""},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestWriteRowsEventv1MinimalImageNullColsNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)
	options := testEventOptions{
		primaryKey: []uint64{0},
		eventType:  replication.WRITE_ROWS_EVENTv1,
		nullBitmap: []byte{28},
		skippedColumns: [][]int{
			{2, 3},
			{2, 3, 4},
		},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", nil, nil, ""},
		{int32(1), "task1", nil, nil, nil},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,description) VALUES (?,?,?);").WithArgs("1", "task1", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,description) VALUES (?,?,?);").WithArgs("1", "task1", nil).WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)
}

func TestDeleteRowsEventV1FullImageWithPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)

	options := testEventOptions{
		primaryKey:      []uint64{0},
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ?;").WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}
func TestDeleteRowsEventV1FullImageNoPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, false)
	// Without Primary Key
	options := testEventOptions{
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ? and title = ? and start_date = ? and due_date = ? and description = ?;").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}

func TestDeleteRowsEventV1MinimalImageWithPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)

	options := testEventOptions{
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		skippedColumns:  [][]int{{1, 2, 3, 4}},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ?;").WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}

func TestDeleteRowsEventV1MinimalImageNoPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, false)

	options := testEventOptions{
		eventType:       replication.DELETE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		skippedColumns:  [][]int{{1, 2, 3, 4}},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
	})

	mock.ExpectExec("DELETE FROM service.task WHERE ask_id = ?;").WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.DELETE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1FullImageWithPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)

	options := testEventOptions{
		primaryKey:      []uint64{0},
		eventType:       replication.UPDATE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ?;").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ?;").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}
func TestUpdateRowsEventV1FullImageNoPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, false)

	options := testEventOptions{
		eventType:       replication.UPDATE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
		{int32(1), "task1", "2021-05-02", "2022-05-02", "Test Entry Old"},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ? and title = ? and start_date = ? and due_date = ? and description = ?;").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1", "task1", "2021-05-02", "2022-05-02", "Test Entry Old").WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec("UPDATE service.task SET ask_id = ?, title = ?, start_date = ?, due_date = ?, description = ? WHERE ask_id = ? and title = ? and start_date = ? and due_date = ? and description = ?;").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry New", "1", "task1", "2021-05-02", "2022-05-02", "Test Entry Old").WillReturnResult(sqlmock.NewResult(0, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1MinimalImageWithPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)

	options := testEventOptions{
		primaryKey: []uint64{0},
		eventType:  replication.UPDATE_ROWS_EVENTv1,
		nullBitmap: []byte{28},
		skippedColumns: [][]int{
			{1, 2, 3, 4},
			{0, 1, 2, 3},
		},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
		{nil, nil, nil, nil, "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET description = ? WHERE ask_id = ?;").WithArgs("Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func TestUpdateRowsEventV1MinimalImageNoPKNoRowMetadata(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, false)

	options := testEventOptions{
		eventType:  replication.UPDATE_ROWS_EVENTv1,
		nullBitmap: []byte{28},
		skippedColumns: [][]int{
			{1, 2, 3, 4},
			{0, 1, 2, 3},
		},
		hasFullMetadata: false,
	}

	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), nil, nil, nil, nil},
		{nil, nil, nil, nil, "Test Entry New"},
	})

	mock.ExpectExec("UPDATE service.task SET description = ? WHERE ask_id = ?;").WithArgs("Test Entry New", "1").WillReturnResult(sqlmock.NewResult(0, 1))
	execRowsEventTest(t, mock, mariaDBStream, replication.UPDATE_ROWS_EVENTv1, rowsEvent)
}

func TestWriteTxThenQuery(t *testing.T) {
	mariaDBStream, mock := setup(t, false, false, true)
	options := testEventOptions{
		eventType:       replication.WRITE_ROWS_EVENTv1,
		nullBitmap:      []byte{28},
		hasFullMetadata: false,
	}
	rowsEvent := createRowsTestEvent(1, options, [][]interface{}{
		{int32(1), "task1", "2021-05-02", "2022-05-02", ""},
		{int32(2), "task1", "2021-05-02", "2022-05-02", "Test Entry"},
		{int32(3), "task1", "2021-05-02", "2022-05-02", nil},
	})

	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("1", "task1", "2021-05-02", "2022-05-02", "").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("2", "task1", "2021-05-02", "2022-05-02", "Test Entry").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO service.task (ask_id,title,start_date,due_date,description) VALUES (?,?,?,?,?);").WithArgs("3", "task1", "2021-05-02", "2022-05-02", nil).WillReturnResult(sqlmock.NewResult(1, 1))

	execRowsEventTest(t, mock, mariaDBStream, replication.WRITE_ROWS_EVENTv1, rowsEvent)

	queryEvent := replication.QueryEvent{
		Schema: []byte("service"),
		Query:  []byte("INSERT INTO task (ask_id, title, start_date, due_date, description) VALUES ( '2', 'task1', '2021-05-02', '2022-05-02', 'Test Entry');"),
	}

	mock.ExpectExec("use service;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO task (ask_id, title, start_date, due_date, description) VALUES ( '2', 'task1', '2021-05-02', '2022-05-02', 'Test Entry');").WillReturnResult(sqlmock.NewResult(1, 1))

	execQueryEventTest(t, mock, mariaDBStream, queryEvent, false)

}

func TestCleanupLocksPluginNoLocks(t *testing.T) {

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("test setup failed: %v", err.Error())
		t.FailNow()
	}

	cfg := &config.MariaDBStream{
		Name:      "test",
		Databases: []string{"test"},
	}

	mock.ExpectQuery("select PLUGIN_STATUS from INFORMATION_SCHEMA.PLUGINS where PLUGIN_NAME = 'METADATA_LOCK_INFO';").WillReturnRows(sqlmock.NewRows([]string{"PLUGIN_STATUS"}).AddRow("ACTIVE"))

	mock.ExpectQuery("select connection_id();").WillReturnRows(sqlmock.NewRows([]string{"connection_id()"}).AddRow(123))

	lockInfoResult := sqlmock.NewRows([]string{"THREAD_ID", "LOCK_MODE", "TABLE_SCHEMA"})
	// same connection
	lockInfoResult.AddRow(123, "MDL_EXCLUSIVE", "test")
	// different connection, unrelevant schema
	lockInfoResult.AddRow(100, "MDL_EXCLUSIVE", "something")

	mock.ExpectQuery("select THREAD_ID, LOCK_MODE, TABLE_SCHEMA from INFORMATION_SCHEMA.METADATA_LOCK_INFO;").WillReturnRows(lockInfoResult)

	actRemovedLocks, err := cleanupLocks(db, cfg)
	if err != nil {
		t.Errorf("test setup failed: %v", err.Error())
		t.FailNow()
	}

	if actRemovedLocks != 0 {
		t.Errorf("Expected 1 lock to be removed, actual : %v", actRemovedLocks)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Failed expectations not met: %s", err.Error())
	}
}

func TestCleanupLocksPluginRemoveLock(t *testing.T) {

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("test setup failed: %v", err.Error())
		t.FailNow()
	}

	cfg := &config.MariaDBStream{
		Name:      "test",
		Databases: []string{"test"},
	}

	mock.ExpectQuery("select PLUGIN_STATUS from INFORMATION_SCHEMA.PLUGINS where PLUGIN_NAME = 'METADATA_LOCK_INFO';").WillReturnRows(sqlmock.NewRows([]string{"PLUGIN_STATUS"}).AddRow("ACTIVE"))

	mock.ExpectQuery("select connection_id();").WillReturnRows(sqlmock.NewRows([]string{"connection_id()"}).AddRow(123))

	lockInfoResult := sqlmock.NewRows([]string{"THREAD_ID", "LOCK_MODE", "TABLE_SCHEMA"})
	// same connection
	lockInfoResult.AddRow(123, "MDL_EXCLUSIVE", "test")
	// different connection, unrelevant schema
	lockInfoResult.AddRow(100, "MDL_EXCLUSIVE", "something")
	// different connection, relevant schema
	lockInfoResult.AddRow(101, "MDL_EXCLUSIVE", "test")

	mock.ExpectQuery("select THREAD_ID, LOCK_MODE, TABLE_SCHEMA from INFORMATION_SCHEMA.METADATA_LOCK_INFO;").WillReturnRows(lockInfoResult)

	mock.ExpectExec("kill connection 101;").WillReturnResult(sqlmock.NewResult(0, 0))

	actRemovedLocks, err := cleanupLocks(db, cfg)
	if err != nil {
		t.Errorf("test setup failed: %v", err.Error())
		t.FailNow()
	}

	if actRemovedLocks != 1 {
		t.Errorf("Expected 1 lock to be removed, actual : %v", actRemovedLocks)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Failed expectations not met: %s", err.Error())
	}
}

func setup(t *testing.T, parseSQL, hasRowMetadata bool, hasPrimaryKey bool) (mariaDBStream *MariaDBStream, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("test setup failed: %s", err.Error())
		t.FailNow()
	}
	databases := map[string]struct{}{
		"service": {},
	}
	config := config.MariaDBStream{
		ParseSchema: parseSQL,
	}
	mock.ExpectBegin()
	retryHandler := newRetryHandler(db, &config)
	if err := retryHandler.beginTx(context.TODO()); err != nil {
		t.Error("test setup failed to create transaction")
	}

	tableMetadata := map[string]map[string]map[int]columnDefinition{}
	if !hasRowMetadata {
		tableMetadata = map[string]map[string]map[int]columnDefinition{
			"service": {
				"task": {
					1: {
						name:  "ask_id",
						isKey: hasPrimaryKey,
					},
					2: {
						name: "title",
					},
					3: {
						name:       "start_date",
						isNullable: true,
					},
					4: {
						name:       "due_date",
						isNullable: true,
					},
					5: {
						name:       "description",
						isNullable: true,
					},
				},
			},
		}
	}

	mariaDBStream = &MariaDBStream{sqlParser: parser.New(), db: db, retry: retryHandler, databases: databases, cfg: config, tableMetadata: tableMetadata}
	return
}

func execRowsEventTest(t *testing.T, mock sqlmock.Sqlmock, mariaDBStream *MariaDBStream, eventType replication.EventType, rowsEvent replication.RowsEvent) {

	event := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: eventType,
		},
		Event: &rowsEvent,
	}
	err := mariaDBStream.ProcessBinlogEvent(context.TODO(), event)
	if err != nil {
		t.Errorf("Failed to replicate %s: %s", eventType.String(), err.Error())
		t.FailNow()
	}

	mock.ExpectCommit()
	err = mariaDBStream.retry.commit(context.TODO())
	if err != nil {
		t.Errorf("Failed to commit %s: %s", eventType.String(), err.Error())
		t.FailNow()
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Failed expectations not met: %s", err.Error())
	}
}

func execQueryEventTest(t *testing.T, mock sqlmock.Sqlmock, mariaDBStream *MariaDBStream, queryEvent replication.QueryEvent, expectCommit bool) {
	event := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.QUERY_EVENT,
		},
		Event: &queryEvent,
	}
	err := mariaDBStream.ProcessBinlogEvent(context.TODO(), event)

	if err != nil {
		t.Errorf("Failed to replicate %s: %s", replication.QUERY_EVENT.String(), err.Error())
		t.FailNow()
	}

	if expectCommit {
		mock.ExpectCommit()
		err = mariaDBStream.retry.commit(context.TODO())
		if err != nil {
			t.Errorf("Failed to commit: %s", err.Error())
			t.FailNow()
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Failed expectations not met: %s", err.Error())
	}
}

func createRowsTestEvent(version int, options testEventOptions, rows [][]interface{}) replication.RowsEvent {
	rowsEvent := replication.RowsEvent{
		Version: version,
		TableID: 1337,
		Table: &replication.TableMapEvent{
			Schema: []byte("service"),
			Table:  []byte("task"),
		},
		ColumnCount:    5,
		Rows:           rows,
		SkippedColumns: options.skippedColumns,
	}
	if options.hasFullMetadata {
		rowsEvent.Table.PrimaryKey = options.primaryKey
		rowsEvent.Table.NullBitmap = options.nullBitmap
		rowsEvent.Table.ColumnName = [][]byte{
			[]byte("ask_id"),
			[]byte("title"),
			[]byte("start_date"),
			[]byte("due_date"),
			[]byte("description"),
		}
	}
	return rowsEvent
}
