package gbinlog

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/Breeze0806/gbinlog/replication"
)

var (
	tesInfo = &mysqlTableInfo{
		name: MysqlTableName{
			DbName:    "vt_test_keyspace",
			TableName: "vt_a",
		},
		columns: []MysqlColumn{
			&mysqlColumnAttribute{
				field: "id",
				typ:   "int(11)",
			},
			&mysqlColumnAttribute{
				field: "message",
				typ:   "varchar(256)",
			},
		},
	}
)

const (
	testDSN      = "test:123456@tcp(192.168.88.128:3306)/mysql"
	testServerID = 1234
)

type mockMapper struct {
}

func newMockMapper() *mockMapper {
	return &mockMapper{}
}

func (m *mockMapper) MysqlTable(name MysqlTableName) (MysqlTable, error) {
	return tesInfo, nil
}

func getInputData() []replication.BinlogEvent {
	// Create a tableMap event on the table.

	f := replication.NewMySQL56BinlogFormat()
	s := replication.NewFakeBinlogStream()
	s.ServerID = 62344

	tableID := uint64(0x102030405060)
	tm := &replication.TableMap{
		Flags:    0x8090,
		Database: "vt_test_keyspace",
		Name:     "vt_a",
		Types: []byte{
			replication.TypeLong,
			replication.TypeVarchar,
		},
		CanBeNull: replication.NewServerBitmap(2),
		Metadata: []uint16{
			0,
			384, // A VARCHAR(128) in utf8 would result in 384.
		},
	}
	tm.CanBeNull.Set(1, true)

	// Do an insert packet with all fields set.
	insertRows := replication.Rows{
		Flags:       0x1234,
		DataColumns: replication.NewServerBitmap(2),
		Rows: []replication.Row{
			{
				NullColumns: replication.NewServerBitmap(2),
				Data: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x04, 0x00, // len('abcd')
					'a', 'b', 'c', 'd', // 'abcd'
				},
			},
		},
	}
	insertRows.DataColumns.Set(0, true)
	insertRows.DataColumns.Set(1, true)

	// Do an update packet with all fields set.
	updateRows := replication.Rows{
		Flags:           0x1234,
		IdentifyColumns: replication.NewServerBitmap(2),
		DataColumns:     replication.NewServerBitmap(2),
		Rows: []replication.Row{
			{
				NullIdentifyColumns: replication.NewServerBitmap(2),
				NullColumns:         replication.NewServerBitmap(2),
				Identify: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x03, 0x00, // len('abc')
					'a', 'b', 'c', // 'abc'
				},
				Data: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x04, 0x00, // len('abcd')
					'a', 'b', 'c', 'd', // 'abcd'
				},
			},
		},
	}
	updateRows.IdentifyColumns.Set(0, true)
	updateRows.IdentifyColumns.Set(1, true)
	updateRows.DataColumns.Set(0, true)
	updateRows.DataColumns.Set(1, true)

	// Do a delete packet with all fields set.
	deleteRows := replication.Rows{
		Flags:           0x1234,
		IdentifyColumns: replication.NewServerBitmap(2),
		Rows: []replication.Row{
			{
				NullIdentifyColumns: replication.NewServerBitmap(2),
				Identify: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x03, 0x00, // len('abc')
					'a', 'b', 'c', // 'abc'
				},
			},
		},
	}
	deleteRows.IdentifyColumns.Set(0, true)
	deleteRows.IdentifyColumns.Set(1, true)

	return []replication.BinlogEvent{
		replication.NewRotateEvent(f, s, uint64(testBinlogPosParseEvents.Offset), testBinlogPosParseEvents.Filename),
		replication.NewFormatDescriptionEvent(f, s),
		replication.NewTableMapEvent(f, s, tableID, tm),
		replication.NewQueryEvent(f, s, replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		replication.NewWriteRowsEvent(f, s, tableID, insertRows),
		replication.NewUpdateRowsEvent(f, s, tableID, updateRows),
		replication.NewDeleteRowsEvent(f, s, tableID, deleteRows),
		replication.NewXIDEvent(f, s),
	}
}

func checkTransactionEqual(t *Transaction, right *Transaction) error {
	if t.NowPosition != right.NowPosition {
		return fmt.Errorf("NowPosition is not equal. left: %v, right: %v", t.NowPosition, right.NowPosition)
	}
	if t.NextPosition != right.NextPosition {
		return fmt.Errorf("NextPosition is not equal. left: %v, right: %v", t.NextPosition, right.NextPosition)
	}

	if len(t.Events) != len(right.Events) {
		return fmt.Errorf("len of Events is not match.left: %v right: %v", len(t.Events), len(right.Events))
	}

	for i := range t.Events {
		if err := checkStreamEventEqual(t.Events[i], right.Events[i]); err != nil {
			return fmt.Errorf("%d RowValues is not match for %v", i, err)
		}
	}
	return nil
}

func checkStreamEventEqual(s *StreamEvent, right *StreamEvent) error {
	if s.Type != right.Type {
		return fmt.Errorf("type is not equal. left: %v, right: %v", s.Type, right.Type)
	}
	if s.Table != right.Table {
		return fmt.Errorf("name is not equal. left: %v, right: %v", s.Table, right.Table)
	}
	if s.Timestamp != right.Timestamp {
		return fmt.Errorf("timestamp is not equal. left: %v, right: %v", s.Timestamp, right.Timestamp)
	}

	if len(s.RowValues) != len(right.RowValues) {
		return fmt.Errorf("len of RowValues is not match.left: %v right: %v",
			len(s.RowValues), len(right.RowValues))
	}

	if len(s.RowIdentifies) != len(right.RowIdentifies) {
		return fmt.Errorf("len of RowIdentifies is not match.left: %v right: %v",
			len(s.RowIdentifies), len(right.RowIdentifies))
	}

	for i := range s.RowValues {
		if err := checkRowDataEqual(s.RowValues[i], right.RowValues[i]); err != nil {
			return fmt.Errorf("%d RowValues is not match for %v", i, err)
		}
	}

	for i := range s.RowIdentifies {
		if err := checkRowDataEqual(s.RowIdentifies[i], right.RowIdentifies[i]); err != nil {
			return fmt.Errorf("%d RowIdentifies is not match for %v", i, err)
		}
	}
	return nil
}

func checkRowDataEqual(r *RowData, right *RowData) error {
	if len(r.Columns) != len(right.Columns) {
		return fmt.Errorf("len of columns is not match.left: %v right: %v", len(r.Columns), len(right.Columns))
	}
	for i := range r.Columns {
		if err := checkColumnDataEqual(r.Columns[i], right.Columns[i]); err != nil {
			return fmt.Errorf("%d Column is not match for %v", i, err)
		}
	}
	return nil
}

func checkColumnDataEqual(c *ColumnData, right *ColumnData) error {
	if c.Filed != right.Filed {
		return fmt.Errorf("filed is not equal. left: %v, right: %v", c.Filed, right.Filed)
	}

	if c.Type != right.Type {
		return fmt.Errorf("type is not equal. left: %v, right: %v", c.Type, right.Type)
	}

	if c.IsEmpty != right.IsEmpty {
		return fmt.Errorf("isEmpty is not equal. left: %v, right: %v", c.IsEmpty, right.IsEmpty)
	}

	if bytes.Compare(c.Data, right.Data) != 0 {
		return fmt.Errorf("data is not equal. left: %v, right: %v", string(c.Data), string(right.Data))
	}
	return nil
}

func TestRowStreamer_parseEvents(t *testing.T) {

	input := getInputData()

	want := &Transaction{
		NowPosition: testBinlogPosParseEvents,
		NextPosition: Position{
			Filename: testBinlogPosParseEvents.Filename,
			Offset:   4,
		},
		Events: []*StreamEvent{
			{
				Type:      StatementInsert,
				Timestamp: 1407805592,
				Table:     tesInfo.name,
				Query:     replication.Query{},
				RowValues: []*RowData{
					{
						Columns: []*ColumnData{
							{
								Filed: "id",
								Data:  []byte("1076895760"),
								Type:  columnTypeLong,
							},
							{
								Filed: "message",
								Data:  []byte("abcd"),
								Type:  columnTypeVarchar,
							},
						},
					},
				},
			},
			{
				Type:      StatementUpdate,
				Table:     tesInfo.name,
				Timestamp: 1407805592,
				RowIdentifies: []*RowData{
					{
						Columns: []*ColumnData{
							{
								Filed: "id",
								Data:  []byte("1076895760"),
								Type:  columnTypeLong,
							},
							{
								Filed: "message",
								Data:  []byte("abc"),
								Type:  columnTypeVarchar,
							},
						},
					},
				},
				RowValues: []*RowData{
					{
						Columns: []*ColumnData{
							{
								Filed: "id",
								Data:  []byte("1076895760"),
								Type:  columnTypeLong,
							},
							{
								Filed: "message",
								Data:  []byte("abcd"),
								Type:  columnTypeVarchar,
							},
						},
					},
				},
			},
			{
				Type:      StatementDelete,
				Timestamp: 1407805592,
				Table:     tesInfo.name,
				RowIdentifies: []*RowData{
					{
						Columns: []*ColumnData{
							{
								Filed: "id",
								Data:  []byte("1076895760"),
								Type:  columnTypeLong,
							},
							{
								Filed: "message",
								Data:  []byte("abc"),
								Type:  columnTypeVarchar,
							},
						},
					},
				},
			},
		},
	}

	m := newMockMapper()

	s, err := NewStreamer(testDSN, testServerID, m)
	if err != nil {
		t.Fatalf("NewStreamer err: %#v", err)
		return
	}
	s.SetBinlogPosition(testBinlogPosParseEvents)

	var out *Transaction
	s.sendTransaction = func(tran *Transaction) error {
		out = tran
		return nil
	}

	events := make(chan replication.BinlogEvent)
	go func() {
		for i := range input {
			events <- input[i]
		}
		close(events)
	}()

	ctx := context.Background()

	_, pErr := s.parseEvents(ctx, events)

	if pErr != nil {
		t.Fatalf("parseEvents err != %v, err: %v", nil, pErr)
	}

	if err := checkTransactionEqual(out, want); err != nil {
		t.Fatalf("NowPosition want != out, err: %v", err)
	}
}

func TestRowStreamer_SetStartBinlogPosition(t *testing.T) {
	m := newMockMapper()
	s, err := NewStreamer(testDSN, testServerID, m)
	if err != nil {
		t.Fatalf("NewStreamer err: %v", err)
	}
	s.SetBinlogPosition(testBinlogPosParseEvents)
	if s.binlogPosition() != testBinlogPosParseEvents {
		t.Fatalf("want != out, input:%+v want:%+v out %+v", testBinlogPosParseEvents, testBinlogPosParseEvents, s.nowPos)
	}
}

func TestStreamer_Error(t *testing.T) {
	m := newMockMapper()
	s, err := NewStreamer(testDSN, testServerID, m)
	if err != nil {
		t.Fatalf("NewStreamer err: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	errors := make(chan *Error, 1)
	s.errChan = errors

	errors <- newError(errStreamEOF)
	if s.Error() != nil {
		t.Fatalf("err != %v err: %v", nil, err)
	}

	errors <- newError(errStreamEOF)
	if s.Error() != nil {
		t.Fatalf("err != %v err: %v", nil, err)
	}

	errors <- newError(context.Canceled)
	if s.Error() != nil {
		t.Fatalf("err != %v err: %v", nil, err)
	}

	errMock := fmt.Errorf("mock error")
	errors <- newError(errMock)
	if s.Error().(*Error).Original() != errMock {
		t.Fatalf("err != %v err: %v", errMock, err)
	}

	cancel()
	errors <- newError(errMock)
	if s.Error() != nil {
		t.Fatalf("err != %v err: %v", nil, err)
	}

	close(errors)
	if s.Error() != nil {
		t.Fatalf("err != %v err: %v", nil, err)
	}
}
