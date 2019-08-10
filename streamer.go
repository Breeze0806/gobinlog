package gbinlog

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Breeze0806/gbinlog/replication"
	"github.com/Breeze0806/mysql"
)

//MysqlTableMapper 用于获取表信息的接口
type MysqlTableMapper interface {
	MysqlTable(name MysqlTableName) (MysqlTable, error)
}

//Streamer 从github.com/youtube/vitess/go/vt/binlog/binlog_streamer.go的基础上移植过来
//专门用来RowStreamer解析row模式的binlog event，将其变为对应的事务
type Streamer struct {
	dsn             string
	serverID        uint32
	nowPos          atomic.Value
	tableMapper     MysqlTableMapper
	sendTransaction SendTransactionFunc
	errChan         <-chan *Error
	ctx             context.Context
}

//SendTransactionFunc 处理事务信息函数，你可以将一个chan注册到这个函数中如
//   func getTransaction(tran *Transaction) error{
//	     Transactions <- tran
//	     return nil
//   }
//如果这个函数返回错误，那么RowStreamer.Stream会停止dump以及解析binlog且返回错误
type SendTransactionFunc func(*Transaction) error

type tableCache struct {
	tableMap *replication.TableMap
	table    MysqlTable
}

//NewStreamer dsn是mysql数据库的信息，serverID是标识该数据库的信息
func NewStreamer(dsn string, serverID uint32,
	tableMapper MysqlTableMapper) (*Streamer, error) {
	return &Streamer{
		dsn:         dsn,
		serverID:    serverID,
		tableMapper: tableMapper,
	}, nil
}

//SetBinlogPosition 设置开始的binlog位置
func (s *Streamer) SetBinlogPosition(startPos Position) {
	s.nowPos.Store(startPos)
}

func (s *Streamer) binlogPosition() Position {
	return s.nowPos.Load().(Position)
}

//Stream 注册一个处理事务信息函数到Stream中
func (s *Streamer) Stream(ctx context.Context, sendTransaction SendTransactionFunc) error {
	s.ctx = ctx
	conn, err := newSlaveConnection(func() (conn dumpConn, e error) {
		return mysql.NewDumpConn(s.dsn, ctx)
	})
	if err != nil {
		return err.msgf("newMysqlConn fail.")
	}
	defer conn.close()
	s.sendTransaction = sendTransaction
	var events <-chan replication.BinlogEvent
	var pos Position
	events, err = conn.startDumpFromBinlogPosition(ctx, s.serverID, s.binlogPosition())
	if err != nil {
		return err.msgf("startDumpFromBinlogPosition fail in pos: %+v", s.nowPos)
	}
	s.errChan = conn.errChan
	pos, err = s.parseEvents(ctx, events)
	s.SetBinlogPosition(pos)
	if err != nil {
		return err.msgf("parseEvents fail in pos: %+v", err)
	}
	return nil
}

//Error 每次使用Stream后需要检测Error
func (s *Streamer) Error() error {
	select {
	case err, ok := <-s.errChan:
		if ok {
			switch {
			case s.ctx.Err() == context.Canceled:
				return nil
			case err.Original() == context.Canceled,
				err.Original() == errStreamEOF:
				return nil
			default:
				return err
			}
		}
		return nil
	}
}

func (s *Streamer) parseEvents(ctx context.Context, events <-chan replication.BinlogEvent) (Position, *Error) {
	var tranEvents []*StreamEvent
	var format replication.BinlogFormat
	var err error
	pos := s.binlogPosition()
	tablesMaps := make(map[uint64]*tableCache)
	autocommit := true

	begin := func() {
		if tranEvents != nil {
			// If this happened, it would be a legitimate error.
			lw.logger().Errorf("parseEvents BEGIN in binlog stream while still in another transaction; dropping %d transactionEvents: %+v", len(tranEvents), tranEvents)
		}
		tranEvents = make([]*StreamEvent, 0, 10)
		autocommit = false
	}

	commit := func(ev replication.BinlogEvent) error {
		now := pos
		pos.Offset = ev.NextPosition()
		next := pos
		tran := newTransaction(now, next, int64(ev.Timestamp()), tranEvents)
		if err = s.sendTransaction(tran); err != nil {
			return fmt.Errorf("sendTransaction error: %v", err)
		}
		tranEvents = nil
		autocommit = true
		return nil
	}

	for {
		var ev replication.BinlogEvent
		var ok bool
		select {
		case ev, ok = <-events:
			if !ok {
				lw.logger().Infof("parseEvents reached end of binlog event stream")
				return pos, nil
			}
		case <-ctx.Done():
			lw.logger().Infof("parseEvents stopping early due to binlog Streamer service shutdown or client disconnect")
			return pos, nil
		}

		// Validate the buffer before reading fields from it.
		if !ev.IsValid() {
			return pos, newError(fmt.Errorf("invalid data: %+v", ev)).
				msgf("parseEvents can't parse binlog event.")
		}

		// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
		// seen one, because another one might come along (e.g. on lw.logger() rotate due to
		// binlog settings change) that changes the format.
		if ev.IsFormatDescription() {
			format, err = ev.Format()
			if err != nil {
				return pos, newError(err).
					msgf("parseEvents can't parse FORMAT_DESCRIPTION_EVENT event data: %+v", ev)
			}
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a format description event:%+v",
				ev.NextPosition(), format)
			continue
		}

		// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
		// tells us the size of the event header.
		if format.IsZero() {
			// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
			// is a fake ROTATE_EVENT, which the master sends to tell us the name
			// of the current binlog file.
			if ev.IsRotate() {
				continue
			}
			return pos, newError(fmt.
				Errorf("parseEvents got a real event before FORMAT_DESCRIPTION_EVENT: %+v", ev))
		}

		// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
		ev, _, err = ev.StripChecksum(format)
		if err != nil {
			return pos, newError(err).msgf(
				"parseEvents can't strip checksum from binlog event, event data: %+v", ev)
		}

		switch {
		case ev.IsXID(): // XID_EVENT (equivalent to COMMIT)
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a xid event: %v:", pos, ev)
			if err = commit(ev); err != nil {
				return pos, newError(err).msgf("parseEvents commit fail in XID event")
			}

		case ev.IsRotate():
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a xid event %v:", pos, ev)
			var filename string
			var offset int64
			if filename, offset, err = ev.Rotate(format); err != nil {
				return pos, newError(err).msgf("parseEvents Rotate fail.")
			}
			pos.Filename = filename
			pos.Offset = offset
		case ev.IsQuery():
			q, err := ev.Query(format)
			if err != nil {
				return pos, newError(err).msgf(
					"parseEvents can't get query from binlog event. event data: %+v", ev)
			}
			typ := GetStatementCategory(q.SQL)

			lw.logger().Debugf("parseEvents pos: %+v binlog event is a query event: %+v query: %v", pos, ev, q.SQL)

			switch typ {
			case StatementBegin:
				begin()
			case StatementCreate, StatementAlter, StatementDrop, StatementRename, StatementTruncate, StatementSet:
				tranEvents = append(tranEvents, &StreamEvent{
					Type:      typ,
					Query:     q,
					Timestamp: int64(ev.Timestamp()),
				})
				if autocommit {
					if err = commit(ev); err != nil {
						return pos, newError(err).msgf("parseEvents commit fail in Query event")
					}
				}
			case StatementDelete, StatementInsert, StatementUpdate:
				tranEvents = append(tranEvents, &StreamEvent{
					Type:      typ,
					Query:     q,
					Timestamp: int64(ev.Timestamp()),
				})
				if autocommit {
					if err = commit(ev); err != nil {
						return pos, newError(err).msgf("parseEvents commit fail in Query event")
					}
				}
			case StatementRollback:
				tranEvents = nil
				fallthrough
			case StatementCommit:
				if err = commit(ev); err != nil {
					return pos, newError(err).msgf("parseEvents commit fail in Query event")
				}
			default:
				lw.logger().Errorf("parseEvents we have a sql in binlog position: %+v error: %v", pos,
					fmt.Errorf("parseEvents SQL query %s  statement in row binlog SQL: %s", typ.String(), q.SQL))
				//return pos, fmt.Errorf("parseEvents SQL query %s  statement in row binlog SQL: %s", typ.String(), q.SQL)
			}

		case ev.IsTableMap():
			tableID := ev.TableID(format)
			tm, err := ev.TableMap(format)

			if err != nil {
				return pos, newError(err).msgf("parseEvents TableMap fail. event data: %v", ev)
			}
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a table map event, tableID: %v table map: %+v",
				pos, tableID, *tm)

			if _, ok = tablesMaps[tableID]; ok {
				tablesMaps[tableID].tableMap = tm
				continue
			}

			tc := &tableCache{
				tableMap: tm,
			}

			name := NewMysqlTableName(tm.Database, tm.Name)

			var info MysqlTable
			if info, err = s.tableMapper.MysqlTable(name); err != nil {
				return pos, newError(err).msgf("parseEvents MysqlTable fail. table: %v", err)
			}

			if len(info.Columns()) != tm.CanBeNull.Count() {
				return Position{},
					newError(fmt.Errorf("parseEvents the length of column in tableMap(%d) "+
						"did not equal to the length of column in table info(%d)", tm.CanBeNull.Count(),
						len(info.Columns())))
			}
			tc.table = info
			tablesMaps[tableID] = tc

		case ev.IsWriteRows():
			tableID := ev.TableID(format)
			tc, ok := tablesMaps[tableID]
			if !ok {
				return pos, newError(fmt.Errorf("parseEvents unknown tableID %v in WriteRows event", tableID))
			}
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a write rows event, tableID: %v tc.tableMap: %+v",
				pos, tableID, tc.tableMap)
			rows, err := ev.Rows(format, tc.tableMap)
			if err != nil {
				return pos, newError(err).msgf("Rows fail in WriteRows event. event data: %v", ev)
			}
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a write rows event, tableID: %v rows: %+v",
				pos, tableID, rows)

			tranEvent, err := appendInsertEventFromRows(tc, &rows, int64(ev.Timestamp()))
			if err != nil {
				return pos, newError(err)
			}

			tranEvents = append(tranEvents, tranEvent)
			if autocommit {
				if err = commit(ev); err != nil {
					return pos, newError(err).msgf("parseEvents commit fail in WriteRows event")
				}
			}

		case ev.IsUpdateRows():
			tableID := ev.TableID(format)
			tc, ok := tablesMaps[tableID]
			if !ok {
				return pos, newError(fmt.Errorf("parseEvents unknown tableID %v in UpdateRows event", tableID))
			}
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a update rows event, tableID: %v tc.tableMap: %+v",
				pos, tableID, tc.tableMap)
			rows, err := ev.Rows(format, tc.tableMap)
			if err != nil {
				return pos, newError(err).msgf("Rows fail in UpdateRows event. event data: %v", ev)
			}

			lw.logger().Debugf("parseEvents pos: %+v binlog event is a update rows event, tableID: %v rows: %+v",
				pos, tableID, rows)

			tranEvent, err := appendUpdateEventFromRows(tc, &rows, int64(ev.Timestamp()))
			if err != nil {
				return pos, newError(err)
			}
			tranEvents = append(tranEvents, tranEvent)
			if autocommit {
				if err = commit(ev); err != nil {
					return pos, newError(err).msgf("parseEvents commit fail in UpdateRows event")
				}
			}
		case ev.IsDeleteRows():
			tableID := ev.TableID(format)
			tc, ok := tablesMaps[tableID]
			if !ok {
				return pos, newError(fmt.Errorf("parseEvents unknown tableID %v in DeleteRows event", tableID))
			}

			lw.logger().Debugf("parseEvents pos: %+v binlog event is a delete rows event, tableID: %v tc.tableMap: %+v",
				pos, tableID, tc.tableMap)

			rows, err := ev.Rows(format, tc.tableMap)
			if err != nil {
				return pos, newError(err).msgf("Rows fail in DeleteRows event. event data: %v", ev)
			}

			lw.logger().Debugf("parseEvents pos: %+v", "binlog event is a delete rows event, tableID: %v rows: %+v",
				pos, tableID, rows)
			tranEvent, err := appendDeleteEventFromRows(tc, &rows, int64(ev.Timestamp()))
			if err != nil {
				return pos, newError(err)
			}

			tranEvents = append(tranEvents, tranEvent)
			if autocommit {
				if err = commit(ev); err != nil {
					return pos, newError(err).msgf("parseEvents commit fail in DeleteRows event")
				}
			}
		case ev.IsPreviousGTIDs():
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a PreviousGTIDs event: %+v", pos, ev)
		case ev.IsGTID():
			lw.logger().Debugf("parseEvents pos: %+v binlog event is a GTID event: %+v", pos, ev)

		case ev.IsRand():
			//todo deal with the Rand error
			return pos, newError(fmt.Errorf("binlog event is a Rand event: %+v", ev))
		case ev.IsIntVar():
			//todo deal with the IntVar error
			return pos, newError(fmt.Errorf("binlog event is a IntVar event: %+v", ev))
		case ev.IsRowsQuery():
			//todo deal with the RowsQuery error
			return pos, newError(fmt.Errorf("binlog event is a RowsQuery event: %+v", ev))
		}
	}
}

func appendUpdateEventFromRows(tc *tableCache, rows *replication.Rows, timestamp int64) (*StreamEvent, error) {
	ev := newStreamEvent(StatementUpdate, timestamp, tc.table.Name())
	for i := range rows.Rows {
		identifies, err := getIdentifiesFromRow(tc, rows, i)
		if err != nil {
			return ev, err
		}
		ev.RowIdentifies = append(ev.RowIdentifies, identifies)

		values, err := getValuesFromRow(tc, rows, i)
		if err != nil {
			return ev, err
		}
		ev.RowValues = append(ev.RowValues, values)
	}

	return ev, nil
}

func appendInsertEventFromRows(tc *tableCache, rows *replication.Rows, timestamp int64) (*StreamEvent, error) {
	ev := newStreamEvent(StatementInsert, timestamp, tc.table.Name())
	for i := range rows.Rows {
		values, err := getValuesFromRow(tc, rows, i)
		if err != nil {
			return ev, err
		}
		ev.RowValues = append(ev.RowValues, values)
	}
	return ev, nil
}

func appendDeleteEventFromRows(tc *tableCache, rows *replication.Rows, timestamp int64) (*StreamEvent, error) {
	ev := newStreamEvent(StatementDelete, timestamp, tc.table.Name())
	for i := range rows.Rows {
		identifies, err := getIdentifiesFromRow(tc, rows, i)
		if err != nil {
			return ev, err
		}
		ev.RowIdentifies = append(ev.RowIdentifies, identifies)
	}
	return ev, nil
}

func getValuesFromRow(tc *tableCache, rs *replication.Rows, rowIndex int) (*RowData, error) {
	data := rs.Rows[rowIndex].Data
	valueIndex := 0
	pos := 0

	if rs.DataColumns.Count() != len(tc.table.Columns()) {
		return nil, fmt.Errorf("getValuesFromRow the length of column(%d) in rows did not equal to "+
			"the length of column in table metadata(%d)", rs.DataColumns.Count(), len(tc.table.Columns()))
	}
	values := newRowData(rs.IdentifyColumns.Count())

	for c := 0; c < rs.DataColumns.Count(); c++ {
		column := newColumnData(tc.table.Columns()[c].Field(), ColumnType(tc.tableMap.Types[c]),
			false)

		if !rs.DataColumns.Bit(c) {
			column.IsEmpty = true
			values.Columns = append(values.Columns, column)
			continue
		}

		if rs.Rows[rowIndex].NullColumns.Bit(valueIndex) {
			column.Data = nil
			values.Columns = append(values.Columns, column)
			valueIndex++
			continue
		}

		var l int
		var err error

		column.Data, l, err = replication.CellBytes(data, pos, tc.tableMap.Types[c], tc.tableMap.Metadata[c],
			tc.table.Columns()[c].IsUnSignedInt())

		if err != nil {
			return nil, err
		}

		values.Columns = append(values.Columns, column)

		pos += l
		valueIndex++
	}

	return values, nil
}

func getIdentifiesFromRow(tc *tableCache, rs *replication.Rows, rowIndex int) (*RowData, error) {
	data := rs.Rows[rowIndex].Identify
	identifyIndex := 0
	pos := 0
	if rs.IdentifyColumns.Count() != len(tc.table.Columns()) {
		return nil, fmt.Errorf("getIdentifiesFromRow the length of IdentifyColumns(%d) in rows did not equal to "+
			"the length of column in table metadata(%d)", rs.IdentifyColumns.Count(), len(tc.table.Columns()))
	}
	identifies := newRowData(rs.IdentifyColumns.Count())
	for c := 0; c < rs.IdentifyColumns.Count(); c++ {

		column := newColumnData(tc.table.Columns()[c].Field(), ColumnType(tc.tableMap.Types[c]),
			false)
		if !rs.IdentifyColumns.Bit(c) {
			column.IsEmpty = true
			identifies.Columns = append(identifies.Columns, column)
			continue
		}

		if rs.Rows[rowIndex].NullIdentifyColumns.Bit(identifyIndex) {
			column.Data = nil
			identifies.Columns = append(identifies.Columns, column)
			identifyIndex++
			continue
		}

		var l int
		var err error

		column.Data, l, err = replication.CellBytes(data, pos, tc.tableMap.Types[c], tc.tableMap.Metadata[c],
			tc.table.Columns()[c].IsUnSignedInt())
		if err != nil {
			return nil, err
		}

		identifies.Columns = append(identifies.Columns, column)

		pos += l
		identifyIndex++
	}

	return identifies, nil
}
