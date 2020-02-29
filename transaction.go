package gobinlog

import (
	"encoding/json"
	"time"

	"github.com/Breeze0806/gobinlog/replication"
)

//Transaction 代表一组有事务的binlog evnet
type Transaction struct {
	NowPosition  Position       //在binlog中的当前位置
	NextPosition Position       //在binlog中的下一个位置
	Timestamp    int64          //执行时间
	Events       []*StreamEvent //一组有事务的binlog evnet
}

//newTransaction 创建Transaction
func newTransaction(now, next Position, timestamp int64,
	events []*StreamEvent) *Transaction {
	return &Transaction{
		NowPosition:  now,
		NextPosition: next,
		Timestamp:    timestamp,
		Events:       events,
	}
}

//MarshalJSON 实现Transaction的json序列化
func (t *Transaction) MarshalJSON() ([]byte, error) {
	tJSON := struct {
		NowPosition  Position       `json:"nowPosition"`
		NextPosition Position       `json:"nextPosition"`
		Timestamp    string         `json:"timestamp"`
		Events       []*StreamEvent `json:"events"`
	}{
		NowPosition:  t.NowPosition,
		NextPosition: t.NextPosition,
		Timestamp:    time.Unix(t.Timestamp, 0).Local().String(),
		Events:       t.Events,
	}
	return json.Marshal(tJSON)
}

//StreamEvent means a SQL or a rows in binlog
type StreamEvent struct {
	Type          StatementType     //语句类型
	Table         MysqlTableName    //表名
	Query         replication.Query //sql
	Timestamp     int64             //执行时间
	RowValues     []*RowData        //which data come to used for StatementInsert and  StatementUpdate
	RowIdentifies []*RowData        //which data come from used for  StatementUpdate and StatementDelete
}

//newStreamEvent 创建StreamEvent
func newStreamEvent(tranType StatementType,
	timestamp int64, table MysqlTableName) *StreamEvent {
	return &StreamEvent{
		Type:          tranType,
		Table:         table,
		Timestamp:     timestamp,
		Query:         replication.Query{},
		RowValues:     make([]*RowData, 0, 10),
		RowIdentifies: make([]*RowData, 0, 10),
	}
}

type baseStreamEventJSON struct {
	Table     MysqlTableName `json:"name"`
	Type      string         `json:"type"`
	Timestamp string         `json:"timestamp"`
}

//MarshalJSON 实现StreamEvent的json序列化
func (s *StreamEvent) MarshalJSON() ([]byte, error) {
	b := baseStreamEventJSON{
		Table:     s.Table,
		Type:      s.Type.String(),
		Timestamp: time.Unix(s.Timestamp, 0).Local().String(),
	}
	if s.Query.SQL != "" {
		sqlJSON := struct {
			baseStreamEventJSON
			SQL string `json:"sql"`
		}{
			baseStreamEventJSON: b,
			SQL:                 s.Query.SQL,
		}
		return json.Marshal(sqlJSON)
	}
	RowJSON := struct {
		baseStreamEventJSON
		RowValues     []*RowData `json:"rowValues"`
		RowIdentifies []*RowData `json:"rowIdentifies"`
	}{
		baseStreamEventJSON: b,
		RowValues:           s.RowValues,
		RowIdentifies:       s.RowIdentifies,
	}
	return json.Marshal(RowJSON)
}

//RowData 行数据
type RowData struct {
	Columns []*ColumnData
}

//newRowData 创建RowData
func newRowData(cnt int) *RowData {
	return &RowData{
		Columns: make([]*ColumnData, 0, cnt),
	}
}

//ColumnData 单个列的信息
type ColumnData struct {
	Filed   string     // 字段信息
	Type    ColumnType // binlog中的列类型
	IsEmpty bool       // data is empty,即该列没有变化
	Data    []byte     // the data
}

//newColumnData 创建ColumnData
func newColumnData(filed string, typ ColumnType, isEmpty bool) *ColumnData {
	return &ColumnData{
		Filed:   filed,
		Type:    typ,
		IsEmpty: isEmpty,
	}
}

type baseColumnJSON struct {
	Filed   string `json:"filed"`
	Type    string `json:"type"`
	IsEmpty bool   `json:"isEmpty"`
}

//MarshalJSON 实现ColumnData的json序列化
func (c *ColumnData) MarshalJSON() ([]byte, error) {
	b := baseColumnJSON{
		Filed:   c.Filed,
		Type:    c.Type.String(),
		IsEmpty: c.IsEmpty,
	}

	var i interface{} = string(c.Data)
	if c.Data == nil {
		i = nil
	}
	notNullJSON := struct {
		baseColumnJSON
		Data interface{} `json:"data"`
	}{
		baseColumnJSON: b,
		Data:           i,
	}
	return json.Marshal(notNullJSON)
}
