package gbinlog

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	//_ "github.com/go-sql-driver/mysql" you need it in you own project
)

const (
	mysqlUnsigned = "unsigned" //无符号
)

//列属性
type mysqlColumnAttribute struct {
	field         string //列名
	typ           string //列类型
	null          string //是否为空
	key           string //PRI代表主键，UNI代表唯一索引
	columnDefault []byte //默认值
	extra         string //其他备注信息
}

func (m *mysqlColumnAttribute) Field() string {
	return m.field
}

func (m *mysqlColumnAttribute) IsUnSignedInt() bool {
	return strings.Contains(m.typ, mysqlUnsigned)
}

type mysqlTableInfo struct {
	name    MysqlTableName
	columns []MysqlColumn
}

func (m *mysqlTableInfo) Name() MysqlTableName {
	return m.name
}

func (m *mysqlTableInfo) Columns() []MysqlColumn {
	return m.columns
}

type exampleMysqlTableMapper struct {
	db *sql.DB
}

func (e *exampleMysqlTableMapper) GetBinlogFormat() (format FormatType, err error) {
	query := "SHOW VARIABLES LIKE 'binlog_format'"
	var name, str string
	err = e.db.QueryRow(query).Scan(&name, &str)
	if err != nil {
		err = fmt.Errorf("QueryRow fail. query: %s, error: %v", query, err)
		return
	}
	format = FormatType(str)
	return
}

func (e *exampleMysqlTableMapper) GetBinlogPosition() (pos Position, err error) {
	query := "SHOW MASTER STATUS"
	var metaDoDb, metaIgnoreDb, executedGTidSet string
	err = e.db.QueryRow(query).Scan(&pos.Filename, &pos.Offset, &metaDoDb, &metaIgnoreDb, &executedGTidSet)
	if err != nil {
		err = fmt.Errorf("query fail. query: %s, error: %v", query, err)
		return
	}
	return
}

func (e *exampleMysqlTableMapper) MysqlTable(name MysqlTableName) (MysqlTable, error) {
	info := &mysqlTableInfo{
		name:    name,
		columns: make([]MysqlColumn, 0, 10),
	}

	query := "desc " + name.String()
	rows, err := e.db.Query(query)
	if err != nil {
		return info, fmt.Errorf("query failed query: %s, error: %v", query, err)
	}
	defer rows.Close()

	for i := 0; rows.Next(); i++ {
		column := &mysqlColumnAttribute{}
		err = rows.Scan(&column.field, &column.typ, &column.null, &column.key, &column.columnDefault, &column.extra)
		if err != nil {
			return info, err
		}
		info.columns = append(info.columns, column)
	}
	return info, nil
}

func showTransaction(t *Transaction) {
	b, err := t.MarshalJSON()
	if err != nil {
		lw.logger().Errorf("MarshalJSON fail. err: %v", err)
		return
	}
	lw.logger().Print("%v", string(b))
}

func ExampleStreamer_Stream() {
	SetLogger(NewDefaultLogger(os.Stdout, DebugLevel))
	dsn := "example:example@tcp(localhost:3306)/mysql?charset=utf8mb4"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		lw.logger().Errorf("open fail. err: %v", err)
		return
	}
	defer db.Close()

	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(4)

	e := &exampleMysqlTableMapper{db: db}
	format, err := e.GetBinlogFormat()
	if err != nil {
		lw.logger().Errorf("getBinlogFormat fail. err: %v", err)
		return
	}

	if !format.IsRow() {
		lw.logger().Errorf("binlog format is not row. format: %v", format)
		return
	}

	pos, err := e.GetBinlogPosition()
	if err != nil {
		lw.logger().Errorf("GetBinlogPosition fail. err: %v", err)
		return
	}

	r, err := NewStreamer(dsn, 1234, e)
	if err != nil {
		lw.logger().Errorf("NewStreamer fail. err: %v", err)
		return
	}
	r.SetStartBinlogPosition(pos)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	processWait := make(chan os.Signal, 1)
	signal.Notify(processWait, os.Kill, os.Interrupt)

	go func() {
		select {
		case <-processWait:
			cancel()
		}
	}()

	err = r.Stream(ctx, func(t *Transaction) error {
		showTransaction(t)
		return nil
	})

	if err != nil {
		log.Fatalf("Stream fail. err: %v", err)
		return
	}
}
