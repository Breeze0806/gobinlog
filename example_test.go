package gbinlog

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlUnsigned = "unsigned" //无符号
)

//列属性
type mysqlColumnAttribute struct {
	field string //列名
	typ   string //列类型
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
		var null, key, extra string
		var columnDefault []byte
		err = rows.Scan(&column.field, &column.typ, &null, &key, &columnDefault, &extra)
		if err != nil {
			return info, err
		}
		info.columns = append(info.columns, column)
	}
	return info, nil
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
	pos := Position{
		Filename: "mysql-bin.000004",
		Offset:   2757,
	}
	s, err := NewStreamer(dsn, 1234, e)
	if err != nil {
		lw.logger().Errorf("NewStreamer fail. err: %v", err)
		return
	}
	s.SetBinlogPosition(pos)

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

	err = s.Stream(ctx, func(t *Transaction) error {
		fmt.Printf("%v", *t)
		return nil
	})

	if err != nil {
		log.Fatalf("Stream fail. err: %v", err)
		return
	}

	err = s.Error()
	if err != nil {
		log.Fatalf("Stream fail. err: %v", err)
	}
}
