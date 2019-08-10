package main

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/Breeze0806/gbinlog"
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
	return strings.Contains(strings.ToLower(m.typ), mysqlUnsigned)
}

type mysqlTableInfo struct {
	name    gbinlog.MysqlTableName
	columns []gbinlog.MysqlColumn
}

func (m *mysqlTableInfo) Name() gbinlog.MysqlTableName {
	return m.name
}

func (m *mysqlTableInfo) Columns() []gbinlog.MysqlColumn {
	return m.columns
}

type mysqlTableMapper struct {
	db   *sql.DB
	info *mysqlTableInfo
}

func (m *mysqlTableMapper) GetBinlogFormat() (format gbinlog.FormatType, err error) {
	query := "SHOW VARIABLES LIKE 'binlog_format'"
	var name, str string
	err = m.db.QueryRow(query).Scan(&name, &str)
	if err != nil {
		err = fmt.Errorf("QueryRow fail. query: %s, error: %v", query, err)
		return
	}
	format = gbinlog.FormatType(str)
	return
}

func (m *mysqlTableMapper) GetBinlogPosition() (pos gbinlog.Position, err error) {
	query := "SHOW MASTER STATUS"
	var metaDoDb, metaIgnoreDb, executedGTidSet string
	err = m.db.QueryRow(query).Scan(&pos.Filename, &pos.Offset, &metaDoDb, &metaIgnoreDb, &executedGTidSet)
	if err != nil {
		err = fmt.Errorf("query fail. query: %s, error: %v", query, err)
		return
	}
	return
}

func (m *mysqlTableMapper) MysqlTable(name gbinlog.MysqlTableName) (gbinlog.MysqlTable, error) {
	if m.info != nil {
		return m.info, nil
	}

	info := &mysqlTableInfo{
		name:    name,
		columns: make([]gbinlog.MysqlColumn, 0, 10),
	}

	query := "desc " + name.String()
	rows, err := m.db.Query(query)
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
	m.info = info
	return info, nil
}

func showTransaction(t *gbinlog.Transaction, w io.Writer) {
	b, err := t.MarshalJSON()
	if err != nil {
		return
	}
	fmt.Fprintln(w, string(b))
}
