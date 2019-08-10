/*
Package gbinlog 将自己伪装成slave获取mysql主从复杂流来
获取mysql数据库的数据变更，提供轻量级，快速的dump协议交互
以及binlog的row模式下的格式解析。使用方式较为简单，首先你
要实现一个MysqlTableMapper

	type mysqlColumnAttribute struct {
		field         string
		typ           string
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

		var null,key,extra string
		var columnDefault []byte
		for i := 0; rows.Next(); i++ {
			column := &mysqlColumnAttribute{}
			err = rows.Scan(&column.field, &column.typ, &null, &key, &columnDefault, &extra)
			if err != nil {
				return info, err
			}
			info.columns = append(info.columns, column)
		}
		return info, nil
	}

再申请一个NewRowStreamer,数据库连接信息为user:password@tcp(ip:port)/db
user是mysql的用户名，password是mysql的密码，ip是mysql的ip地址，
port是mysql的端口，db是mysql的数据库名，serverID要与主库不同，
SetStartBinlogPosition的参数可以通过SHOW MASTER STATUS获取

	dsn := "example:example@tcp(localhost:3306)/mysql"
	r, err := NewStreamer(dsn, 1234, e)
	if err != nil {
		fmt.Printf("NewStreamer fail. err: %v", err)
		return
	}
	r.SetStartBinlogPosition(pos)

然后开启Stream，可以在SendTransactionFun用于处理事务信息函数，如打印事务信息

	ctx := context.Background()
	err = r.Stream(ctx, func(t *Transaction) error {
		fmt.Printf("%v", *t)
		return nil
	})

最后可以通过ctx的cancal结束gbinlog流的同步
*/
package gbinlog
