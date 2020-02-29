package gobinlog

//MysqlColumn 用于实现mysql表列的接口
type MysqlColumn interface {
	Field() string       //列字段名
	IsUnSignedInt() bool //是否是无符号整形
}

//MysqlTable 用于实现mysql表的接口
type MysqlTable interface {
	Name() MysqlTableName   //表名
	Columns() []MysqlColumn //所有列
}

//MysqlTableName mysql的表名
type MysqlTableName struct {
	DbName    string `json:"db"`    //数据库名
	TableName string `json:"table"` //表名
}

//String 打印
func (m *MysqlTableName) String() string {
	return "`" + m.DbName + "`.`" + m.TableName + "`"
}

//NewMysqlTableName 创建MysqlTableName
func NewMysqlTableName(database, table string) MysqlTableName {
	return MysqlTableName{
		DbName:    database,
		TableName: table,
	}
}
