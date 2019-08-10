package gbinlog

import (
	"strings"

	"github.com/Breeze0806/gbinlog/replication"
)

//StatementType means the sql statement type
type StatementType int

//sql语句类型
const (
	StatementUnknown  StatementType = iota //不知道的语句
	StatementBegin                         //开始语句
	StatementCommit                        //提交语句
	StatementRollback                      //回滚语句
	StatementInsert                        //插入语句
	StatementUpdate                        //更新语句
	StatementDelete                        //删除语句
	StatementCreate                        //创建表语句
	StatementAlter                         //改变表属性语句
	StatementDrop                          //删除表语句
	StatementTruncate                      //截取表语句
	StatementRename                        //重命名表语句
	StatementSet                           //设置属性语句
)

var (
	statementPrefixes = map[string]StatementType{
		"begin":    StatementBegin,
		"commit":   StatementCommit,
		"rollback": StatementRollback,
		"insert":   StatementInsert,
		"update":   StatementUpdate,
		"delete":   StatementDelete,
		"create":   StatementCreate,
		"alter":    StatementAlter,
		"drop":     StatementDrop,
		"truncate": StatementTruncate,
		"rename":   StatementRename,
		"set":      StatementSet,
	}

	statementStrings = map[StatementType]string{
		StatementBegin:    "begin",
		StatementCommit:   "commit",
		StatementRollback: "rollback",
		StatementInsert:   "insert",
		StatementUpdate:   "update",
		StatementDelete:   "delete",
		StatementCreate:   "create",
		StatementAlter:    "alter",
		StatementDrop:     "drop",
		StatementTruncate: "truncate",
		StatementRename:   "rename",
		StatementSet:      "set",
	}
)

//String 表语句类型的信息
func (s StatementType) String() string {
	if s, ok := statementStrings[s]; ok {
		return s
	}
	return "unknown"
}

//IsDDL 是否是数据定义语句
func (s StatementType) IsDDL() bool {
	switch s {
	case StatementAlter, StatementDrop, StatementCreate, StatementTruncate, StatementRename:
		return true
	default:
		return false
	}
}

//GetStatementCategory we can get statement type from a SQL
func GetStatementCategory(sql string) StatementType {
	if i := strings.IndexByte(sql, byte(' ')); i >= 0 {
		sql = sql[:i]
	}
	if s, ok := statementPrefixes[strings.ToLower(sql)]; ok {
		return s
	}
	return StatementUnknown
}

//列数据类型
const (
	ColumnTypeDecimal    = replication.TypeDecimal    //精确实数
	ColumnTypeTiny       = replication.TypeTiny       //int8
	ColumnTypeShort      = replication.TypeShort      //int16
	ColumnTypeLong       = replication.TypeLong       //int32
	ColumnTypeFloat      = replication.TypeFloat      //float32
	ColumnTypeDouble     = replication.TypeDouble     //float64
	ColumnTypeNull       = replication.TypeNull       //null
	ColumnTypeTimestamp  = replication.TypeTimestamp  //时间戳
	ColumnTypeLongLong   = replication.TypeLongLong   //int64
	ColumnTypeInt24      = replication.TypeInt24      //int24
	ColumnTypeDate       = replication.TypeDate       //日期
	ColumnTypeTime       = replication.TypeTime       //时间
	ColumnTypeDateTime   = replication.TypeDateTime   //日期时间
	ColumnTypeYear       = replication.TypeYear       //year
	ColumnTypeNewDate    = replication.TypeNewDate    //日期
	ColumnTypeVarchar    = replication.TypeVarchar    //可变字符串
	ColumnTypeBit        = replication.TypeBit        //bit
	ColumnTypeTimestamp2 = replication.TypeTimestamp2 //时间戳
	ColumnTypeDateTime2  = replication.TypeDateTime2  //日期时间
	ColumnTypeTime2      = replication.TypeTime2      //时间
	ColumnTypeJSON       = replication.TypeJSON       //json
	ColumnTypeNewDecimal = replication.TypeNewDecimal //精确实数
	ColumnTypeEnum       = replication.TypeEnum       //枚举
	ColumnTypeSet        = replication.TypeSet        //字符串
	ColumnTypeTinyBlob   = replication.TypeTinyBlob   //小型二进制
	ColumnTypeMediumBlob = replication.TypeMediumBlob //中型二进制
	ColumnTypeLongBlob   = replication.TypeLongBlob   //长型二进制
	ColumnTypeBlob       = replication.TypeBlob       //长型二进制
	ColumnTypeVarString  = replication.TypeVarString  //可变字符串
	ColumnTypeString     = replication.TypeString     //字符串
	ColumnTypeGeometry   = replication.TypeGeometry   //几何
)

//ColumnType 从binlog中获取的列类型
type ColumnType int

var (
	columnTypeStrings = map[ColumnType]string{
		ColumnTypeDecimal:    "Decimal",
		ColumnTypeTiny:       "Tiny",
		ColumnTypeShort:      "Short",
		ColumnTypeLong:       "Long",
		ColumnTypeFloat:      "Float",
		ColumnTypeDouble:     "Double",
		ColumnTypeNull:       "Null",
		ColumnTypeTimestamp:  "Timestamp",
		ColumnTypeLongLong:   "LongLong",
		ColumnTypeInt24:      "Int24",
		ColumnTypeDate:       "Date",
		ColumnTypeTime:       "Time",
		ColumnTypeDateTime:   "DateTime",
		ColumnTypeYear:       "Year",
		ColumnTypeNewDate:    "NewDate",
		ColumnTypeVarchar:    "Varchar",
		ColumnTypeBit:        "Bit",
		ColumnTypeTimestamp2: "Timestamp2",
		ColumnTypeDateTime2:  "DateTime2",
		ColumnTypeTime2:      "Time2",
		ColumnTypeJSON:       "JSON",
		ColumnTypeNewDecimal: "NewDecimal",
		ColumnTypeEnum:       "Enum",
		ColumnTypeSet:        "Set",
		ColumnTypeTinyBlob:   "TinyBlob",
		ColumnTypeMediumBlob: "MediumBlob",
		ColumnTypeLongBlob:   "LongBlob",
		ColumnTypeBlob:       "Blob",
		ColumnTypeVarString:  "VarString",
		ColumnTypeString:     "String",
		ColumnTypeGeometry:   "Geometry",
	}
)

//String 打印
func (c ColumnType) String() string {
	if s, ok := columnTypeStrings[c]; ok {
		return s
	}
	return "unknown"
}

//IsInteger 是否是整形
func (c ColumnType) IsInteger() bool {
	switch c {
	case ColumnTypeTiny, ColumnTypeShort, ColumnTypeInt24, ColumnTypeLong,
		ColumnTypeLongLong:
		return true
	default:
		return false
	}
}

//IsFloat 是否是实数
func (c ColumnType) IsFloat() bool {
	switch c {
	case ColumnTypeFloat, ColumnTypeDouble:
		return true
	default:
		return false
	}
}

//IsDecimal 是否是精确实数
func (c ColumnType) IsDecimal() bool {
	switch c {
	case ColumnTypeDecimal, ColumnTypeNewDecimal:
		return true
	default:
		return false
	}
}

//IsTimestamp 是否是时间戳
func (c ColumnType) IsTimestamp() bool {
	switch c {
	case ColumnTypeTimestamp, ColumnTypeTimestamp2:
		return true
	default:
		return false
	}
}

//IsTime 是否是时间
func (c ColumnType) IsTime() bool {
	switch c {
	case ColumnTypeTime, ColumnTypeTime2:
		return true
	default:
		return false
	}
}

//IsDate 是否是日期
func (c ColumnType) IsDate() bool {
	switch c {
	case ColumnTypeDate, ColumnTypeNewDate:
		return true
	default:
		return false
	}
}

//IsDateTime 是否是日期时间
func (c ColumnType) IsDateTime() bool {
	switch c {
	case ColumnTypeDateTime, ColumnTypeDateTime2:
		return true
	default:
		return false
	}
}

//IsBlob 是否是二进制
func (c ColumnType) IsBlob() bool {
	switch c {
	case ColumnTypeTinyBlob, ColumnTypeMediumBlob, ColumnTypeLongBlob, ColumnTypeBlob:
		return true
	default:
		return false
	}
}

//IsBit 是否是bit
func (c ColumnType) IsBit() bool {
	switch c {
	case ColumnTypeBit:
		return true
	default:
		return false
	}
}

//IsString 是否是字符串
func (c ColumnType) IsString() bool {
	switch c {
	case ColumnTypeVarchar, ColumnTypeVarString, ColumnTypeString:
		return true
	default:
		return false
	}
}

//IsGeometry 是否是几何
func (c ColumnType) IsGeometry() bool {
	switch c {
	case ColumnTypeGeometry:
		return true
	default:
		return false
	}
}

//FormatType binlog格式类型
type FormatType string

//binlog格式类型
var (
	FormatTypeRow       = FormatType("ROW")       //列
	FormatTypeMixed     = FormatType("MIXED")     //混合
	FormatTypeStatement = FormatType("STATEMENT") //语句
)

//IsRow 是否是列binlog格式类型
func (f FormatType) IsRow() bool {
	return f == FormatTypeRow
}
