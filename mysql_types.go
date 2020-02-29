package gobinlog

import (
	"strings"

	"github.com/Breeze0806/gobinlog/replication"
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
	columnTypeDecimal    ColumnType = replication.TypeDecimal    //精确实数
	columnTypeTiny                  = replication.TypeTiny       //int8
	columnTypeShort                 = replication.TypeShort      //int16
	columnTypeLong                  = replication.TypeLong       //int32
	columnTypeFloat                 = replication.TypeFloat      //float32
	columnTypeDouble                = replication.TypeDouble     //float64
	columnTypeNull                  = replication.TypeNull       //null
	columnTypeTimestamp             = replication.TypeTimestamp  //时间戳
	columnTypeLongLong              = replication.TypeLongLong   //int64
	columnTypeInt24                 = replication.TypeInt24      //int24
	columnTypeDate                  = replication.TypeDate       //日期
	columnTypeTime                  = replication.TypeTime       //时间
	columnTypeDateTime              = replication.TypeDateTime   //日期时间
	columnTypeYear                  = replication.TypeYear       //year
	columnTypeNewDate               = replication.TypeNewDate    //日期
	columnTypeVarchar               = replication.TypeVarchar    //可变字符串
	columnTypeBit                   = replication.TypeBit        //bit
	columnTypeTimestamp2            = replication.TypeTimestamp2 //时间戳
	columnTypeDateTime2             = replication.TypeDateTime2  //日期时间
	columnTypeTime2                 = replication.TypeTime2      //时间
	columnTypeJSON                  = replication.TypeJSON       //json
	columnTypeNewDecimal            = replication.TypeNewDecimal //精确实数
	columnTypeEnum                  = replication.TypeEnum       //枚举
	columnTypeSet                   = replication.TypeSet        //集合
	columnTypeTinyBlob              = replication.TypeTinyBlob   //小型二进制
	columnTypeMediumBlob            = replication.TypeMediumBlob //中型二进制
	columnTypeLongBlob              = replication.TypeLongBlob   //长型二进制
	columnTypeBlob                  = replication.TypeBlob       //长型二进制
	columnTypeVarString             = replication.TypeVarString  //可变字符串
	columnTypeString                = replication.TypeString     //字符串
	columnTypeGeometry              = replication.TypeGeometry   //几何
)

//ColumnType 从binlog中获取的列类型
type ColumnType int

var (
	columnTypeStrings = map[ColumnType]string{
		columnTypeDecimal:    "Decimal",
		columnTypeTiny:       "Tiny",
		columnTypeShort:      "Short",
		columnTypeLong:       "Long",
		columnTypeFloat:      "Float",
		columnTypeDouble:     "Double",
		columnTypeNull:       "Null",
		columnTypeTimestamp:  "Timestamp",
		columnTypeLongLong:   "LongLong",
		columnTypeInt24:      "Int24",
		columnTypeDate:       "Date",
		columnTypeTime:       "Time",
		columnTypeDateTime:   "DateTime",
		columnTypeYear:       "Year",
		columnTypeNewDate:    "NewDate",
		columnTypeVarchar:    "Varchar",
		columnTypeBit:        "Bit",
		columnTypeTimestamp2: "Timestamp2",
		columnTypeDateTime2:  "DateTime2",
		columnTypeTime2:      "Time2",
		columnTypeJSON:       "JSON",
		columnTypeNewDecimal: "NewDecimal",
		columnTypeEnum:       "Enum",
		columnTypeSet:        "Set",
		columnTypeTinyBlob:   "TinyBlob",
		columnTypeMediumBlob: "MediumBlob",
		columnTypeLongBlob:   "LongBlob",
		columnTypeBlob:       "Blob",
		columnTypeVarString:  "VarString",
		columnTypeString:     "String",
		columnTypeGeometry:   "Geometry",
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
	case columnTypeTiny, columnTypeShort, columnTypeInt24, columnTypeLong,
		columnTypeLongLong:
		return true
	default:
		return false
	}
}

//IsFloat 是否是实数
func (c ColumnType) IsFloat() bool {
	switch c {
	case columnTypeFloat, columnTypeDouble:
		return true
	default:
		return false
	}
}

//IsDecimal 是否是精确实数
func (c ColumnType) IsDecimal() bool {
	switch c {
	case columnTypeDecimal, columnTypeNewDecimal:
		return true
	default:
		return false
	}
}

//IsTimestamp 是否是时间戳
func (c ColumnType) IsTimestamp() bool {
	switch c {
	case columnTypeTimestamp, columnTypeTimestamp2:
		return true
	default:
		return false
	}
}

//IsTime 是否是时间
func (c ColumnType) IsTime() bool {
	switch c {
	case columnTypeTime, columnTypeTime2:
		return true
	default:
		return false
	}
}

//IsDate 是否是日期
func (c ColumnType) IsDate() bool {
	switch c {
	case columnTypeDate, columnTypeNewDate:
		return true
	default:
		return false
	}
}

//IsDateTime 是否是日期时间
func (c ColumnType) IsDateTime() bool {
	switch c {
	case columnTypeDateTime, columnTypeDateTime2:
		return true
	default:
		return false
	}
}

//IsBlob 是否是二进制
func (c ColumnType) IsBlob() bool {
	switch c {
	case columnTypeTinyBlob, columnTypeMediumBlob, columnTypeLongBlob, columnTypeBlob:
		return true
	default:
		return false
	}
}

//IsBit 是否是bit
func (c ColumnType) IsBit() bool {
	switch c {
	case columnTypeBit:
		return true
	default:
		return false
	}
}

//IsString 是否是字符串
func (c ColumnType) IsString() bool {
	switch c {
	case columnTypeVarchar, columnTypeVarString, columnTypeString:
		return true
	default:
		return false
	}
}

//IsGeometry 是否是几何
func (c ColumnType) IsGeometry() bool {
	switch c {
	case columnTypeGeometry:
		return true
	default:
		return false
	}
}

//FormatType binlog格式类型
type FormatType string

//binlog格式类型
var (
	formatTypeRow       = FormatType("ROW")       //列
	formatTypeMixed     = FormatType("MIXED")     //混合
	formatTypeStatement = FormatType("STATEMENT") //语句
)

//IsRow 是否是列binlog格式类型
func (f FormatType) IsRow() bool {
	return f == formatTypeRow
}

//IsMixed 是否是混合binlog格式类型
func (f FormatType) IsMixed() bool {
	return f == formatTypeMixed
}

//IsStatement 是否是语句binlog格式类型
func (f FormatType) IsStatement() bool {
	return f == formatTypeStatement
}
