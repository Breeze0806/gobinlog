package gbinlog

import (
	"testing"
)

func TestGetStatementCategory(t *testing.T) {
	testCases := map[string]StatementType{
		"BEGIN":    StatementBegin,
		"COMMIT":   StatementCommit,
		"ROLLBACK": StatementRollback,
		"INSERT INTO  values(\"tom\",\"tom@yahoo.com\")":                         StatementInsert,
		"UPDATE items,month SET items.price=month.price WHERE items.id=month.id": StatementUpdate,
		"DELETE FROM runoob_tbl WHERE runoob_id=3":                               StatementDelete,
		"CREATE TABLE IF NOT EXISTS test(id int,name varchar(10)) engine=ndb":    StatementCreate,
		"ALTER TABLE sj_resource_charges add unique emp_name2(cardnumber)":       StatementAlter,
		"DROP TABLE example_table":                                               StatementDrop,
		"TRUNCATE TABLE example_table":                                           StatementTruncate,
		"RENAME TABLE current_db.tbl_name TO other_db.tbl_names":                 StatementRename,
		"SET @@sort_buffer_size=1000000":                                         StatementSet,
		"SELECT * FROM mysql":                                                    StatementUnknown,
		"START STATEMENT":                                                        StatementUnknown,
	}

	for input, want := range testCases {
		out := GetStatementCategory(input)
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestStatementType_String(t *testing.T) {
	testCases := map[StatementType]string{
		StatementBegin:     "begin",
		StatementCommit:    "commit",
		StatementRollback:  "rollback",
		StatementInsert:    "insert",
		StatementUpdate:    "update",
		StatementDelete:    "delete",
		StatementCreate:    "create",
		StatementAlter:     "alter",
		StatementDrop:      "drop",
		StatementTruncate:  "truncate",
		StatementRename:    "rename",
		StatementSet:       "set",
		StatementType(123): "unknown",
	}
	for input, want := range testCases {
		out := input.String()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestStatementType_IsDDL(t *testing.T) {
	testCases := map[StatementType]bool{
		StatementBegin:     false,
		StatementCommit:    false,
		StatementRollback:  false,
		StatementInsert:    false,
		StatementUpdate:    false,
		StatementDelete:    false,
		StatementCreate:    true,
		StatementAlter:     true,
		StatementDrop:      true,
		StatementTruncate:  true,
		StatementRename:    true,
		StatementSet:       false,
		StatementType(123): false,
	}

	for input, want := range testCases {
		out := input.IsDDL()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_String(t *testing.T) {
	testCases := map[ColumnType]string{
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
		ColumnType(128):      "unknown",
	}

	for input, want := range testCases {
		out := input.String()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsInteger(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       true,
		columnTypeShort:      true,
		columnTypeLong:       true,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   true,
		columnTypeInt24:      true,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}

	for input, want := range testCases {
		out := input.IsInteger()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsFloat(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      true,
		columnTypeDouble:     true,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsFloat()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsDecimal(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    true,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: true,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsDecimal()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsString(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    true,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  true,
		columnTypeString:     true,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsString()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsBit(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        true,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsBit()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsBlob(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   true,
		columnTypeMediumBlob: true,
		columnTypeLongBlob:   true,
		columnTypeBlob:       true,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsBlob()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsDate(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       true,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    true,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsDate()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsTime(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       true,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      true,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsTime()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsTimestamp(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  true,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: true,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsTimestamp()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsDateTime(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   true,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  true,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   false,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsDateTime()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestColumnType_IsGeometry(t *testing.T) {
	testCases := map[ColumnType]bool{
		columnTypeDecimal:    false,
		columnTypeTiny:       false,
		columnTypeShort:      false,
		columnTypeLong:       false,
		columnTypeFloat:      false,
		columnTypeDouble:     false,
		columnTypeNull:       false,
		columnTypeTimestamp:  false,
		columnTypeLongLong:   false,
		columnTypeInt24:      false,
		columnTypeDate:       false,
		columnTypeTime:       false,
		columnTypeDateTime:   false,
		columnTypeYear:       false,
		columnTypeNewDate:    false,
		columnTypeVarchar:    false,
		columnTypeBit:        false,
		columnTypeTimestamp2: false,
		columnTypeDateTime2:  false,
		columnTypeTime2:      false,
		columnTypeJSON:       false,
		columnTypeNewDecimal: false,
		columnTypeEnum:       false,
		columnTypeSet:        false,
		columnTypeTinyBlob:   false,
		columnTypeMediumBlob: false,
		columnTypeLongBlob:   false,
		columnTypeBlob:       false,
		columnTypeVarString:  false,
		columnTypeString:     false,
		columnTypeGeometry:   true,
		ColumnType(128):      false,
	}
	for input, want := range testCases {
		out := input.IsGeometry()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestFormatType_IsRow(t *testing.T) {
	testCases := map[FormatType]bool{
		FormatType(formatTypeRow):       true,
		FormatType(formatTypeMixed):     false,
		FormatType(formatTypeStatement): false,
	}
	for input, want := range testCases {
		out := input.IsRow()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestFormatType_IsMixed(t *testing.T) {
	testCases := map[FormatType]bool{
		FormatType(formatTypeRow):       false,
		FormatType(formatTypeMixed):     true,
		FormatType(formatTypeStatement): false,
	}
	for input, want := range testCases {
		out := input.IsMixed()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}

func TestFormatType_IsStatement(t *testing.T) {
	testCases := map[FormatType]bool{
		FormatType(formatTypeRow):       false,
		FormatType(formatTypeMixed):     false,
		FormatType(formatTypeStatement): true,
	}
	for input, want := range testCases {
		out := input.IsStatement()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}
