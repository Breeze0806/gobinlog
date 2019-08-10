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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       true,
		ColumnTypeShort:      true,
		ColumnTypeLong:       true,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   true,
		ColumnTypeInt24:      true,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      true,
		ColumnTypeDouble:     true,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    true,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: true,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    true,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  true,
		ColumnTypeString:     true,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        true,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   true,
		ColumnTypeMediumBlob: true,
		ColumnTypeLongBlob:   true,
		ColumnTypeBlob:       true,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       true,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    true,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       true,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      true,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  true,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: true,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   true,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  true,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   false,
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
		ColumnTypeDecimal:    false,
		ColumnTypeTiny:       false,
		ColumnTypeShort:      false,
		ColumnTypeLong:       false,
		ColumnTypeFloat:      false,
		ColumnTypeDouble:     false,
		ColumnTypeNull:       false,
		ColumnTypeTimestamp:  false,
		ColumnTypeLongLong:   false,
		ColumnTypeInt24:      false,
		ColumnTypeDate:       false,
		ColumnTypeTime:       false,
		ColumnTypeDateTime:   false,
		ColumnTypeYear:       false,
		ColumnTypeNewDate:    false,
		ColumnTypeVarchar:    false,
		ColumnTypeBit:        false,
		ColumnTypeTimestamp2: false,
		ColumnTypeDateTime2:  false,
		ColumnTypeTime2:      false,
		ColumnTypeJSON:       false,
		ColumnTypeNewDecimal: false,
		ColumnTypeEnum:       false,
		ColumnTypeSet:        false,
		ColumnTypeTinyBlob:   false,
		ColumnTypeMediumBlob: false,
		ColumnTypeLongBlob:   false,
		ColumnTypeBlob:       false,
		ColumnTypeVarString:  false,
		ColumnTypeString:     false,
		ColumnTypeGeometry:   true,
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
		FormatType(FormatTypeRow):       true,
		FormatType(FormatTypeMixed):     false,
		FormatType(FormatTypeStatement): false,
	}
	for input, want := range testCases {
		out := input.IsRow()
		if want != out {
			t.Fatalf("want != out input: %v, want: %v out: %v", input, want, out)
		}
	}
}
