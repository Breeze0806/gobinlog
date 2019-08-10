package replication

import (
	"reflect"
	"testing"
)

// StringValuesForTests is a helper method to return the string value
// of all columns in a row in a Row. Only use it in tests, as the
// returned values cannot be interpreted correctly without the schema.
// We assume everything is unsigned in this method.
func (rs *Rows) BytesValuesForTests(tm *TableMap, rowIndex int) ([][]byte, error) {
	var result [][]byte

	valueIndex := 0
	data := rs.Rows[rowIndex].Data
	pos := 0
	for c := 0; c < rs.DataColumns.Count(); c++ {
		if !rs.DataColumns.Bit(c) {
			continue
		}

		if rs.Rows[rowIndex].NullColumns.Bit(valueIndex) {
			// This column is represented, but its value is NULL.
			result = append(result, nil)
			valueIndex++
			continue
		}

		// We have real data
		value, l, err := CellBytes(data, pos, tm.Types[c], tm.Metadata[c], true)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
		pos += l
		valueIndex++
	}

	return result, nil
}

// StringIdentifiesForTests is a helper method to return the string
// identify of all columns in a row in a Row. Only use it in tests, as the
// returned values cannot be interpreted correctly without the schema.
// We assume everything is unsigned in this method.
func (rs *Rows) BytesIdentifiesForTests(tm *TableMap, rowIndex int) ([][]byte, error) {
	var result [][]byte

	valueIndex := 0
	data := rs.Rows[rowIndex].Identify
	pos := 0
	for c := 0; c < rs.IdentifyColumns.Count(); c++ {
		if !rs.IdentifyColumns.Bit(c) {
			continue
		}

		if rs.Rows[rowIndex].NullIdentifyColumns.Bit(valueIndex) {
			// This column is represented, but its value is NULL.
			result = append(result, nil)
			valueIndex++
			continue
		}

		// We have real data
		value, l, err := CellBytes(data, pos, tm.Types[c], tm.Metadata[c], true)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
		pos += l
		valueIndex++
	}

	return result, nil
}

// TestFormatDescriptionEvent tests both MySQL 5.6 and MariaDB 10.0
// FormatDescriptionEvent is working properly.
func TestFormatDescriptionEvent(t *testing.T) {
	// MySQL 5.6
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	ev := NewFormatDescriptionEvent(f, s)
	if !ev.IsValid() {
		t.Fatalf("IsValid() returned false")
	}
	if !ev.IsFormatDescription() {
		t.Fatalf("IsFormatDescription returned false")
	}
	gotF, err := ev.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	if !reflect.DeepEqual(gotF, f) {
		t.Fatalf("Parsed BinlogFormat doesn't match, got:\n%v\nexpected:\n%v", gotF, f)
	}

	// MariaDB
	f = NewMariaDBBinlogFormat()
	s = NewFakeBinlogStream()

	ev = NewFormatDescriptionEvent(f, s)
	if !ev.IsValid() {
		t.Fatalf("IsValid() returned false")
	}
	if !ev.IsFormatDescription() {
		t.Fatalf("IsFormatDescription returned false")
	}
	gotF, err = ev.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	if !reflect.DeepEqual(gotF, f) {
		t.Fatalf("Parsed BinlogFormat doesn't match, got:\n%v\nexpected:\n%v", gotF, f)
	}
}

func TestQueryEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	q := Query{
		Database: "my database",
		SQL:      "my query",
		Charset: &Charset{
			Client: 0x1234,
			Conn:   0x5678,
			Server: 0x9abc,
		},
	}
	ev := NewQueryEvent(f, s, q)
	if !ev.IsValid() {
		t.Fatalf("NewQueryEvent returned an invalid ev")
	}
	if !ev.IsQuery() {
		t.Fatalf("NewQueryEvent returned a non-query ev: %v", ev)
	}
	ev, _, err := ev.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	gotQ, err := ev.Query(f)
	if err != nil {
		t.Fatalf("ev.Query() failed: %v", err)
	}
	if !reflect.DeepEqual(gotQ, q) {
		t.Fatalf("ev.Query() returned %v was expecting %v", gotQ, q)
	}
}

func TestXIDEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	ev := NewXIDEvent(f, s)
	if !ev.IsValid() {
		t.Fatalf("NewXIDEvent().IsValid() is false")
	}
	if !ev.IsXID() {
		t.Fatalf("NewXIDEvent().IsXID() is false")
	}
}

func TestIntVarEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	ev := NewIntVarEvent(f, s, IntVarLastInsertID, 0x123456789abcdef0)
	if !ev.IsValid() {
		t.Fatalf("NewIntVarEvent().IsValid() is false")
	}
	if !ev.IsIntVar() {
		t.Fatalf("NewIntVarEvent().IsIntVar() is false")
	}
	name, value, err := ev.IntVar(f)
	if name != IntVarLastInsertID || value != 0x123456789abcdef0 || err != nil {
		t.Fatalf("IntVar() returned %v/%v/%v", name, value, err)
	}

	ev = NewIntVarEvent(f, s, IntVarInvalidInt, 0x123456789abcdef0)
	if !ev.IsValid() {
		t.Fatalf("NewIntVarEvent().IsValid() is false")
	}
	if !ev.IsIntVar() {
		t.Fatalf("NewIntVarEvent().IsIntVar() is false")
	}
	name, value, err = ev.IntVar(f)
	if err == nil {
		t.Fatalf("IntVar(invalid) returned %v/%v/%v", name, value, err)
	}
}

func TestInvalidEvents(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	// InvalidEvent
	ev := NewInvalidEvent()
	if ev.IsValid() {
		t.Fatalf("NewInvalidEvent().IsValid() is true")
	}

	// InvalidFormatDescriptionEvent
	ev = NewInvalidFormatDescriptionEvent(f, s)
	if !ev.IsValid() {
		t.Fatalf("NewInvalidFormatDescriptionEvent().IsValid() is false")
	}
	if !ev.IsFormatDescription() {
		t.Fatalf("NewInvalidFormatDescriptionEvent().IsFormatDescription() is false")
	}
	if _, err := ev.Format(); err == nil {
		t.Fatalf("NewInvalidFormatDescriptionEvent().Format() returned err=nil")
	}

	// InvalidQueryEvent
	ev = NewInvalidQueryEvent(f, s)
	if !ev.IsValid() {
		t.Fatalf("NewInvalidQueryEvent().IsValid() is false")
	}
	if !ev.IsQuery() {
		t.Fatalf("NewInvalidQueryEvent().IsQuery() is false")
	}
	if _, err := ev.Query(f); err == nil {
		t.Fatalf("NewInvalidQueryEvent().Query() returned err=nil")
	}
}

func TestMariadDBGTIDEVent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()
	s.ServerID = 0x87654321

	// With built-in begin.
	event := NewMariaDBGTIDEvent(f, s, MariadbGTID{Domain: 0, Sequence: 0x123456789abcdef0}, true)
	if !event.IsValid() {
		t.Fatalf("NewMariaDBGTIDEvent().IsValid() is false")
	}
	if !event.IsGTID() {
		t.Fatalf("NewMariaDBGTIDEvent().IsGTID() if false")
	}
	event, _, err := event.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	gtid, hasBegin, err := event.GTID(f)
	if err != nil {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned error: %v", err)
	}
	if !hasBegin {
		t.Fatalf("NewMariaDBGTIDEvent() didn't store hasBegin properly.")
	}
	mgtid, ok := gtid.(MariadbGTID)
	if !ok {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned a non-MariaDBGTID GTID")
	}
	if mgtid.Domain != 0 || mgtid.Server != 0x87654321 || mgtid.Sequence != 0x123456789abcdef0 {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned invalid GITD: %v", mgtid)
	}

	// Without built-in begin.
	event = NewMariaDBGTIDEvent(f, s, MariadbGTID{Domain: 0, Sequence: 0x123456789abcdef0}, false)
	if !event.IsValid() {
		t.Fatalf("NewMariaDBGTIDEvent().IsValid() is false")
	}
	if !event.IsGTID() {
		t.Fatalf("NewMariaDBGTIDEvent().IsGTID() if false")
	}
	event, _, err = event.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	gtid, hasBegin, err = event.GTID(f)
	if err != nil {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned error: %v", err)
	}
	if hasBegin {
		t.Fatalf("NewMariaDBGTIDEvent() didn't store hasBegin properly.")
	}
	mgtid, ok = gtid.(MariadbGTID)
	if !ok {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned a non-MariaDBGTID GTID")
	}
	if mgtid.Domain != 0 || mgtid.Server != 0x87654321 || mgtid.Sequence != 0x123456789abcdef0 {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned invalid GITD: %v", mgtid)
	}
}

func TestTableMapEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	tm := &TableMap{
		Flags:    0x8090,
		Database: "my_database",
		Name:     "my_table",
		Types: []byte{
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeTime,
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeVarchar,
		},
		CanBeNull: NewServerBitmap(10),
		Metadata: []uint16{
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			384, // Length of the varchar field.
		},
	}
	tm.CanBeNull.Set(1, true)
	tm.CanBeNull.Set(2, true)
	tm.CanBeNull.Set(5, true)
	tm.CanBeNull.Set(9, true)

	ev := NewTableMapEvent(f, s, 0x102030405060, tm)
	if !ev.IsValid() {
		t.Fatalf("NewTableMapEvent().IsValid() is false")
	}
	if !ev.IsTableMap() {
		t.Fatalf("NewTableMapEvent().IsTableMap() if false")
	}

	ev, _, err := ev.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	tableID := ev.TableID(f)
	if tableID != 0x102030405060 {
		t.Fatalf("NewTableMapEvent().TableID returned %x", tableID)
	}
	gotTm, err := ev.TableMap(f)
	if err != nil {
		t.Fatalf("NewTableMapEvent().TableMapEvent() returned error: %v", err)
	}
	if !reflect.DeepEqual(gotTm, tm) {
		t.Fatalf("NewTableMapEvent().TableMapEvent() got TableMap:\n%v\nexpected:\n%v", gotTm, tm)
	}
}

func TestRowsEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	tm := &TableMap{
		Flags:    0x8090,
		Database: "my_database",
		Name:     "my_table",
		Types: []byte{
			TypeLong,
			TypeVarchar,
		},
		CanBeNull: NewServerBitmap(2),
		Metadata: []uint16{
			0,
			384,
		},
	}
	tm.CanBeNull.Set(1, true)

	// Do an update packet with all fields set.
	rows := Rows{
		Flags:           0x1234,
		IdentifyColumns: NewServerBitmap(2),
		DataColumns:     NewServerBitmap(2),
		Rows: []Row{
			{
				NullIdentifyColumns: NewServerBitmap(2),
				NullColumns:         NewServerBitmap(2),
				Identify: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x03, 0x00, // len('abc')
					'a', 'b', 'c', // 'abc'
				},
				Data: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x04, 0x00, // len('abcd')
					'a', 'b', 'c', 'd', // 'abcd'
				},
			},
		},
	}

	// All rows are included, none are NULL.
	rows.IdentifyColumns.Set(0, true)
	rows.IdentifyColumns.Set(1, true)
	rows.DataColumns.Set(0, true)
	rows.DataColumns.Set(1, true)

	// Test the Rows we just created, to be sure.
	// 1076895760 is 0x40302010.
	identifies, err := rows.BytesIdentifiesForTests(tm, 0)
	if err != nil {
		t.Fatalf("BytesIdentifiesForTests fail, err: %v", err)
	}
	if expected := [][]byte{[]byte("1076895760"), []byte("abc")}; !reflect.DeepEqual(identifies, expected) {
		t.Fatalf("bad Rows identify, got %v expected %v", identifies, expected)
	}

	values, err := rows.BytesValuesForTests(tm, 0)
	if err != nil {
		t.Fatalf("BytesIdentifiesForTests fail, err: %v", err)
	}
	if expected := [][]byte{[]byte("1076895760"), []byte("abcd")}; !reflect.DeepEqual(values, expected) {
		t.Fatalf("bad Rows data, got %v expected %v", values, expected)
	}

	ev := NewUpdateRowsEvent(f, s, 0x102030405060, rows)
	if !ev.IsValid() {
		t.Fatal("NewRowsEvent().IsValid() is false")
	}
	if !ev.IsUpdateRows() {
		t.Fatal("NewRowsEvent().IsUpdateRows() if false")
	}

	ev, _, err = ev.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	tableID := ev.TableID(f)
	if tableID != 0x102030405060 {
		t.Fatalf("NewRowsEvent().TableID returned %x", tableID)
	}
	gotRows, err := ev.Rows(f, tm)
	if err != nil {
		t.Fatalf("NewRowsEvent().Rows() returned error: %v", err)
	}
	if !reflect.DeepEqual(gotRows, rows) {
		t.Fatalf("NewRowsEvent().Rows() got Rows:\n%v\nexpected:\n%v", gotRows, rows)
	}
}
