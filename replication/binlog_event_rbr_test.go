package replication

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestCellLengthAndData(t *testing.T) {
	testCases := []struct {
		typ           byte
		metadata      uint16
		isUnSignedInt bool
		data          []byte
		out           []byte
	}{{
		typ:           TypeTiny,
		data:          []byte{0x82},
		isUnSignedInt: true,
		out:           []byte("130"),
	}, {
		typ:  TypeTiny,
		data: []byte{0xfe},
		out:  []byte("-2"),
	}, {
		typ:  TypeYear,
		data: []byte{0x82},
		out:  []byte("2030"),
	}, {
		typ:           TypeShort,
		data:          []byte{0x82, 0x81},
		isUnSignedInt: true,
		out:           []byte(fmt.Sprintf("%v", 0x8182)),
	}, {
		typ:  TypeShort,
		data: []byte{0xfe, 0xff},
		out:  []byte(fmt.Sprintf("%v", -1-int32(0x0001))),
	}, {
		typ:           TypeInt24,
		data:          []byte{0x83, 0x82, 0x81},
		isUnSignedInt: true,
		out:           []byte(fmt.Sprintf("%v", 0x818283)),
	}, {
		typ:  TypeInt24,
		data: []byte{0xfd, 0xfe, 0xff},
		out:  []byte(fmt.Sprintf("%v", -1-int32(0x000102))),
	}, {
		typ:           TypeLong,
		data:          []byte{0x84, 0x83, 0x82, 0x81},
		isUnSignedInt: true,
		out:           []byte(fmt.Sprintf("%v", 0x81828384)),
	}, {
		typ:  TypeLong,
		data: []byte{0xfc, 0xfd, 0xfe, 0xff},
		out:  []byte(fmt.Sprintf("%v", -1-int32(0x00010203))),
	}, {
		// 3.1415927E+00 = 0x40490fdb
		typ:  TypeFloat,
		data: []byte{0xdb, 0x0f, 0x49, 0x40},
		out:  []byte("3.1415927"),
	}, {
		// 3.1415926535E+00 = 0x400921fb54411744
		typ:  TypeDouble,
		data: []byte{0x44, 0x17, 0x41, 0x54, 0xfb, 0x21, 0x09, 0x40},
		out:  []byte("3.1415926535"),
	}, {
		// 0x58d137c5 = 1490106309 = 2017-03-21 14:25:09 utc
		typ:  TypeTimestamp,
		data: []byte{0xc5, 0x37, 0xd1, 0x58},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 0, time.UTC).
			Local().Format("2006-01-02 15:04:05")),
	}, {
		typ:           TypeLongLong,
		data:          []byte{0x88, 0x87, 0x86, 0x85, 0x84, 0x83, 0x82, 0x81},
		isUnSignedInt: true,
		out:           []byte(fmt.Sprintf("%v", uint64(0x8182838485868788))),
	}, {
		typ:  TypeLongLong,
		data: []byte{0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff},
		out:  []byte(fmt.Sprintf("%v", -1-int64(0x0001020304050607))),
	}, {
		typ: TypeDate,
		// 2010 << 9 + 10 << 5 + 3 = 1029443 = 0x0fb543
		data: []byte{0x43, 0xb5, 0x0f},
		out:  []byte("2010-10-03"),
	}, {
		typ: TypeNewDate,
		// 2010 << 9 + 10 << 5 + 3 = 1029443 = 0x0fb543
		data: []byte{0x43, 0xb5, 0x0f},
		out:  []byte("2010-10-03"),
	}, {
		typ: TypeTime,
		// 154532 = 0x025ba4
		data: []byte{0xa4, 0x5b, 0x02},
		out:  []byte("15:45:32"),
	}, {
		typ: TypeDateTime,
		// 19840304154532 = 0x120b6e4807a4
		data: []byte{0xa4, 0x07, 0x48, 0x6e, 0x0b, 0x12, 0x00, 0x00},
		out:  []byte("1984-03-04 15:45:32"),
	}, {
		typ:      TypeVarchar,
		metadata: 20, // one byte length encoding
		data:     []byte{3, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeVarchar,
		metadata: 384, // two bytes length encoding
		data:     []byte{3, 0, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeBit,
		metadata: 0x0107,
		data:     []byte{0x3, 0x1},
		out:      []byte{3, 1},
	}, {
		// 0x58d137c5 = 1490106309 = 2017-03-21 14:25:09 utc
		typ:      TypeTimestamp2,
		metadata: 0,
		data:     []byte{0x58, 0xd1, 0x37, 0xc5},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 0, time.UTC).
			Local().Format("2006-01-02 15:04:05")),
	}, {
		typ:      TypeTimestamp2,
		metadata: 1,
		data:     []byte{0x58, 0xd1, 0x37, 0xc5, 70},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 700000000, time.UTC).
			Local().Format("2006-01-02 15:04:05.9")),
	}, {
		typ:      TypeTimestamp2,
		metadata: 2,
		data:     []byte{0x58, 0xd1, 0x37, 0xc5, 76},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 760000000, time.UTC).
			Local().Format("2006-01-02 15:04:05.99")),
	}, {
		typ:      TypeTimestamp2,
		metadata: 3,
		// 7650 = 0x1de2
		data: []byte{0x58, 0xd1, 0x37, 0xc5, 0x1d, 0xe2},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 765000000, time.UTC).
			Local().Format("2006-01-02 15:04:05.999")),
	}, {
		typ:      TypeTimestamp2,
		metadata: 4,
		// 7654 = 0x1de6
		data: []byte{0x58, 0xd1, 0x37, 0xc5, 0x1d, 0xe6},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 765400000, time.UTC).
			Local().Format("2006-01-02 15:04:05.9999")),
	}, {
		typ:      TypeTimestamp2,
		metadata: 5,
		// 76540 = 0x0badf6
		data: []byte{0x58, 0xd1, 0x37, 0xc5, 0x0b, 0xad, 0xf6},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 765430000, time.UTC).
			Local().Format("2006-01-02 15:04:05.99999")),
	}, {
		typ:      TypeTimestamp2,
		metadata: 6,
		// 765432 = 0x0badf8
		data: []byte{0x58, 0xd1, 0x37, 0xc5, 0x0b, 0xad, 0xf8},
		out: []byte(time.Date(2017, time.March, 21, 14, 25, 9, 765432000, time.UTC).
			Local().Format("2006-01-02 15:04:05.999999")),
	}, {
		typ:      TypeDateTime2,
		metadata: 0,
		// (2012 * 13 + 6) << 22 + 21 << 17 + 15 << 12 + 45 << 6 + 17)
		// = 109734198097 = 0x198caafb51
		// Then have to add 0x8000000000 = 0x998caafb51
		data: []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51},
		out:  []byte("2012-06-21 15:45:17"),
	}, {
		typ:      TypeDateTime2,
		metadata: 1,
		data:     []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 70},
		out:      []byte("2012-06-21 15:45:17.7"),
	}, {
		typ:      TypeDateTime2,
		metadata: 2,
		data:     []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 76},
		out:      []byte("2012-06-21 15:45:17.76"),
	}, {
		typ:      TypeDateTime2,
		metadata: 3,
		// 7650 = 0x1de2
		data: []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x1d, 0xe2},
		out:  []byte("2012-06-21 15:45:17.765"),
	}, {
		typ:      TypeDateTime2,
		metadata: 4,
		// 7654 = 0x1de6
		data: []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x1d, 0xe6},
		out:  []byte("2012-06-21 15:45:17.7654"),
	}, {
		typ:      TypeDateTime2,
		metadata: 5,
		// 765430 = 0x0badf6
		data: []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x0b, 0xad, 0xf6},
		out:  []byte("2012-06-21 15:45:17.76543"),
	}, {
		typ:      TypeDateTime2,
		metadata: 6,
		// 765432 = 0x0badf8
		data: []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x0b, 0xad, 0xf8},
		out:  []byte("2012-06-21 15:45:17.765432"),
	}, {
		// This first set of tests is from a comment in
		//  sql-common/my_time.c:
		//
		// Disk value  intpart frac   Time value   Memory value
		// 800000.00    0      0      00:00:00.00  0000000000.000000
		// 7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
		// 7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
		// 7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
		// 7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
		// 7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x80, 0x00, 0x00, 0x00},
		out:      []byte("00:00:00.00"),
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x7f, 0xff, 0xff, 0xff},
		out:      []byte("-00:00:00.01"),
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x7f, 0xff, 0xff, 0x9d},
		out:      []byte("-00:00:00.99"),
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x7f, 0xff, 0xff, 0x00},
		out:      []byte("-00:00:01.00"),
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x7f, 0xff, 0xfe, 0xff},
		out:      []byte("-00:00:01.01"),
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x7f, 0xff, 0xfe, 0xf6},
		out:      []byte("-00:00:01.10"),
	}, {
		// Similar tests for 4 decimals.
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0x80, 0x00, 0x00, 0x00, 0x00},
		out:      []byte("00:00:00.0000"),
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0x7f, 0xff, 0xff, 0xff, 0xff},
		out:      []byte("-00:00:00.0001"),
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0x7f, 0xff, 0xff, 0xff, 0x9d},
		out:      []byte("-00:00:00.0099"),
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0x7f, 0xff, 0xff, 0x00, 0x00},
		out:      []byte("-00:00:01.0000"),
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0x7f, 0xff, 0xfe, 0xff, 0xff},
		out:      []byte("-00:00:01.0001"),
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0x7f, 0xff, 0xfe, 0xff, 0xf6},
		out:      []byte("-00:00:01.0010"),
	}, {
		// Similar tests for 6 decimals.
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00},
		out:      []byte("00:00:00.000000"),
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff},
		out:      []byte("-00:00:00.000001"),
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0x9d},
		out:      []byte("-00:00:00.000099"),
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0x7f, 0xff, 0xff, 0x00, 0x00, 0x00},
		out:      []byte("-00:00:01.000000"),
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0x7f, 0xff, 0xfe, 0xff, 0xff, 0xff},
		out:      []byte("-00:00:01.000001"),
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0x7f, 0xff, 0xfe, 0xff, 0xff, 0xf6},
		out:      []byte("-00:00:01.000010"),
	}, {
		// Few more tests.
		typ:      TypeTime2,
		metadata: 0,
		data:     []byte{0x80, 0x00, 0x00},
		out:      []byte("00:00:00"),
	}, {
		typ:      TypeTime2,
		metadata: 1,
		data:     []byte{0x80, 0x00, 0x01, 0x0a},
		out:      []byte("00:00:01.1"),
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x80, 0x00, 0x01, 0x0a},
		out:      []byte("00:00:01.10"),
	}, {
		typ:      TypeTime2,
		metadata: 0,
		// 15 << 12 + 34 << 6 + 54 = 63670 = 0x00f8b6
		// and need to add 0x800000
		data: []byte{0x80, 0xf8, 0xb6},
		out:  []byte("15:34:54"),
	}, {
		typ:      TypeJSON,
		metadata: 2,
		data: []byte{0x0f, 0x00,
			0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98},
		out: []byte(`JSON_OBJECT('a','b')`),
	}, {
		typ:      TypeJSON,
		metadata: 4,
		data: []byte{0x0f, 0x00, 0x00, 0x00,
			0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98},
		out: []byte(`JSON_OBJECT('a','b')`),
	}, {
		typ:      TypeEnum,
		metadata: 1,
		data:     []byte{0x03},
		out:      []byte("3"),
	}, {
		typ:      TypeEnum,
		metadata: 2,
		data:     []byte{0x01, 0x02},
		out:      []byte(fmt.Sprintf("%v", 0x0201)),
	}, {
		typ:      TypeSet,
		metadata: 2,
		data:     []byte{0x01, 0x02},
		out:      []byte{0x01, 0x02},
	}, {
		typ:      TypeString,
		metadata: TypeString<<8 | 5, // maximum length = 5
		data:     []byte{0x04, 0x01, 0x02, 0x03, 0x04},
		out:      []byte{0x01, 0x02, 0x03, 0x04},
	}, {
		// Length is encoded in 10 bits, 2 of them are in a weird place.
		// In this test, we set the two high bits.
		// 773 = 512 + 256 + 5
		// This requires 2 bytes to store the length.
		typ:      TypeString,
		metadata: (TypeString<<8 ^ 0x3000) | 5, // maximum length = 773
		data:     []byte{0x04, 0x00, 0x01, 0x02, 0x03, 0x04},
		out:      []byte{0x01, 0x02, 0x03, 0x04},
	}, {
		// See strings/decimal.c function decimal2bin for why these
		// values are here.
		typ:      TypeNewDecimal,
		metadata: 14<<8 | 4,
		data:     []byte{0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x04, 0xD2},
		out:      []byte("1234567890.1234"),
	}, {
		typ:      TypeNewDecimal,
		metadata: 14<<8 | 4,
		data:     []byte{0x7E, 0xF2, 0x04, 0xC7, 0x2D, 0xFB, 0x2D},
		out:      []byte("-1234567890.1234"),
	}, {
		typ:      TypeNewDecimal,
		metadata: 14<<8 | 4,
		data:     []byte{0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x00, 0x01},
		out:      []byte("1234567890.0001"),
	}, {
		typ:      TypeBlob,
		metadata: 1,
		data:     []byte{0x3, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeBlob,
		metadata: 2,
		data:     []byte{0x3, 0x00, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeBlob,
		metadata: 3,
		data:     []byte{0x3, 0x00, 0x00, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeBlob,
		metadata: 4,
		data:     []byte{0x3, 0x00, 0x00, 0x00, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeVarString,
		metadata: 20, // one byte length encoding
		data:     []byte{3, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeVarString,
		metadata: 384, // two bytes length encoding
		data:     []byte{3, 0, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeGeometry,
		metadata: 1,
		data:     []byte{0x3, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeGeometry,
		metadata: 2,
		data:     []byte{0x3, 0x00, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeGeometry,
		metadata: 3,
		data:     []byte{0x3, 0x00, 0x00, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}, {
		typ:      TypeGeometry,
		metadata: 4,
		data:     []byte{0x3, 0x00, 0x00, 0x00, 'a', 'b', 'c'},
		out:      []byte("abc"),
	}}

	for _, c := range testCases {
		// Copy the data into a larger buffer (one extra byte
		// on both sides), so we make sure the 'pos' field works.
		padded := make([]byte, len(c.data)+2)
		copy(padded[1:], c.data)

		// Test cellLength.
		l, err := cellLength(padded, 1, c.typ, c.metadata)
		if err != nil || l != len(c.data) {
			t.Errorf("tesc cellLength(%v,%v) returned unexpected result: %v %v was expected %v <nil>", c.typ, c.data, l, err, len(c.data))
		}

		// Test CellBytes.
		out, l, err := CellBytes(padded, 1, c.typ, c.metadata, c.isUnSignedInt)
		if err != nil || l != len(c.data) || !bytes.Equal(out, c.out) {
			t.Errorf("tesc cellData(%v,%v) returned unexpected result: %v %v %v, was expecting %v %v <nil>", c.typ, c.data, string(out), l, err, string(c.out), len(c.data))
		}
	}
}
