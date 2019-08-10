package gbinlog

import (
	"testing"
	"time"

	"github.com/Breeze0806/gbinlog/replication"
)

const (
	mysqlPrimaryKeyDescription    = "PRI"            //主键
	mysqlAutoIncrementDescription = "auto_increment" //自增
)

func TestTransaction_MarshalJSON(t *testing.T) {
	testCases := []struct {
		input *Transaction
		want  string
	}{
		{
			input: &Transaction{
				NowPosition: testBinlogPosParseEvents,
				NextPosition: Position{
					Filename: testBinlogPosParseEvents.Filename,
					Offset:   4,
				},
				Events: []*StreamEvent{
					{
						Type:      StatementInsert,
						Timestamp: 1407805592,
						Table:     tesInfo.name,
						Query:     replication.Query{SQL: "insert into vt_test_keyspace.vt_a(id,message)values(1076895760,'abcd')"},
					},
					{
						Type:      StatementUpdate,
						Table:     tesInfo.name,
						Timestamp: 1407805592,
						RowIdentifies: []*RowData{
							{
								Columns: []*ColumnData{
									{
										Filed: "id",
										Data:  []byte("1076895760"),
										Type:  ColumnTypeLong,
									},
									{
										Filed: "message",
										Data:  []byte("abc"),
										Type:  ColumnTypeVarchar,
									},
								},
							},
						},
						RowValues: []*RowData{
							{
								Columns: []*ColumnData{
									{
										Filed: "id",
										Data:  []byte("1076895760"),
										Type:  ColumnTypeLong,
									},
									{
										Filed: "message",
										Data:  []byte("abcd"),
										Type:  ColumnTypeVarchar,
									},
								},
							},
						},
					},
					{
						Type:      StatementDelete,
						Timestamp: 1407805592,
						Table:     tesInfo.name,
						RowIdentifies: []*RowData{
							{
								Columns: []*ColumnData{
									{
										Filed: "id",
										Data:  []byte("1076895760"),
										Type:  ColumnTypeLong,
									},
									{
										Filed: "message",
										Data:  nil,
										Type:  ColumnTypeVarchar,
									},
								},
							},
						},
					},
				},
			},
			want: `{"nowPosition":{"filename":"binlog.000005","offset":0},"nextPosition":{"filename":"binlog.000005","offset":4},"timestamp":"` +
				time.Unix(0, 0).Local().String() + `","events":[{"name":{"db":"vt_test_keyspace","table":"vt_a"},"type":"insert","timestamp":"` +
				time.Date(2014, time.August, 12, 1, 6, 32, 0, time.UTC).Local().String() + `","sql":"insert into vt_test_keyspace.vt_a(id,message)values(1076895760,'abcd')"},{"name":{"db":"vt_test_keyspace","table":"vt_a"},"type":"update","timestamp":"` +
				time.Date(2014, time.August, 12, 1, 6, 32, 0, time.UTC).Local().String() + `","rowValues":[{"Columns":[{"filed":"id","type":"Long","isEmpty":false,"data":"1076895760"},{"filed":"message","type":"Varchar","isEmpty":false,"data":"abcd"}]}],"rowIdentifies":[{"Columns":[{"filed":"id","type":"Long","isEmpty":false,"data":"1076895760"},{"filed":"message","type":"Varchar","isEmpty":false,"data":"abc"}]}]},{"name":{"db":"vt_test_keyspace","table":"vt_a"},"type":"delete","timestamp":"` +
				time.Date(2014, time.August, 12, 1, 6, 32, 0, time.UTC).Local().String() + `","rowValues":null,"rowIdentifies":[{"Columns":[{"filed":"id","type":"Long","isEmpty":false,"data":"1076895760"},{"filed":"message","type":"Varchar","isEmpty":false,"data":null}]}]}]}`,
		},
	}
	for _, v := range testCases {
		out, err := v.input.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != v.want {
			//t.Log(string(out))
			t.Fatalf("want != out,want: %v,out: %v", v.want, string(out))
		}
	}
}
