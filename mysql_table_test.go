package gbinlog

import "testing"

func TestMysqlTableName_String(t *testing.T) {
	testCases := []struct {
		input MysqlTableName
		want  string
	}{
		{
			input: NewMysqlTableName("db", "table"),
			want:  "`db`.`table`",
		},
	}

	for _, v := range testCases {
		out := v.input.String()
		if v.want != out {
			t.Fatalf("want != out input: %+v want: %v, out: %v", v.input, v.want, out)
		}
	}
}
