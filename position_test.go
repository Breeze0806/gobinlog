package gbinlog

import (
	"testing"
)

var testBinlogPosParseEvents = Position{
	Filename: "binlog.000005",
	Offset:   0,
}

func TestPosition_IsZero(t *testing.T) {
	testCases := []struct {
		input Position
		want  bool
	}{
		{
			input: Position{
				Filename: "",
				Offset:   0,
			},
			want: true,
		},
		{
			input: Position{
				Filename: "",
				Offset:   1,
			},
			want: true,
		},
		{
			input: testBinlogPosParseEvents,
			want:  true,
		},
		{
			input: Position{
				Filename: "xxx",
				Offset:   1,
			},
			want: false,
		},
	}

	for _, v := range testCases {
		out := v.input.IsZero()
		if v.want != out {
			t.Fatalf("want != out input: %+v want: %v, out: %v", v.input, v.want, out)
		}
	}
}
