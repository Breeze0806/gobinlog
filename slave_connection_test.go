package gobinlog

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Breeze0806/mysql"
)

type mockDumpConn struct {
	reader *bufio.Reader
}

func newMockDumpConn(buf *bytes.Buffer) *mockDumpConn {
	return &mockDumpConn{
		reader: bufio.NewReader(buf),
	}
}

func (m *mockDumpConn) Close() error {
	return nil
}

func (m *mockDumpConn) Exec(_ string) error {
	return nil
}
func (m *mockDumpConn) NoticeDump(_ uint32, _ uint32, _ string, _ uint16) error {
	return nil
}

func (m *mockDumpConn) ReadPacket() ([]byte, error) {
	return m.reader.ReadBytes('0')
}

func (m *mockDumpConn) HandleErrorPacket(data []byte) error {
	return fmt.Errorf("%v", string(data))
}

func Test_newSlaveConnection(t *testing.T) {
	_, err := newSlaveConnection(func() (conn dumpConn, e error) {
		return newMockDumpConn(bytes.NewBuffer(nil)), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConnection fail. err: %v", err)
	}
}

func Test_slaveConnection_startDumpFromBinlogPosition(t *testing.T) {
	connBuf := bytes.NewBuffer(nil)
	s, err := newSlaveConnection(func() (conn dumpConn, e error) {
		return newMockDumpConn(connBuf), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConnection fail. err: %v", err)
	}
	defer s.close()

	testCases := []struct {
		input []byte
		want  string
	}{
		{
			input: []byte{mysql.PacketOK, 's', 't', 'a', 'r', 't', '0'},
			want:  "start0",
		},
		{
			input: []byte{mysql.PacketOK, 'x', 'x', 'x', '0'},
			want:  "xxx0",
		},
		{
			input: []byte{mysql.PacketOK, 'e', 'n', 'd', '0'},
			want:  "end0",
		},
	}

	for _, v := range testCases {
		connBuf.Write(v.input)
	}

	events, err := s.startDumpFromBinlogPosition(context.Background(), 1, Position{})
	if err != nil {
		t.Fatalf("startDumpFromBinlogPosition fail. err: %v", err)
	}
	for _, v := range testCases {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			out := string(ev.Bytes())
			if v.want != out {
				t.Fatalf("want != out,want: %v, out: %v", v.want, out)
			}
		}
	}
}

func Test_slaveConnection_startDumpFromBinlogPosition_Error(t *testing.T) {
	connBuf := bytes.NewBuffer(nil)
	s, err := newSlaveConnection(func() (conn dumpConn, e error) {
		return newMockDumpConn(connBuf), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConnection fail. err: %v", err)
	}
	defer s.close()

	testCase := struct {
		input []byte
		want  string
	}{
		input: []byte{mysql.PacketERR, 'm', 'i', 's', 's', '0'},
		want:  "miss0",
	}

	connBuf.Write(testCase.input)

	event, err := s.startDumpFromBinlogPosition(context.Background(), 1, Position{})
	if err != nil {
		t.Fatalf("startDumpFromBinlogPosition fail. err: %v", err)
	}
	<-event
	err = <-s.errors()
	if !strings.Contains(err.Error(), testCase.want) {
		t.Fatalf("log does not Contains wamt, error: %v, want: %v", err, testCase.want)
	}
}

func Test_slaveConnection_startDumpFromBinlogPosition_EOF(t *testing.T) {
	connBuf := bytes.NewBuffer(nil)
	s, err := newSlaveConnection(func() (conn dumpConn, e error) {
		return newMockDumpConn(connBuf), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConnection fail. err: %v", err)
	}
	defer s.close()

	testCase := struct {
		input []byte
		want  error
	}{

		input: []byte{mysql.PacketEOF, 'm', 'i', 's', 's', '0'},
		want:  errStreamEOF,
	}

	connBuf.Write(testCase.input)

	event, err := s.startDumpFromBinlogPosition(context.Background(), 1, Position{})
	if err != nil {
		t.Fatalf("startDumpFromBinlogPosition fail. err: %v", err)
	}
	<-event
	sErr := <-s.errors()
	if sErr.Original() != testCase.want {
		t.Fatalf("log does not Contains wamt, error: %v, want: %v", sErr, testCase.want)
	}
}
