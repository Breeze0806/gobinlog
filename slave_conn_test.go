package gbinlog

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
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

func Test_newSlaveConn(t *testing.T) {
	_, err := newSlaveConn(func() (conn dumpConn, e error) {
		return newMockDumpConn(bytes.NewBuffer(nil)), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConn fail. err: %v", err)
	}
}

func Test_slaveConn_startDumpFromBinlogPosition(t *testing.T) {
	SetLogger(NewDefaultLogger(os.Stdout, DebugLevel))

	connBuf := bytes.NewBuffer(nil)
	s, err := newSlaveConn(func() (conn dumpConn, e error) {
		return newMockDumpConn(connBuf), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConn fail. err: %v", err)
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

func Test_slaveConn_startDumpFromBinlogPosition_Error(t *testing.T) {
	logBuf := bytes.NewBuffer(nil)

	SetLogger(NewDefaultLogger(newMockWriter(logBuf), DebugLevel))

	connBuf := bytes.NewBuffer(nil)
	s, err := newSlaveConn(func() (conn dumpConn, e error) {
		return newMockDumpConn(connBuf), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConn fail. err: %v", err)
	}
	defer s.close()

	testCases := []struct {
		input []byte
		want  string
	}{
		{
			input: []byte{mysql.PacketERR, 'm', 'i', 's', 's', '0'},
			want:  "miss0",
		},
	}

	for _, v := range testCases {
		connBuf.Write(v.input)
	}

	event, err := s.startDumpFromBinlogPosition(context.Background(), 1, Position{})
	if err != nil {
		t.Fatalf("startDumpFromBinlogPosition fail. err: %v", err)
	}
	<-event
	for _, v := range testCases {
		out := logBuf.String()
		if !strings.Contains(out, v.want) {
			t.Fatalf("log does not Contains wamt, log: %v, want: %v", out, v.want)
		}
	}
}

func Test_slaveConn_startDumpFromBinlogPosition_EOF(t *testing.T) {
	logBuf := bytes.NewBuffer(nil)

	SetLogger(NewDefaultLogger(newMockWriter(logBuf), DebugLevel))

	connBuf := bytes.NewBuffer(nil)
	s, err := newSlaveConn(func() (conn dumpConn, e error) {
		return newMockDumpConn(connBuf), nil
	})
	if err != nil {
		t.Fatalf("newSlaveConn fail. err: %v", err)
	}
	defer s.close()

	testCases := []struct {
		input []byte
		want  string
	}{
		{
			input: []byte{mysql.PacketEOF, 'm', 'i', 's', 's', '0'},
			want:  ErrStreamEOF.Error(),
		},
	}

	for _, v := range testCases {
		connBuf.Write(v.input)
	}

	event, err := s.startDumpFromBinlogPosition(context.Background(), 1, Position{})
	if err != nil {
		t.Fatalf("startDumpFromBinlogPosition fail. err: %v", err)
	}
	<-event
	for _, v := range testCases {
		out := logBuf.String()
		if !strings.Contains(out, v.want) {
			t.Fatalf("log does not Contains wamt, log: %v, want: %v", out, v.want)
		}
	}
}
