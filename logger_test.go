package gobinlog

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type mockWriter struct {
	mu  sync.Mutex
	buf *bytes.Buffer
}

func newMockWriter(buf *bytes.Buffer) *mockWriter {
	return &mockWriter{
		buf: buf,
	}
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Write(p)
}

func TestNewDefaultLogger(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	SetLogger(NewDefaultLogger(newMockWriter(buf), DebugLevel))

	testCases := []struct {
		printf func(string, ...interface{})
		format string
		args   []interface{}
	}{
		{
			printf: _log.Debugf,
			format: "debug %d",
			args:   []interface{}{DebugLevel},
		},
		{
			printf: _log.Infof,
			format: "info %d",
			args:   []interface{}{InfoLevel},
		},
		{
			printf: _log.Errorf,
			format: "error %d",
			args:   []interface{}{ErrorLevel},
		},
	}

	for _, v := range testCases {
		buf.Reset()
		v.printf(v.format, v.args...)
		a := strings.Split(buf.String(), ": ")
		out := a[len(a)-1]
		out = out[:len(out)-1]
		want := fmt.Sprintf(v.format, v.args...)

		if want != out {
			t.Fatalf("want != out want: %v[%v] out: %v[%v] log: %v.", want, len(want), out, len(out), buf.String())
		}
	}
}

func TestDefaultLogger_Print(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	SetLogger(NewDefaultLogger(newMockWriter(buf), DebugLevel))

	testCases := []struct {
		print func(...interface{})
		args  []interface{}
	}{
		{
			print: _log.Print,
			args:  []interface{}{DebugLevel},
		},
	}

	for _, v := range testCases {
		buf.Reset()
		v.print(v.args...)
		a := strings.Split(buf.String(), ": ")
		out := a[len(a)-1]
		out = out[:len(out)-1]
		want := fmt.Sprint(v.args...)

		if want != out {
			t.Fatalf("want != out want: %v[%v] out: %v[%v] log: %v.", want, len(want), out, len(out), buf.String())
		}
	}
}
