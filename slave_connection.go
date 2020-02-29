package gobinlog

import (
	"context"
	"sync"

	"github.com/Breeze0806/gobinlog/replication"
	"github.com/Breeze0806/mysql"
)

type dumpConn interface {
	Close() error
	Exec(string) error
	NoticeDump(uint32, uint32, string, uint16) error
	ReadPacket() ([]byte, error)
	HandleErrorPacket([]byte) error
}

// slaveConnection 从github.com/youtube/vitess/go/vt/mysqlctl/slave_connection.go的基础上移植过来
// slaveConn通过StartDumpFromBinlogPosition和mysql库进行binlog dump，将自己伪装成slave，
// 先执行SET @master_binlog_checksum=@@global.binlog_checksum，然后发送 binlog dump包，
// 最后获取binlog日志，通过chan将binlog日志通过binlog event的格式传出。
type slaveConnection struct {
	dc          dumpConn
	destruction sync.Once
	errChan     chan *Error
}

func newSlaveConnection(dumpConn func() (dumpConn, error)) (*slaveConnection, *Error) {
	m, err := dumpConn()
	if err != nil {
		return nil, newError(err).msgf("dumpConn fail")
	}

	s := &slaveConnection{
		dc:      m,
		errChan: make(chan *Error, 1),
	}

	if err := s.prepareForReplication(); err != nil {
		s.close()
		return nil, err
	}

	return s, nil
}

func (s *slaveConnection) errors() <-chan *Error {
	return s.errChan
}

func (s *slaveConnection) close() {
	s.destruction.Do(
		func() {
			if s.dc != nil {
				s.dc.Close()
				_log.Infof("Close closing slave socket to unblock reads")
			}
		})
}

func (s *slaveConnection) prepareForReplication() *Error {
	if err := s.dc.Exec("SET @master_binlog_checksum=@@global.binlog_checksum"); err != nil {
		return newError(err).
			msgf("prepareForReplication failed to set @master_binlog_checksum=@@global.binlog_checksum")
	}
	return nil
}

func (s *slaveConnection) startDumpFromBinlogPosition(ctx context.Context, serverID uint32,
	pos Position) (<-chan replication.BinlogEvent, *Error) {
	_log.Infof("startDumpFromBinlogPosition sending binlog dump command: nowPos: %+v slaveID: %v",
		pos, serverID)
	if err := s.dc.NoticeDump(serverID, uint32(pos.Offset), pos.Filename, 0); err != nil {
		return nil, newError(err).msgf("noticeDump fail")
	}

	// FIXME(xd.fang) I think we can use a buffered channel for better performance.
	eventChan := make(chan replication.BinlogEvent)

	go func() {
		defer func() {
			close(eventChan)
		}()

		for {
			ev, err := s.readBinlogEvent()
			if err != nil {
				_log.Errorf("startDumpFromBinlogPosition readBinlogEvent fail. reason: %v", err)
				s.errChan <- err
				close(s.errChan)
				return
			}

			select {
			case eventChan <- ev:
			case <-ctx.Done():
				_log.Infof("startDumpFromBinlogPosition stop by ctx. reason: %v", ctx.Err())
				s.errChan <- newError(ctx.Err()).msgf("startDumpFromBinlogPosition cancel")
				close(s.errChan)
				return
			}
		}
	}()

	return eventChan, nil
}

func (s *slaveConnection) readBinlogEvent() (replication.BinlogEvent, *Error) {
	buf, err := s.dc.ReadPacket()
	if err != nil {
		return nil, newError(err).msgf("readPacket fail.")
	}
	switch buf[0] {
	case mysql.PacketEOF:
		return nil, newError(errStreamEOF).msgf("readBinlogEvent reach end")
	case mysql.PacketERR:
		return nil, newError(s.dc.HandleErrorPacket(buf)).msgf("fetch error packet")
	default:
	}
	data := make([]byte, len(buf)-1)
	copy(data, buf[1:])
	return replication.NewMysql56BinlogEvent(data), nil
}
