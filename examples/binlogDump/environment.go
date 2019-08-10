package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Breeze0806/gbinlog"
	_ "github.com/go-sql-driver/mysql"
)

type environment struct {
	config      *config
	db          *sql.DB
	logger      *os.File
	out         *os.File
	streamer    *gbinlog.Streamer
	tableMapper *mysqlTableMapper
	err         error
}

func newEnvironment(filename string) *environment {
	e := &environment{}
	e.config, e.err = newConfig(filename)
	return e
}

func (e *environment) build() error {
	return e.initLogger().initOut().initDb().initTableMapper().initStreamer().err
}

func (e *environment) initLogger() *environment {
	if e.err != nil {
		return e
	}
	var writer io.Writer

	if e.config.LogStdOut {
		writer = os.Stdout
	} else {
		e.logger, e.err = os.OpenFile(e.config.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
		if e.err != nil {
			return e
		}
		writer = e.logger
	}
	log.SetFlags(log.Lmicroseconds | log.LstdFlags | log.Lshortfile)
	log.SetOutput(writer)
	gbinlog.SetLogger(gbinlog.NewDefaultLogger(writer, e.config.logLevel()))
	return e
}

func (e *environment) initOut() *environment {
	if e.err != nil {
		return e
	}
	e.out, e.err = os.OpenFile(e.config.OutFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	return e
}

func (e *environment) initDb() *environment {
	if e.err != nil {
		return e
	}
	e.db, e.err = sql.Open("mysql", e.config.DSN)
	if e.err != nil {
		return e
	}

	e.db.SetMaxIdleConns(2)
	e.db.SetMaxOpenConns(4)
	return e
}

func (e *environment) initTableMapper() *environment {
	if e.err != nil {
		return e
	}
	e.tableMapper = &mysqlTableMapper{db: e.db}

	return e
}

func (e *environment) initStreamer() *environment {
	if e.err != nil {
		return e
	}

	format, err := e.tableMapper.GetBinlogFormat()
	if err != nil {
		e.err = err
		return e
	}
	if !format.IsRow() {
		e.err = fmt.Errorf("binlog format is not row. format: %v", format)
		return e
	}

	pos, err := e.tableMapper.GetBinlogPosition()
	if err != nil {
		e.err = err
		return e
	}

	e.streamer, e.err = gbinlog.NewStreamer(e.config.DSN, e.config.ServerID, e.tableMapper)
	if err != nil {
		return e
	}
	e.streamer.SetBinlogPosition(pos)
	return e
}

func (e *environment) close() {
	if e.db != nil {
		e.db.Close()
	}
	if e.out != nil {
		e.out.Close()
	}
	if e.logger != nil {
		e.logger.Close()
	}
}
