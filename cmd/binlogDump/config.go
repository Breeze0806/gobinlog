package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/Breeze0806/gobinlog"
)

type config struct {
	DSN       string `json:"dsn"`
	OutFile   string `json:"outFile"`
	LogFile   string `json:"logFile"`
	LogLevel  string `json:"logLevel"`
	ServerID  uint32 `json:"serverID"`
	LogStdOut bool   `json:"logStdOut"`
}

var levelMap = map[string]gobinlog.LogLevel{
	"debug": gobinlog.DebugLevel,
	"info":  gobinlog.InfoLevel,
	"error": gobinlog.ErrorLevel,
}

func (c *config) logLevel() gobinlog.LogLevel {
	return levelMap[c.LogLevel]
}

func newConfig(filename string) (*config, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	c := &config{}
	err = json.Unmarshal(buf, c)
	if err != nil {
		return nil, err
	}
	if _, ok := levelMap[c.LogLevel]; !ok {
		return nil, fmt.Errorf("logLevel is invalid. level: %v", c.LogLevel)
	}
	return c, nil
}