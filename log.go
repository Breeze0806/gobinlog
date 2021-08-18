package gobinlog

import (
	"os"

	"github.com/Breeze0806/go/log"
	"github.com/Breeze0806/mysql"
)

var _log log.Logger = log.NewDefaultLogger(os.Stderr, log.DebugLevel, "[gobinlog]")

//SetLogger 设置一个符合Logger日志来打印binlog包的调试信息
func SetLogger(logger log.Logger) {
	log.SetLogger(logger)
	mysql.SetLogger(logger)
	_log = log.GetLogger()
}
