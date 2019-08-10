# gbinlog

[![Build Status][ci-img]][ci][![Coverage Status][cov-img]][cov][![LICENSE][license-img]][license]

gbinlog将自己伪装成slave获取mysql主从复杂流来获取mysql数据库的数据变更，提供轻量级，快速的dump协议交互以及binlog的row模式下的格式解析

## Features
+ 轻量级，快速的dump协议交互以及binlog的row模式格式解析
+ 支持mysql 5.6.x,5.7.x,8.0.x的所有数据类型变更
+ 支持使用完整dump协议连接数据库并接受binlog数据
+ 提供函数来接受解析后完整的事务数据
+ 事务数据提供变更的列名，列数据类型，bytes类型的数据

## Requests
+ mysql 5.6+
+ golang 1.9+

## Installation
go get github.com/Breeze0806/mysql
go get github.com/Breeze0806/gbinlog

## Quick Start
### Prepare
+ 对于自建MySQL，需要先开启Binlog写入功能，配置binlog-format为ROW模式
+ 授权examle链接MySQL账号具有作为MySQL slave的权限，如果已有账户可直接grant

### Coding
+ 检查mysql的binlog格式是否是row模式，并且获取一个正确的binlog位置（以文件名和位移量作定义）
+ 实现MysqlTableMapper接口，该接口是用于获取表信息的，主要是获取列属性
+ 表MysqlTable和列MysqlColumn需要实现，用于MysqlTableMapper接口
+ 生成一个RowStreamer，设置一个正确的binlog位置并使用Stream接受数据，具体可以使用sendTransaction进行具体的行为定义

See the [binlogStream](examples/binlogDump/README.md) and [doocumentation](https://github.com/Breeze0806/gbinlog#godoc) for more details.

### GoDoc

运行make doc，就可以使用浏览器打开[documentation](http://localhost:6080/pkg/github.com/Breeze0806/gbinlog/)

### GoReport

see [goreportcard](https://github.com/gojp/goreportcard) for more detals

#### Install
```bash
go get github.com/gojp/goreportcard
make install
```
#### Modify
you should modify in download/download.go
```go
	if ex {
		log.Println("Update", root.Repo)
		err = root.VCS.Download(fullLocalPath)
		if err != nil && firstAttempt {
			// may have been rebased; we delete the directory, then try one more time:
			log.Printf("Failed to download %q (%v), trying again...", root.Repo, err.Error())
			err = os.RemoveAll(fullLocalPath)
			if err != nil {
				log.Println("Failed to delete path:", fullLocalPath, err)
			}
			return download(path, dest, false)
		} else if err != nil {
			return root, err
		}
	}
```
to
```go
    if ex {
        log.Println("Update", root.Repo)
        return root,nil
    }
```
and copy gbinlog to _repos/src/github.com/Breeze0806/gbinlog

#### Run 
```bash
go build && ./goreportcard -http=:6060
```
用浏览器打开[GoReport](http://localhost:6060)，键入github.com/Breeze0806/gbinlog获取报告

[ci-img]: https://travis-ci.com/Breeze0806/gbinlog.svg?token=tRFzqxkgFsLcVYfq8uKg&branch=master
[ci]: https://travis-ci.com/Breeze0806/gbinlog
[cov-img]: https://codecov.io/gh/Breeze0806/gbinlog/branch/master/graph/badge.svg?token=UGb27Nysga
[cov]: https://codecov.io/gh/Breeze0806/gbinlog
[license-img]: https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license]: https://github.com/Breeze0806/gbinlog/blob/master/LICENSE