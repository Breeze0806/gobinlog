# gobinlog

[![Go Report Card][report-img]][report][![GoDoc][doc-img]][doc][![Build Status][ci-img]][ci][![Coverage Status][cov-img]][cov][![LICENSE][license-img]][license]

gobinlog将自己伪装成slave获取mysql主从复杂流来获取mysql数据库的数据变更，提供轻量级，快速的dump协议交互以及binlog的row模式下的格式解析

## Features
+ 轻量级，快速的dump协议交互以及binlog的row模式格式解析
+ 支持mysql 5.6.x,5.7.x,8.0.x的所有数据类型变更
+ 支持使用完整dump协议连接数据库并接受binlog数据
+ 提供函数来接受解析后完整的事务数据
+ 事务数据提供变更的列名，列数据类型，bytes类型的数据

## Requests
+ mysql 5.6+
+ golang 1.11+

## Installation

第三方库管理已经托管到go mod下，请开启环境变量

## Quick Start
### Prepare
+ 对于自建MySQL，需要先开启Binlog写入功能，配置binlog-format为ROW模式
+ 授权examle链接MySQL账号具有作为MySQL slave的权限，如果已有账户可直接grant

### Coding
+ 检查mysql的binlog格式是否是row模式，并且获取一个正确的binlog位置（以文件名和位移量作定义）
+ 实现MysqlTableMapper接口，该接口是用于获取表信息的，主要是获取列属性
+ 表MysqlTable和列MysqlColumn需要实现，用于MysqlTableMapper接口
+ 生成一个RowStreamer，设置一个正确的binlog位置并使用Stream接受数据，具体可以使用sendTransaction进行具体的行为定义

See the [binlogStream](cmd/binlogDump/README.md) and [documentation](https://github.com/Breeze0806/gobinlog#godoc) for more details.

[report-img]:https://goreportcard.com/badge/github.com/Breeze0806/gobinlog
[report]:https://goreportcard.com/report/github.com/Breeze0806/gobinlog
[doc-img]:https://godoc.org/github.com/Breeze0806/gobinlog?status.svg
[doc]:https://godoc.org/github.com/Breeze0806/gobinlog
[ci-img]: https://travis-ci.com/Breeze0806/gobinlog.svg?token=tRFzqxkgFsLcVYfq8uKg&branch=master
[ci]: https://travis-ci.com/Breeze0806/gobinlog
[cov-img]: https://codecov.io/gh/Breeze0806/gobinlog/branch/master/graph/badge.svg?token=UGb27Nysga
[cov]: https://codecov.io/gh/Breeze0806/gobinlog
[license-img]: https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license]: https://github.com/Breeze0806/gobinlog/blob/master/LICENSE
