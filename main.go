/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmsvr"
	"github.com/dtm-labs/dtm/dtmsvr/config"
	"github.com/dtm-labs/dtm/dtmsvr/storage/registry"

	// load the microserver driver
	_ "github.com/Zzaniu/dtmzrpc"
)

// Version declares version info
var Version string

func version() {
	if Version == "" {
		Version = "0.0.0-dev"
	}
	fmt.Printf("dtm version: %s\n", Version)
}

func usage() {
	cmd := filepath.Base(os.Args[0])
	s := "Usage: %s [options]\n\n"
	fmt.Fprintf(os.Stderr, s, cmd)
	flag.PrintDefaults()
}

var isVersion = flag.Bool("v", false, "Show the version of dtm.")
var isDebug = flag.Bool("d", false, "Set log level to debug.")
var isHelp = flag.Bool("h", false, "Show the help information about etcd.")
var isReset = flag.Bool("r", false, "Reset dtm server data.")
var confFile = flag.String("c", "", "Path to the server configuration file.")

func main() {
	flag.Parse()
	if flag.NArg() > 0 || *isHelp {
		usage()
		return
	} else if *isVersion {
		version()
		return
	}
	config.MustLoadConfig(*confFile)
	conf := &config.Config
	if *isDebug {
		conf.LogLevel = "debug"
	}
	logger.InitLog2(conf.LogLevel, conf.Log.Outputs, conf.Log.RotationEnable, conf.Log.RotationConfigJSON)
	if *isReset {
		// 这里好像是去重新执行一下 sql 语句
		dtmsvr.PopulateDB(false)
	}
	// 设置 M 吧好像是
	_, _ = maxprocs.Set(maxprocs.Logger(logger.Infof))
	// ping 一下看 mysql 或者 redis 依赖是否 OK
	// mysql 是用 select 1 来判断是否 OK
	registry.WaitStoreUp()
	// 启动服务
	dtmsvr.StartSvr()              // 启动dtmsvr的api服务
	go dtmsvr.CronExpiredTrans(-1) // 启动dtmsvr的定时过期查询
	select {}
}
