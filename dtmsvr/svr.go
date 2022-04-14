/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
    "context"
    "fmt"
    "net"
    "time"

    "github.com/dtm-labs/dtm/dtmcli"
    "github.com/dtm-labs/dtm/dtmcli/logger"
    "github.com/dtm-labs/dtm/dtmgrpc"
    "github.com/dtm-labs/dtm/dtmgrpc/dtmgimp"
    "github.com/dtm-labs/dtm/dtmgrpc/dtmgpb"
    "github.com/dtm-labs/dtm/dtmutil"
    "github.com/dtm-labs/dtmdriver"
    "google.golang.org/grpc"
)

// StartSvr StartSvr
func StartSvr() {
    logger.Infof("start dtmsvr")
    setServerInfoMetrics()

    // 设置 resty 超时时间
    dtmcli.GetRestyClient().SetTimeout(time.Duration(conf.RequestTimeout) * time.Second)
    // 在客户端拦截器设置超时时间，如果传入的有超时，就用传入的，没有就用全局的
    dtmgrpc.AddUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        ctx2, cancel := context.WithTimeout(ctx, time.Duration(conf.RequestTimeout)*time.Second)
        defer cancel()
        return invoker(ctx2, method, req, reply, cc, opts...)
    })

    // 1.初始化一个 gin.Engine, 设置为 gin.ReleaseMode
    // 2.使用一个 gin.Recovery() 中间件
    // 3.使用一个打印 body 的中间件
    // 4.注册一个支持任何 method 的 ping 接口
    app := dtmutil.GetGinApp()
    // 这里应该是高一些 普罗米修斯 的统计信息
    app = httpMetrics(app)
    // 添加路由， 处理各种状态
    addRoute(app)
    logger.Infof("dtmsvr listen at: %d", conf.HTTPPort)
    go func() {
        err := app.Run(fmt.Sprintf(":%d", conf.HTTPPort))
        if err != nil {
            logger.Errorf("start server err: %v", err)
        }
    }()

    // start grpc server
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.GrpcPort))
    logger.FatalIfError(err)
    // 新建 GRPC 服务，搞两个拦截器
    s := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcMetrics, dtmgimp.GrpcServerLog))
    // 将服务注册到 GRPC
    dtmgpb.RegisterDtmServer(s, &dtmServer{})
    logger.Infof("grpc listening at %v", lis.Addr())
    go func() {
        // 启动服务
        err := s.Serve(lis)
        logger.FatalIfError(err)
    }()

    // 这里是搞一个 goroutine 去创建或者更新分支的状态
    for i := 0; i < int(conf.UpdateBranchAsyncGoroutineNum); i++ {
        go updateBranchAsync()
    }

    time.Sleep(100 * time.Millisecond)
    // 注册 Driver 包含解析器、注册器
    err = dtmdriver.Use(conf.MicroService.Driver)
    logger.FatalIfError(err)
    // 注册服务，要使用上面注册解析器的 Driver 去注册
    err = dtmdriver.GetDriver().RegisterGrpcService(conf.MicroService.Target, conf.MicroService.EndPoint)
    logger.FatalIfError(err)
}

// PopulateDB setup mysql data
func PopulateDB(skipDrop bool) {
    GetStore().PopulateData(skipDrop)
}

// UpdateBranchAsyncInterval interval to flush branch
var UpdateBranchAsyncInterval = 200 * time.Millisecond
var updateBranchAsyncChan chan branchStatus = make(chan branchStatus, 1000)

// updateBranchAsync 异步更新事务分支状态
func updateBranchAsync() {
    flushBranchs := func() {
        defer dtmutil.RecoverPanic(nil)
        updates := []TransBranch{}
        started := time.Now()
        checkInterval := 20 * time.Millisecond
        for time.Since(started) < UpdateBranchAsyncInterval-checkInterval && len(updates) < 20 {
            select {
            case updateBranch := <-updateBranchAsyncChan:
                updates = append(updates, TransBranch{
                    ModelBase:  dtmutil.ModelBase{ID: updateBranch.id},
                    Gid:        updateBranch.gid,
                    Status:     updateBranch.status,
                    FinishTime: updateBranch.finishTime,
                })
            case <-time.After(checkInterval): // 不需要关闭
            }
        }
        for len(updates) > 0 {
            // 更新分支, 如果发生冲突的话, 更新 status、finish_time、update_time 字段
            rowAffected, err := GetStore().UpdateBranches(updates, []string{"status", "finish_time", "update_time"})

            if err != nil {
                logger.Errorf("async update branch status error: %v", err)
                time.Sleep(1 * time.Second)
            } else {
                logger.Infof("flushed %d branch status to db. affected: %d", len(updates), rowAffected)
                updates = []TransBranch{}
            }
        }

    }
    for { // flush branches every 200ms
        flushBranchs()
    }
}
