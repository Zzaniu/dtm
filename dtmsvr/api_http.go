/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"errors"

	"github.com/dtm-labs/dtm2/dtmcli"
	"github.com/dtm-labs/dtm2/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm2/dtmutil"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func addRoute(engine *gin.Engine) {
	// 返回一个全局事务 id
	engine.GET("/api/dtmsvr/newGid", dtmutil.WrapHandler2(newGid))
	// 保存全局事务信息和分支信息，状态为 prepared(tcc 会调用这个)
	engine.POST("/api/dtmsvr/prepare", dtmutil.WrapHandler2(prepare))
	// 保存全局事务信息和分支信息，状态为 submitted 并根据分支信息执行对应的分支调用来执行事务(saga 会调用这个)
	engine.POST("/api/dtmsvr/submit", dtmutil.WrapHandler2(submit))
	// msg 调用 svcAbort 来回滚事务消息(如果状态为 prepared 的话)
	// tcc 和 xa 是回滚
	engine.POST("/api/dtmsvr/abort", dtmutil.WrapHandler2(abort))
	// 如果是 tcc 或者是 xa 的话, 尝试去注册事务的 取消 和 确认 分支信息, 此时全局事务 trans_global 中的状态必须为 prepared
	engine.POST("/api/dtmsvr/registerBranch", dtmutil.WrapHandler2(registerBranch))

	// 这两货是为了兼容旧版本而存在的, 如果不用兼容旧版本的话, 这两就是被删除的货
	engine.POST("/api/dtmsvr/registerXaBranch", dtmutil.WrapHandler2(registerBranch))  // compatible for old sdk
	engine.POST("/api/dtmsvr/registerTccBranch", dtmutil.WrapHandler2(registerBranch)) // compatible for old sdk

	// 这个是支持事务状态信息可查询
	engine.GET("/api/dtmsvr/query", dtmutil.WrapHandler2(query))
	// 这个是查找全局事务信息, 也就是 trans_global 表中的记录
	engine.GET("/api/dtmsvr/all", dtmutil.WrapHandler2(all))

	// add prometheus exporter
	h := promhttp.Handler()
	engine.GET("/api/metrics", func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	})
}

// 返回一个全局的 Gid
func newGid(c *gin.Context) interface{} {
	return map[string]interface{}{"gid": GenGid(), "dtm_result": dtmcli.ResultSuccess}
}

// prepare 保存全局事务信息和分支信息，状态为 prepared
func prepare(c *gin.Context) interface{} {
	return svcPrepare(TransFromContext(c))
}

// submit 保存全局事务信息和分支信息，状态为 submitted 并根据分支信息执行对应的分支调用来执行事务
func submit(c *gin.Context) interface{} {
	return svcSubmit(TransFromContext(c))
}

// tcc 和 xa 是回滚
func abort(c *gin.Context) interface{} {
	return svcAbort(TransFromContext(c))
}

// registerBranch 如果是 tcc 或者是 xa 的话, 尝试去注册事务的 取消 和 确认 分支信息,
// 此时全局事务 trans_global 中的状态必须为 prepared
func registerBranch(c *gin.Context) interface{} {
	data := map[string]string{}
	err := c.BindJSON(&data)
	e2p(err)
	// 这里的数据是写入 trans_branch_op 表中的
	branch := TransBranch{
		Gid:      data["gid"],           // 全局事务 id
		BranchID: data["branch_id"],     // 事务分支 id
		Status:   dtmcli.StatusPrepared, // 状态为准备就绪
		BinData:  []byte(data["data"]),  // 一些数据, 用 body 传递
	}
	// 如果是 tcc 或者 xa 的情况下, 尝试去注册事务的 取消 和 确认 分支信息
	// 此时全局事务 trans_global 中的状态必须为 prepared
	return svcRegisterBranch(data["trans_type"], &branch, data)
}

// query 这个是支持事务状态信息可查询
func query(c *gin.Context) interface{} {
	gid := c.Query("gid")
	if gid == "" {
		return errors.New("no gid specified")
	}
	// 通过 gid 获取全局事务记录信息
	trans := GetStore().FindTransGlobalStore(gid)
	// 通过 gid 获取事务分支记录信息
	branches := GetStore().FindBranches(gid)
	return map[string]interface{}{"transaction": trans, "branches": branches}
}

// all 这个是查找全局事务信息, 也就是 trans_global 表中的记录
func all(c *gin.Context) interface{} {
	// position 类似于分页中的页码, 但是这里是用 id 来实现的, 避免了 offset 的问题
	position := c.Query("position")
	// 默认返回 100 条数据, 可在 param 中通过 limit 来控制
	slimit := dtmimp.OrString(c.Query("limit"), "100")
	// 获取全局事务信息记录
	globals := GetStore().ScanTransGlobalStores(&position, int64(dtmimp.MustAtoi(slimit)))
	return map[string]interface{}{"transactions": globals, "next_position": position}
}
