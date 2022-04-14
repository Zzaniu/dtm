/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
    "fmt"
    "time"

    "github.com/dtm-labs/dtm/dtmcli"
    "github.com/dtm-labs/dtm/dtmcli/dtmimp"
    "github.com/dtm-labs/dtm/dtmcli/logger"
    "github.com/dtm-labs/dtm/dtmutil"
)

// Process process global transaction once
// 并根据分支信息执行对应的分支调用来执行事务
func (t *TransGlobal) Process(branches []TransBranch) error {
    r := t.process(branches)
    // 普罗米修斯 统计信息
    transactionMetrics(t, r == nil)
    return r
}

// process 调用分支执行事务
func (t *TransGlobal) process(branches []TransBranch) error {
    if t.Options != "" {
        // 是否等待啊、重试间隔时间啊、TimeoutToFail 等信息
        dtmimp.MustUnmarshalString(t.Options, &t.TransOptions)
    }
    if t.ExtData != "" {
        // 这个暂时还不太了解是什么信息
        dtmimp.MustUnmarshalString(t.ExtData, &t.Ext)
    }

    // 如果没有设置 WaitResult 就当成异步的处理，搞个 goroutine 去处理就好了
    if !t.WaitResult {
        go func() {
            // 处理分支
            err := t.processInner(branches)
            if err != nil {
                logger.Errorf("processInner err: %v", err)
            }
        }()
        return nil
    }
    // 如果有设置 WaitResult, 需要同步执行
    submitting := t.Status == dtmcli.StatusSubmitted
    err := t.processInner(branches)
    // 如果返回失败了，直接返回失败就好，dtmutil.WrapHandler2 会处理的
    if err != nil {
        return err
    }
    // 如果一开始是 submitted, 执行完后状态不是 succeed 的话，直接返回失败(会有一个全局扫描的程序去处理失败的事务，进行重试或者回滚)
    // 说白了就是 submitted 的时候需要返回一个状态, 只有是succeed的时候才返回成功, 其他都返回失败
    if submitting && t.Status != dtmcli.StatusSucceed {
        return fmt.Errorf("wait result not return success: %w", dtmcli.ErrFailure)
    }
    return nil
}

// processInner 处理分支
// 调一下 ProcessOnce 来执行分支， 执行完后日志记录一下错误信息或者全局事务 id
func (t *TransGlobal) processInner(branches []TransBranch) (rerr error) {
    defer handlePanic(&rerr)
    defer func() {
        // 好像是如果返回 ErrOngoing 错误的话，记一条日志
        if rerr != nil && rerr != dtmcli.ErrOngoing { // dtmcli.ErrOngoing 好像是事务正在进行中吧
            logger.Errorf("processInner got error: %s", rerr.Error())
        }
        if TransProcessedTestChan != nil {
            logger.Debugf("processed: %s", t.Gid)
            // 这个看了下，好像就只是 test 包里面打一个日志？
            TransProcessedTestChan <- t.Gid
            logger.Debugf("notified: %s", t.Gid)
        }
    }()
    logger.Debugf("processing: %s status: %s", t.Gid, t.Status)
    t.lastTouched = time.Now()
    // 调用各自(xa/saga/tcc/msg)的分支处理函数
    // branches：分支信息
    // 成功的话, 状态是 succeed
    // 如果重试的话, 状态是一般是 prepared 或者 aborting ?
    // 如果返回的是 dtmimp.ErrFailure 的话, 就是直接回滚的
    // dtmutil.WrapHandler2 这里有判断错误如果是 dtmimp.ErrFailure 的话直接返回409
    rerr = t.getProcessor().ProcessOnce(branches)
    return
}

// 保存全局事务trans_global和分支trans_branch_op信息
// tcc 的 try 分支是不保存的
func (t *TransGlobal) saveNew() ([]TransBranch, error) {
    t.NextCronInterval = t.getNextCronInterval(cronReset)    // 获取时间间隔
    t.NextCronTime = dtmutil.GetNextTime(t.NextCronInterval) // 下次操作的时间
    t.ExtData = dtmimp.MustMarshalString(t.Ext)
    if t.ExtData == "{}" {
        t.ExtData = ""
    }
    t.Options = dtmimp.MustMarshalString(t.TransOptions)
    if t.Options == "{}" {
        t.Options = ""
    }
    now := time.Now()
    t.CreateTime = &now
    t.UpdateTime = &now
    // 生成分支信息，也就是 trans_branch_op 表中的记录
    branches := t.getProcessor().GenBranches()
    for i := range branches {
        branches[i].CreateTime = &now
        branches[i].UpdateTime = &now
    }
    // saga:
    // 往 trans_global 中插入全局分布式事务信息.
    // 往 trans_branch_op 中插入该全局分布式事务分支正向反向操作信息
    // tcc: try 阶段不保存分支信息, 但会保存全局分布式事务信息
    err := GetStore().MaySaveNewTrans(&t.TransGlobalStore, branches)
    logger.Infof("MaySaveNewTrans result: %v, global: %v branches: %v",
        err, t.TransGlobalStore.String(), dtmimp.MustMarshalString(branches))
    return branches, err
}
