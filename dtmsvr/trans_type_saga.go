/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
    "errors"
    "fmt"
    "time"

    "github.com/dtm-labs/dtm/dtmcli"
    "github.com/dtm-labs/dtm/dtmcli/dtmimp"
    "github.com/dtm-labs/dtm/dtmcli/logger"
)

type transSagaProcessor struct {
    *TransGlobal
}

func init() {
    registorProcessorCreator("saga", func(trans *TransGlobal) transProcessor {
        return &transSagaProcessor{TransGlobal: trans}
    })
}

// GenBranches 生成分支信息，正向反向信息都在这里，包括请求的url
func (t *transSagaProcessor) GenBranches() []TransBranch {
    branches := []TransBranch{}
    fmt.Printf("t.Steps = %#v\n", t.Steps)
    for i, step := range t.Steps {
        branch := fmt.Sprintf("%02d", i+1)
        for _, op := range []string{dtmcli.BranchCompensate, dtmcli.BranchAction} {
            branches = append(branches, TransBranch{
                Gid:      t.Gid,  // 全局事务 ID
                BranchID: branch, // 分支ID
                BinData:  t.BinPayloads[i],
                URL:      step[op],              // 分支事务请求url
                Op:       op,                    // 正向还是反向操作
                Status:   dtmcli.StatusPrepared, // 事务分支的正向反向操作的状态，初始化为准备状态
            })
        }
    }
    return branches
}

// cSagaCustom 一些控制信息
type cSagaCustom struct {
    Orders     map[int][]int `json:"orders"`     // 顺序执行
    Concurrent bool          `json:"concurrent"` // 并发执行
    cOrders    map[int][]int
}

// branchResult 分支结果
type branchResult struct {
    index   int
    status  string
    started bool
    op      string // 补偿还是正向
}

// ProcessOnce 对分支进行相应的操作
// 执行正向或者补偿, 要看分支当前的状态
// svcSubmit 过来的话, 是正向的
// CronExpiredTrans 这边过来的话, 一般都是正向重试操作
// branches 分支是排序好了的
func (t *transSagaProcessor) ProcessOnce(branches []TransBranch) error {
    // when saga tasks is fetched, it always need to process
    logger.Debugf("status: %s timeout: %t", t.Status, t.isTimeout())
    // 如果当前是提交状态，但是呢又超时了，修改状态为 aborting
    if t.Status == dtmcli.StatusSubmitted && t.isTimeout() {
        // 修改数据库和本身的 status 为 aborting, 数据库还会同步修改 update_time
        t.changeStatus(dtmcli.StatusAborting)
    }
    n := len(branches)

    /*
       func (s *Saga) AddBranchOrder(branch int, preBranches []int) *Saga {
           s.orders[branch] = preBranches
           return s
       }

       csaga := dtmcli.NewSaga(dtmutil.DefaultHTTPServer, dtmcli.MustGenGid(dtmutil.DefaultHTTPServer)).
                   Add(busi.Busi+"/TransOut", busi.Busi+"/TransOutRevert", req).
                   Add(busi.Busi+"/TransOut", busi.Busi+"/TransOutRevert", req).
                   Add(busi.Busi+"/TransIn", busi.Busi+"/TransInRevert", req).
                   Add(busi.Busi+"/TransIn", busi.Busi+"/TransInRevert", req).
                   EnableConcurrent().
                   // 2, 3 分支要在 0, 1 分支执行完成后才执行
                   AddBranchOrder(2, []int{0, 1}).
                   AddBranchOrder(3, []int{0, 1})
    */
    csc := cSagaCustom{Orders: map[int][]int{}, cOrders: map[int][]int{}}
    // 如果控制信息不为空, 参考客户端的 Saga.Submit 中的 AddConcurrentContext
    // 这里主要是客户端提交的时候, 如果设置了允许并发, 会将 orders 和 concurrent 赋给CustomData
    if t.CustomData != "" {
        // 将控制信息写入 cSagaCustom.Orders 和 cSagaCustom.Concurrent
        dtmimp.MustUnmarshalString(t.CustomData, &csc)
        // 相当于反转 k v, 效果如下
        // {2: [0, 1], 3: [0, 1]} => {0: [2, 3], 1: [2, 3]}
        for k, v := range csc.Orders {
            for _, b := range v {
                csc.cOrders[b] = append(csc.cOrders[b], k)
            }
        }
    }
    // 如果设置了并发或者有设置持续多久算失败, 那么需要同步更新分支状态
    if csc.Concurrent || t.TimeoutToFail > 0 { // when saga is not normal, update branch sync
        t.updateBranchSync = true
    }
    // resultStats
    // rsA 补偿   rsC 正向
    var rsAToStart, rsAStarted, rsADone, rsAFailed, rsASucceed, rsCToStart, rsCDone, rsCSucceed int
    // save the branch result 保存分支结果
    branchResults := make([]branchResult, n)
    for i := 0; i < n; i++ {
        b := branches[i]
        // 如果是正向的分支 action
        if b.Op == dtmcli.BranchAction {
            // 如果状态为 prepared
            if b.Status == dtmcli.StatusPrepared {
                rsAToStart++
            } else if b.Status == dtmcli.StatusFailed { // 如果状态为 failed
                rsAFailed++
            }
        }
        // 这里就是用分支信息初始化 branchResult
        branchResults[i] = branchResult{index: i, status: branches[i].Status, op: branches[i].Op}
    }

    // 这个用来检查正向分支是否应该执行, current 为 action 分支, 奇数
    shouldRun := func(current int) bool {
        // if !csc.Concurrent，then check the branch in previous step is succeed
        // 如果不是并发的, 且当前分支不是第1个事务的分支, 那么检查下上一个相同 op 的分支状态是不是 succeed, 不是直接返回 false
        // 因为不是并发的情况下, 肯定是按照顺序来执行的. 如果前面的都没有执行成功, 那么这个也不应该执行
        if !csc.Concurrent && current >= 2 && branchResults[current-2].status != dtmcli.StatusSucceed {
            return false
        }
        // if csc.concurrent, then check the Orders. origin one step correspond to 2 step in dtmsvr
        // 检查下当前依赖的前面的分支是否执行成功, 正向的分支序号为奇数
        for _, pre := range csc.Orders[current/2] {
            // 如果依赖的分支状态有不是 succeed 状态的, 直接返回失败
            if branchResults[pre*2+1].status != dtmcli.StatusSucceed {
                return false
            }
        }
        return true
    }

    // 判断是否应该回滚, current 为 compensate 分支, 偶数
    shouldRollback := func(current int) bool {
        rollbacked := func(i int) bool {
            // current compensate op rollbacked or related action still prepared
            // 如果当前补偿分支为 succeed, 下一个正向操作的分支为 prepared, 则认为已回滚
            return branchResults[i].status == dtmcli.StatusSucceed || branchResults[i+1].status == dtmcli.StatusPrepared
        }
        // 如果已回滚, 则不应该再回滚
        if rollbacked(current) {
            return false
        }
        // if !csc.Concurrent，then check the branch in next step is rollbacked
        // 如果不是并发, 且当前分支不是最后一个补偿分支, 且后面的补偿分支还没有回滚, 那么当前分支不回滚
        if !csc.Concurrent && current < n-2 && !rollbacked(current+2) {
            return false
        }
        // if csc.concurrent, then check the cOrders. origin one step correspond to 2 step in dtmsvr
        // 假设有4个事务, 0, 1 是被依赖的事务(被依赖的先执行), 那么:
        // csc.cOrders = {0: [2, 3], 1: [2, 3]}
        // 如果是并发的, 如果是被依赖的分支的反向操作, 如果不是被依赖的分支还没有回滚, 那么被依赖的不应该回滚
        for _, next := range csc.cOrders[current/2] { // cOrders key 为被依赖的事务(一个事务含正反两个分支), v 为后续的那些事务
            if !rollbacked(2 * next) {
                return false
            }
        }
        // 其他情况应该回滚
        return true
    }

    resultChan := make(chan branchResult, n)

    // 异步执行事务分支, 将结果发给 resultChan
    asyncExecBranch := func(i int) {
        var err error
        defer func() {
            if x := recover(); x != nil {
                err = dtmimp.AsError(x)
            }
            // 不管执行的咋样, 都将结果发送给 resultChan. 执行后会修改分支的状态
            resultChan <- branchResult{index: i, status: branches[i].Status, op: branches[i].Op}
            if err != nil && !errors.Is(err, dtmcli.ErrOngoing) {
                logger.Errorf("exec branch error: %v", err)
            }
        }()
        // 执行事务分支, 并更新分支状态(不是mysql/postgres, 或者conf.UpdateBranchSync>0, 或者是异步是同步更新)
        err = t.execBranch(&branches[i], i)
    }

    // 选择正向操作(正向 index 为奇数)去执行
    pickToRunActions := func() []int {
        // toRun := []int{}  // 原本是这样写的, 用下面的方式更好
        var toRun []int
        for current := 1; current < n; current += 2 {
            // TODO 这里为什么要加 &
            br := &branchResults[current]
            // 如果分支没在执行, 且状态是 prepared, 且正向分支应该执行的情况下, 将分支 index 加入将运行的结果中
            if !br.started && br.status == dtmcli.StatusPrepared && shouldRun(current) {
                toRun = append(toRun, current)
            }
        }
        logger.Debugf("toRun picked for action is: %v branchResults: %v compensate orders: %v", toRun, branchResults, csc.cOrders)
        return toRun
    }

    // 选择反向操作(反向 index 为偶数)去执行
    pickToRunCompensates := func() []int {
        toRun := []int{}
        for current := n - 2; current >= 0; current -= 2 {
            br := &branchResults[current]
            // 如果分支没在执行, 且状态是 prepared, 且反向分支应该执行的情况下, 将分支 index 加入将运行的结果中
            if !br.started && br.status == dtmcli.StatusPrepared && shouldRollback(current) {
                toRun = append(toRun, current)
            }
        }
        logger.Debugf("toRun picked for compensate is: %v branchResults: %v compensate orders: %v", toRun, branchResults, csc.cOrders)
        return toRun
    }

    // 异步执行事务分支
    runBranches := func(toRun []int) {
        for _, b := range toRun {
            branchResults[b].started = true
            // 如果是正向分支, 那么 rsAStarted++, 因为后面有个根据 rsAStarted 来判定是否结束
            if branchResults[b].op == dtmcli.BranchAction {
                rsAStarted++
            }
            go asyncExecBranch(b)
        }
    }

    // 等待结果
    waitDoneOnce := func() {
        select {
        case r := <-resultChan:
            // TODO 这里为什么要加 &
            br := &branchResults[r.index]
            br.status = r.status
            // 如果是正向的, rsADone++
            if r.op == dtmcli.BranchAction {
                rsADone++
                // 正向失败直接回滚, rsAFailed++
                if r.status == dtmcli.StatusFailed {
                    rsAFailed++
                } else if r.status == dtmcli.StatusSucceed { // 成功的话, rsASucceed++
                    rsASucceed++
                }
            } else { // 反向的 rsCDone++
                rsCDone++

                // 反向失败是要一直重试的
                if r.status == dtmcli.StatusSucceed {
                    rsCSucceed++
                }
            }
            logger.Debugf("branch done: %v", r)

        //     结果只等待 3s
        case <-time.After(time.Duration(time.Second * 3)):
            logger.Debugf("wait once for done")
        }
    }

    //
    prepareToCompensate := func() {
        // TODO 执行这行的意义在哪里?
        _ = pickToRunActions() // flag started
        for i := 1; i < len(branchResults); i += 2 {
            // these branches may have run. so flag them to status succeed, then run the corresponding
            // compensate
            // TODO 估计这里的意思是, 只要正向分支在运行, 就认为他需要补偿?
            // 如果正向分支已经在运行了, 且状态是 prepared
            if branchResults[i].started && branchResults[i].status == dtmcli.StatusPrepared {
                // 修改状态为 succeed
                branchResults[i].status = dtmcli.StatusSucceed
            }
        }

        for i, b := range branchResults {
            // 如果是反向分支, 且状态为 succeed 且下一个正向分支不是 prepared(成功的才需要反向回滚嘛)
            if b.op == dtmcli.BranchCompensate && b.status != dtmcli.StatusSucceed &&
                branchResults[i+1].status != dtmcli.StatusPrepared {
                // 补偿数++
                rsCToStart++
            }
        }
        logger.Debugf("rsCToStart: %d branchResults: %v", rsCToStart, branchResults)
    }
    timeLimit := time.Now().Add(time.Duration(conf.RequestTimeout+2) * time.Second)

    // 如果未超时且全局状态是 submitted 且正向失败为0的情况下, 去执行正向操作
    for time.Now().Before(timeLimit) && t.Status == dtmcli.StatusSubmitted && !t.isTimeout() && rsAFailed == 0 {
        // 选择正向操作(正向 index 为奇数)去执行. 这里会处理是不是并发的情况
        // 并发的情况是把所有符合条件的都选出来, 非并发是按顺序选择一个
        toRun := pickToRunActions()
        // 异步执行事务分支
        runBranches(toRun)
        // 如果全部完成, 跳出循环
        if rsADone == rsAStarted { // no branch is running, so break
            break
        }
        waitDoneOnce()
    }

    // 如果全局事务状态为 submitted, 且没有失败的, 且成功的数量和需要运行的数量一致, 认为全局事务成功, 修改状态为 succeed
    if t.Status == dtmcli.StatusSubmitted && rsAFailed == 0 && rsAToStart == rsASucceed {
        t.changeStatus(dtmcli.StatusSucceed)
        return nil
    }

    // 如果状态是 submitted, 但是正向 failed 数量不为 0 或者超时了, 修改全局状态为 aborting
    if t.Status == dtmcli.StatusSubmitted && (rsAFailed > 0 || t.isTimeout()) {
        t.changeStatus(dtmcli.StatusAborting)
    }

    // 如果全局状态是 aborting, 准备回滚
    if t.Status == dtmcli.StatusAborting {
        prepareToCompensate()
    }

    // 如果没有超时, 且全局状态为 aborting, 选择回滚的分支, 执行回滚. 如果全部回滚, 退出循环
    for time.Now().Before(timeLimit) && t.Status == dtmcli.StatusAborting {
        toRun := pickToRunCompensates()
        runBranches(toRun)
        if rsCDone == rsCToStart { // no branch is running, so break
            break
        }
        logger.Debugf("rsCDone: %d rsCToStart: %d", rsCDone, rsCToStart)
        waitDoneOnce()
    }

    // 如果状态为 aborting, 回滚数量等于回滚成功数量, 标记全局事务失败
    if t.Status == dtmcli.StatusAborting && rsCToStart == rsCSucceed {
        t.changeStatus(dtmcli.StatusFailed)
    }
    return nil
}
