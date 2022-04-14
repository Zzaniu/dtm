/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
    "errors"
    "fmt"
    "math/rand"
    "runtime/debug"
    "time"

    "github.com/dtm-labs/dtm/dtmcli"
    "github.com/dtm-labs/dtm/dtmcli/dtmimp"
    "github.com/dtm-labs/dtm/dtmcli/logger"
)

// NowForwardDuration will be set in test, trans may be timeout
var NowForwardDuration = time.Duration(0)

// CronForwardDuration will be set in test. cron will fetch trans which expire in CronForwardDuration
var CronForwardDuration = time.Duration(0)

// CronTransOnce cron expired trans. use expireIn as expire time
// 执行到期重试的一条记录
func CronTransOnce() (gid string) {
    defer handlePanic(nil)
    // 查找并更新 next_cron_time 到期的 TransGlobalStore 一条记录
    // 状态必须是 'prepared', 'aborting', 'submitted' 中的一种
    // submitted 说明提交后可能进程异常终止
    // aborting 说明需要回滚
    // prepared 说明全局事务还没有完成
    trans := lockOneTrans(CronForwardDuration)
    if trans == nil {
        return
    }
    gid = trans.Gid
    fmt.Println("gid = ", gid)
    // 等待结果
    trans.WaitResult = true
    // 根据 gid 找到所有的分支， 根据分支 id 正序排序
    branches := GetStore().FindBranches(gid)
    // 对分支进行相应的操作
    err := trans.Process(branches)
    dtmimp.PanicIf(err != nil && !errors.Is(err, dtmcli.ErrFailure), err)
    return
}

// CronExpiredTrans cron expired trans, num == -1 indicate for ever
func CronExpiredTrans(num int) {
    for i := 0; i < num || num == -1; i++ {
        // 查找一个需要重试且到期的记录，进行重试
        // gid 需要重试且到期的全局记录 ID
        gid := CronTransOnce()
        if gid == "" && num != 1 {
            sleepCronTime()
        }
    }
}

// 查找并更新 next_cron_time 到期的 TransGlobalStore 一条记录
func lockOneTrans(expireIn time.Duration) *TransGlobal {
    global := GetStore().LockOneGlobalTrans(expireIn)
    if global == nil {
        return nil
    }
    logger.Infof("cron job return a trans: %s", global.String())
    return &TransGlobal{TransGlobalStore: *global}
}

func handlePanic(perr *error) {
    if err := recover(); err != nil {
        logger.Errorf("----recovered panic %v\n%s", err, string(debug.Stack()))
        if perr != nil {
            *perr = fmt.Errorf("dtm panic: %v", err)
        }
    }
}

func sleepCronTime() {
    normal := time.Duration((float64(conf.TransCronInterval) - rand.Float64()) * float64(time.Second))
    interval := dtmimp.If(CronForwardDuration > 0, 1*time.Millisecond, normal).(time.Duration)
    logger.Debugf("sleeping for %v milli", interval/time.Microsecond)
    time.Sleep(interval)
}
