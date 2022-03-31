/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"fmt"

	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmsvr/storage"
)

// svcSubmit 保存全局事务信息和分支信息，状态为 submitted
// 并根据分支信息执行对应的分支调用来执行事务
func svcSubmit(t *TransGlobal) interface{} {
	t.Status = dtmcli.StatusSubmitted
	// 保存全局事务 trans_global 和分支 trans_branch_op 信息
	branches, err := t.saveNew()

	// 如果主键冲突的话
	if err == storage.ErrUniqueConflict {
		// 通过全局事务 id 获取 trans_global 中的全局事务记录, 找不到直接 panic
		dbt := GetTransGlobal(t.Gid)
		// 如果状态是 prepared，修改状态为 submitted 并更新 update_time
		if dbt.Status == dtmcli.StatusPrepared {
			// 这里只修改 trans_global 的 status 和 update_time 字段的值
			dbt.changeStatus(t.Status)
			// 根据全局分布式事务id寻找该事务所有的分支，顺序是 事务1补偿->事务1正向->事务2补偿->事务2正向...
			branches = GetStore().FindBranches(t.Gid)
		} else if dbt.Status != dtmcli.StatusSubmitted { // 如果状态既不是 prepared 又不是 submitted 直接返回错误
			return fmt.Errorf("current status '%s', cannot sumbmit. %w", dbt.Status, dtmcli.ErrFailure)
		}
	}
	// 根据分支信息执行对应的分支调用来执行事务
	return t.Process(branches)
}

// svcPrepare 保存全局事务信息和分支信息，状态为 prepared
func svcPrepare(t *TransGlobal) interface{} {
	t.Status = dtmcli.StatusPrepared
	// 保存全局事务 trans_global 和分支 trans_branch_op 信息
	_, err := t.saveNew()

	// 如果主键冲突的话
	if err == storage.ErrUniqueConflict {
		// 通过全局事务 id 获取 trans_global 中的全局事务记录, 找不到直接 panic
		dbt := GetTransGlobal(t.Gid)
		// 看下状态是不是 prepared
		if dbt.Status != dtmcli.StatusPrepared {
			return fmt.Errorf("current status '%s', cannot prepare. %w", dbt.Status, dtmcli.ErrFailure)
		}
		return nil
	}
	return err
}

// msg 调用 svcAbort 来回滚事务消息(如果状态为 prepared 的话)
func svcAbort(t *TransGlobal) interface{} {
	// 根据全局事务id从数据库找出来记录并返回TransGlobal
	// 如果没找到的话，直接 panic
	dbt := GetTransGlobal(t.Gid)
	if dbt.TransType == "msg" && dbt.Status == dtmcli.StatusPrepared {
		// 如果全局事务是 msg 且状态为 prepared 直接修改状态为 failed 并更新 rollback_time
		// msg 这种可靠消息最终一致性事务是不回滚的, 应该是第一阶段的事务就失败了, 所以回滚事务消息直接将状态修改为 failed
		dbt.changeStatus(dtmcli.StatusFailed)
		return nil
	}
	// && 优先级大于 ||, 会优先执行 && 操作
	// 如果分布式事务不是 xa 和 tcc 或者全局状态不是 prepared 和 aborting 直接报错
	// 言下之意, msg 已经在上面处理了, 而 saga 是不应该调用这个的
	if t.TransType != "xa" && t.TransType != "tcc" || dbt.Status != dtmcli.StatusPrepared && dbt.Status != dtmcli.StatusAborting {
		return fmt.Errorf("trans type: '%s' current status '%s', cannot abort. %w", dbt.TransType, dbt.Status, dtmcli.ErrFailure)
	}
	// 修改状态为 aborting
	dbt.changeStatus(dtmcli.StatusAborting)
	// 寻找事务分支, 进行重试? 还是回滚? TODO 猜测是重试, 回滚的话大可以直接设置返回值 409
	branches := GetStore().FindBranches(t.Gid)
	// 这个是去处理事务分支
	return dbt.Process(branches)
}

// svcRegisterBranch 如果是 tcc 或者 xa 的情况下, 尝试去注册事务的 取消 和 确认 分支信息
// 此时全局事务 trans_global 中的状态必须为 prepared
func svcRegisterBranch(transType string, branch *TransBranch, data map[string]string) error {
	branches := []TransBranch{*branch, *branch}
	if transType == "tcc" {
		// tcc 的话要新加 cancel 和 confirm
		for i, b := range []string{dtmcli.BranchCancel, dtmcli.BranchConfirm} {
			branches[i].Op = b
			branches[i].URL = data[b]
		}
	} else if transType == "xa" {
		// xa 的话新增 rollback 和 commit
		branches[0].Op = dtmcli.BranchRollback
		branches[0].URL = data["url"]
		branches[1].Op = dtmcli.BranchCommit
		branches[1].URL = data["url"]
	} else {
		return fmt.Errorf("unknow trans type: %s", transType)
	}

	err := dtmimp.CatchP(func() {
		// 这里是在确定全局事务注册了且状态是 prepared 的情况下, 去注册事务分支信息
		GetStore().LockGlobalSaveBranches(branch.Gid, dtmcli.StatusPrepared, branches, -1)
	})
	// 如果全局事务还没有, 或者全局事务状态不是 prepared 的情况下, 直接返回 dtmcli.ErrFailure 进行回滚(一般是空回滚)
	if err == storage.ErrNotFound {
		msg := fmt.Sprintf("no trans with gid: %s status: %s found", branch.Gid, dtmcli.StatusPrepared)
		logger.Errorf(msg)
		return fmt.Errorf("message: %s %w", msg, dtmcli.ErrFailure)
	}
	logger.Infof("LockGlobalSaveBranches result: %v: gid: %s old status: %s branches: %s",
		err, branch.Gid, dtmcli.StatusPrepared, dtmimp.MustMarshalString(branches))
	return err
}
