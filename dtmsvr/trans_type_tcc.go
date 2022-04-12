/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
)

type transTccProcessor struct {
	*TransGlobal
}

func init() {
	registorProcessorCreator("tcc", func(trans *TransGlobal) transProcessor { return &transTccProcessor{TransGlobal: trans} })
}

func (t *transTccProcessor) GenBranches() []TransBranch {
	return []TransBranch{}
}

// ProcessOnce tcc 的处理, 相比于 saga 流程简单不要太多...
func (t *transTccProcessor) ProcessOnce(branches []TransBranch) error {
	if !t.needProcess() {
		return nil
	}
	// 这个应该就是 TccGlobalTransaction2 中的“小概率”事件(网络问题),
	// 或者执行第一个 try 就失败了, 那么状态就为 prepare,
	// 这种一般都是超时进来的
	if t.Status == dtmcli.StatusPrepared && t.isTimeout() {
		// 全局事务状态修改为 aborting 后续进行回滚
		t.changeStatus(dtmcli.StatusAborting)
	}
	// 如果状态是 submitted 执行 confirm 分支, 否则执行 cancel 分支
	op := dtmimp.If(t.Status == dtmcli.StatusSubmitted, dtmcli.BranchConfirm, dtmcli.BranchCancel).(string)
	for current := len(branches) - 1; current >= 0; current-- {
		// 因为 tcc 的 confirm 和 cancel 都是不允许失败的, 所以如果状态为 prepared 的话一直去执行知道变成 succeed 就好了
		if branches[current].Op == op && branches[current].Status == dtmcli.StatusPrepared {
			logger.Debugf("branch info: current: %d ID: %d", current, branches[current].ID)
			// 执行事务分支, 成功的时候会修改数据库状态为 succeed, 失败的时候不会修改状态
			// 所以 tcc 的分支就两个状态, prepared 和 succeed
			err := t.execBranch(&branches[current], current)
			if err != nil {
				return err
			}
		}
	}
	// 如果执行成功, 修改数据库全局分布式事务状态为 succeed, 否则修改为 failed
	t.changeStatus(dtmimp.If(t.Status == dtmcli.StatusSubmitted, dtmcli.StatusSucceed, dtmcli.StatusFailed).(string))
	return nil
}
