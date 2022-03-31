/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dtm-labs/dtm2/dtmcli"
	"github.com/dtm-labs/dtm2/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm2/dtmcli/logger"
	"github.com/dtm-labs/dtm2/dtmgrpc"
	"github.com/dtm-labs/dtm2/dtmgrpc/dtmgimp"
	"github.com/dtm-labs/dtmdriver"
	"google.golang.org/grpc/metadata"
)

func (t *TransGlobal) touchCronTime(ctype cronType) {
	t.lastTouched = time.Now()
	GetStore().TouchCronTime(&t.TransGlobalStore, t.getNextCronInterval(ctype))
	logger.Infof("TouchCronTime for: %s", t.TransGlobalStore.String())
}

// 修改记录的状态
func (t *TransGlobal) changeStatus(status string) {
	updates := []string{"status", "update_time"}
	now := time.Now()
	// 如果状态是成功，添加 finish_time 字段来更新
	if status == dtmcli.StatusSucceed {
		t.FinishTime = &now
		updates = append(updates, "finish_time")
	} else if status == dtmcli.StatusFailed { // 如果是失败，添加 rollback_time 字段来更新
		t.RollbackTime = &now
		updates = append(updates, "rollback_time")
	}
	t.UpdateTime = &now
	// 修改状态， saga 不修改 finish_time 和 rollback_time 字段
	GetStore().ChangeGlobalStatus(&t.TransGlobalStore, status, updates, status == dtmcli.StatusSucceed || status == dtmcli.StatusFailed)
	logger.Infof("ChangeGlobalStatus to %s ok for %s", status, t.TransGlobalStore.String())
	t.Status = status
}

// changeBranchStatus 更新分支状态
func (t *TransGlobal) changeBranchStatus(b *TransBranch, status string, branchPos int) {
	now := time.Now()
	b.Status = status
	b.FinishTime = &now
	b.UpdateTime = &now
	// 如果不是 mysql 和 postgres 或者 conf.UpdateBranchSync > 0 或者 t.updateBranchSync 那么同步更新分支状态, 否则异步更新状态
	if conf.Store.Driver != dtmimp.DBTypeMysql && conf.Store.Driver != dtmimp.DBTypePostgres || conf.UpdateBranchSync > 0 || t.updateBranchSync {
		// 这里就是更新分支状态, 更新失败直接 panic
		GetStore().LockGlobalSaveBranches(t.Gid, t.Status, []TransBranch{*b}, branchPos)
		logger.Infof("LockGlobalSaveBranches ok: gid: %s old status: %s branches: %s",
			b.Gid, dtmcli.StatusPrepared, b.String())
	} else { // 为了性能优化，把branch的status更新异步化
		// 在 StartSvr 会 go updateBranchAsync() 去异步处理
		updateBranchAsyncChan <- branchStatus{id: b.ID, gid: t.Gid, status: status, finishTime: &now}
	}
}

// isTimeout 这个代码看懂了, 但真没看懂是在干啥子...
// saga 一般 return false
// tcc 一定要设置全局的 TimeoutToFail 兜底, 否则无法回滚啊...
func (t *TransGlobal) isTimeout() bool {
	timeout := t.TimeoutToFail
	if t.TimeoutToFail == 0 && t.TransType != "saga" {
		timeout = conf.TimeoutToFail
	}
	if timeout == 0 {
		return false
	}
	return time.Since(*t.CreateTime)+NowForwardDuration >= time.Duration(timeout)*time.Second
}

// needProcess 需不需要执行分支, 状态为 success 或者 failed 的是不需要再处理的
func (t *TransGlobal) needProcess() bool {
	return t.Status == dtmcli.StatusSubmitted || t.Status == dtmcli.StatusAborting || t.Status == dtmcli.StatusPrepared && t.isTimeout()
}

// getURLResult 执行事务分支，并获取结果
func (t *TransGlobal) getURLResult(url string, branchID, op string, branchPayload []byte) error {
	if url == "" { // empty url is success
		return nil
	}
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		// 使用 resty 请求分支中的 url 执行事务分支
		resp, err := dtmimp.RestyClient.R().SetBody(string(branchPayload)).
			// 设置 url params
			SetQueryParams(map[string]string{
				"gid":        t.Gid,
				"trans_type": t.TransType,
				"branch_id":  branchID,
				"op":         op,
			}).
			SetHeader("Content-type", "application/json").
			SetHeaders(t.Ext.Headers). // 设置请求头
			SetHeaders(t.TransOptions.BranchHeaders). // 设置请求头
			// 如果有 BinData 直接或者是xa, 用 post 请求方式, 其他为 get 请求方式
			Execute(dtmimp.If(branchPayload != nil || t.TransType == "xa", "POST", "GET").(string), url)
		if err != nil {
			return err
		}
		return dtmimp.RespAsErrorCompatible(resp)
	}
	// 如果是 t.Protocol 是 http 的话，直接 panic
	// 这里应该是 grpc 才对了
	dtmimp.PanicIf(t.Protocol == "http", fmt.Errorf("bad url for http: %s", url))
	// grpc handler
	server, method, err := dtmdriver.GetDriver().ParseServerMethod(url)
	if err != nil {
		return err
	}
	conn := dtmgimp.MustGetGrpcConn(server, true)
	ctx := dtmgimp.TransInfo2Ctx(t.Gid, t.TransType, branchID, op, "")
	kvs := dtmgimp.Map2Kvs(t.Ext.Headers)
	kvs = append(kvs, dtmgimp.Map2Kvs(t.BranchHeaders)...)
	ctx = metadata.AppendToOutgoingContext(ctx, kvs...)
	err = conn.Invoke(ctx, method, branchPayload, &[]byte{})
	if err == nil {
		return nil
	}
	return dtmgrpc.GrpcError2DtmError(err)
}

// getBranchResult 获取分支结果信息
// tcc 如果执行失败的话, 不会返回状态, 只有成功的时候会返回 succeed
// saga 在成功的时候返回 succeed, 在失败的时候也不返回状态, 除非正向分支返回的错误是 FAILURE, 则返回 failed
func (t *TransGlobal) getBranchResult(branch *TransBranch) (string, error) {
	// 执行分支事务, 并返回执行结果
	err := t.getURLResult(branch.URL, branch.BranchID, branch.Op, branch.BinData)
	if err == nil {
		return dtmcli.StatusSucceed, nil
	} else if t.TransType == "saga" && branch.Op == dtmcli.BranchAction && errors.Is(err, dtmcli.ErrFailure) {
		return dtmcli.StatusFailed, nil
	} else if errors.Is(err, dtmcli.ErrOngoing) {
		return "", dtmcli.ErrOngoing
	}
	return "", fmt.Errorf("http/grpc result should be specified as in:\nhttps://dtm.pub/summary/arch.html#http\nunkown result will be retried: %s", err)
}

// execBranch 执行事务分支
func (t *TransGlobal) execBranch(branch *TransBranch, branchPos int) error {
	status, err := t.getBranchResult(branch)
	if status != "" {
		// 如果状态不为空, 说明要么成功, 要么失败, 这两种情况都需要更新一下分支的状态
		t.changeBranchStatus(branch, status, branchPos)
	}
	branchMetrics(t, branch, status == dtmcli.StatusSucceed)
	// if time pass 1500ms and NextCronInterval is not default, then reset NextCronInterval
	if err == nil && time.Since(t.lastTouched)+NowForwardDuration >= 1500*time.Millisecond ||
		t.NextCronInterval > conf.RetryInterval && t.NextCronInterval > t.RetryInterval {
		t.touchCronTime(cronReset)
	} else if err == dtmimp.ErrOngoing {
		t.touchCronTime(cronKeep)
	} else if err != nil {
		t.touchCronTime(cronBackoff)
	}
	return err
}

func (t *TransGlobal) getNextCronInterval(ctype cronType) int64 {
	if ctype == cronBackoff {
		return t.NextCronInterval * 2
	} else if ctype == cronKeep {
		return t.NextCronInterval
	} else if t.RetryInterval != 0 {
		return t.RetryInterval
	} else if t.TimeoutToFail > 0 && t.TimeoutToFail < conf.RetryInterval {
		return t.TimeoutToFail
	} else {
		return conf.RetryInterval
	}
}
