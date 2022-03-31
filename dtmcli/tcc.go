/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmcli

import (
	"fmt"
	"net/url"

	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/go-resty/resty/v2"
)

// Tcc struct of tcc
type Tcc struct {
	dtmimp.TransBase
}

// TccGlobalFunc type of global tcc call
type TccGlobalFunc func(tcc *Tcc) (*resty.Response, error)

// TccGlobalTransaction begin a tcc global transaction
// dtm dtm server address
// gid global transaction ID
// tccFunc tcc事务函数，里面会定义全局事务的分支
func TccGlobalTransaction(dtm string, gid string, tccFunc TccGlobalFunc) (rerr error) {
	return TccGlobalTransaction2(dtm, gid, func(t *Tcc) {}, tccFunc)
}

// TccGlobalTransaction2 new version of TccGlobalTransaction, add custom param
// 其实是可以直接调用这个的, 然后在 custom 里面设置一些值, 比如说等待结果
func TccGlobalTransaction2(dtm string, gid string, custom func(*Tcc), tccFunc TccGlobalFunc) (rerr error) {
	tcc := &Tcc{TransBase: *dtmimp.NewTransBase(gid, "tcc", dtm, "")}
	custom(tcc)
	// 请求 prepare, 保存全局事务信息和分支信息，状态为 prepared
	rerr = dtmimp.TransCallDtm(&tcc.TransBase, tcc, "prepare")
	if rerr != nil {
		return rerr
	}
	// 小概率情况下，prepare 成功了，但是由于网络状况导致上面Failure，那么不执行下面defer的内容，等待超时后再回滚标记事务失败，也没有问题
	// TODO 这里要看一下超时回滚是根据哪个字段判断超时的
	defer dtmimp.DeferDo(&rerr, func() error {
		// 如果分支注册 OK, try 也调用 OK, 那么直接调用 submit 来执行对应的分支
		return dtmimp.TransCallDtm(&tcc.TransBase, tcc, "submit")
	}, func() error {
		// 如果下面注册分支失败或者调用 try 失败, 直接修改全局分布式事务为 abort 进行回滚
		return dtmimp.TransCallDtm(&tcc.TransBase, tcc, "abort")
	})
	// 这里注册分支和调用 try
	_, rerr = tccFunc(tcc)
	return
}

// TccFromQuery tcc from request info
func TccFromQuery(qs url.Values) (*Tcc, error) {
	tcc := &Tcc{TransBase: *dtmimp.TransBaseFromQuery(qs)}
	if tcc.Dtm == "" || tcc.Gid == "" {
		return nil, fmt.Errorf("bad tcc info. dtm: %s, gid: %s parentID: %s", tcc.Dtm, tcc.Gid, tcc.BranchID)
	}
	return tcc, nil
}

// CallBranch call a tcc branch
// 调用 registerBranch 保存 confirm 和 cancel 分支, 调用 TransRequestBranch 来通过 resty 调用 try 分支
func (t *Tcc) CallBranch(body interface{}, tryURL string, confirmURL string, cancelURL string) (*resty.Response, error) {
	// 创建分支 branch_id
	branchID := t.NewSubBranchID()
	// 注册 tcc 事务的 取消 和 确认 分支信息, 此时全局事务 trans_global 中的状态必须为 prepared
	err := dtmimp.TransRegisterBranch(&t.TransBase, map[string]string{
		"data":        dtmimp.MustMarshalString(body),
		"branch_id":   branchID,
		BranchConfirm: confirmURL,
		BranchCancel:  cancelURL,
	}, "registerBranch")
	// 如果分支注册失败, 直接返回错误
	if err != nil {
		return nil, err
	}
	// 调用 try
	return dtmimp.TransRequestBranch(&t.TransBase, "POST", body, branchID, BranchTry, tryURL)
}
