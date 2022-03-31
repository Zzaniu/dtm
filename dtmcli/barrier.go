/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmcli

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/dtm-labs/dtm2/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm2/dtmcli/logger"
)

// BarrierBusiFunc type for busi func
type BarrierBusiFunc func(tx *sql.Tx) error

// BranchBarrier every branch info
type BranchBarrier struct {
	TransType string
	Gid       string
	BranchID  string
	Op        string
	BarrierID int
}

const opMsg = "msg"

func (bb *BranchBarrier) String() string {
	return fmt.Sprintf("transInfo: %s %s %s %s", bb.TransType, bb.Gid, bb.BranchID, bb.Op)
}

func (bb *BranchBarrier) newBarrierID() string {
	bb.BarrierID++
	return fmt.Sprintf("%02d", bb.BarrierID)
}

// BarrierFromQuery construct transaction info from request
// 从 url 的 params 中获取 trans_type gid branch_id op, 并返回 BranchBarrier 实例
func BarrierFromQuery(qs url.Values) (*BranchBarrier, error) {
	return BarrierFrom(qs.Get("trans_type"), qs.Get("gid"), qs.Get("branch_id"), qs.Get("op"))
}

// BarrierFrom construct transaction info from request
// 从 url 的 params 中获取 trans_type gid branch_id op
func BarrierFrom(transType, gid, branchID, op string) (*BranchBarrier, error) {
	ti := &BranchBarrier{
		TransType: transType,
		Gid:       gid,
		BranchID:  branchID,
		Op:        op,
	}
	if ti.TransType == "" || ti.Gid == "" || ti.BranchID == "" || ti.Op == "" {
		return nil, fmt.Errorf("invalid trans info: %v", ti)
	}
	return ti, nil
}

// insertBarrier 插入屏障记录
func insertBarrier(tx DB, transType string, gid string, branchID string, op string, barrierID string, reason string) (int64, error) {
	if op == "" {
		return 0, nil
	}
	sql := dtmimp.GetDBSpecial().GetInsertIgnoreTemplate(dtmimp.BarrierTableName+"(trans_type, gid, branch_id, op, barrier_id, reason) values(?,?,?,?,?,?)", "uniq_barrier")
	return dtmimp.DBExec(tx, sql, transType, gid, branchID, op, barrierID, reason)
}

// Call 子事务屏障，详细介绍见 https://zhuanlan.zhihu.com/p/388444465
// tx: 本地数据库的事务对象，允许子事务屏障进行事务操作.
// busiCall: 业务函数，仅在必要时被调用.
// 注意: 屏障表和业务数据库应该是在同一个 mysql 实例, 也就是说这两应该是在一个事务中
func (bb *BranchBarrier) Call(tx *sql.Tx, busiCall BarrierBusiFunc) (rerr error) {
	// 返回 barrier_id
	bid := bb.newBarrierID()
	// 如果 panic 或者 err != nil 回滚, 否则提交
	defer dtmimp.DeferDo(&rerr, func() error {
		return tx.Commit()
	}, func() error {
		return tx.Rollback()
	})

	// 映射(如果是补偿的, 那么映射正向的 op)
	// 如果当前为 action/try 那么 originOp 就是 action/try, 如果当前为 cancel/compensate, originOp 为 try/action
	originOp := map[string]string{
		BranchCancel:     BranchTry,
		BranchCompensate: BranchAction,
	}[bb.Op]

	// 插入屏障的原理是利用 insert ignore into, 然后判断是否成功插入来实现的
	// dtm_barrier.barrier unique key: gid、branch_id、op、barrier_id
	// 正向的时候, 这个一般是可以插进去的(op action  resaon action)(除非有别的线程已经插进去了)
	// 反向的时候, 如果之前正向的操作成功了的话, 这里是无法插入的,
	// 如果这里成功插入的话, 说明之前正向操作没有执行(op action  resaon compensate)
	// tcc try 的话, 这里是插入(op try  reason try), 下面无法插入
	// tcc cancel 的话, 这里无法插入(op try  reason cancel)
	originAffected, oerr := insertBarrier(tx, bb.TransType, bb.Gid, bb.BranchID, originOp, bid, bb.Op)
	// 这个在正向的时候, 一般是插不进去的(op action  resaon action)(rerr 为 nil  currentAffected 为 0) TODO msg除外，后续看到 msg 再回来补
	// 反向的时候, 这个是可以插入成功的(op compensate  resaon compensate)
	// tcc cancel 的话, 这里是插入(op cancel  reason cancel)
	currentAffected, rerr := insertBarrier(tx, bb.TransType, bb.Gid, bb.BranchID, bb.Op, bid, bb.Op)
	logger.Debugf("originAffected: %d currentAffected: %d", originAffected, currentAffected)

	// TODO 这里是跟 msg 相关的, 后续看到 msg 再回来补
	if rerr == nil && bb.Op == opMsg && currentAffected == 0 { // for msg's DoAndSubmit, repeated insert should be rejected.
		return ErrDuplicated
	}

	if rerr == nil {
		rerr = oerr
	}

	// 如果是补偿分支, 且 originAffected > 0(就是说 action/try 能插进去), 说明正向操作/try 压根没有执行或者执行失败了
	// 如果 currentAffected 为0, 说明已经有补偿过了, 不需要再补偿了
	if (bb.Op == BranchCancel || bb.Op == BranchCompensate) && originAffected > 0 || // null compensate
		currentAffected == 0 { // repeated request or dangled request
		return
	}

	// 执行 busiCall
	if rerr == nil {
		rerr = busiCall(tx)
	}
	return
}

// CallWithDB the same as Call, but with *sql.DB
func (bb *BranchBarrier) CallWithDB(db *sql.DB, busiCall BarrierBusiFunc) error {
	tx, err := db.Begin()
	if err == nil {
		err = bb.Call(tx, busiCall)
	}
	return err
}

// QueryPrepared queries prepared data
func (bb *BranchBarrier) QueryPrepared(db *sql.DB) error {
	_, err := insertBarrier(db, bb.TransType, bb.Gid, "00", "msg", "01", "rollback")
	var reason string
	if err == nil {
		sql := fmt.Sprintf("select reason from %s where gid=? and branch_id=? and op=? and barrier_id=?", dtmimp.BarrierTableName)
		err = db.QueryRow(sql, bb.Gid, "00", "msg", "01").Scan(&reason)
	}
	if reason == "rollback" {
		return ErrFailure
	}
	return err
}
