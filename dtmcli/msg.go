/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmcli

import (
	"database/sql"
	"errors"

	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
)

// Msg reliable msg type
type Msg struct {
	dtmimp.TransBase
}

// NewMsg create new msg
func NewMsg(server string, gid string) *Msg {
	return &Msg{TransBase: *dtmimp.NewTransBase(gid, "msg", server, "")}
}

// Add add a new step
// msg 添加 action
func (s *Msg) Add(action string, postData interface{}) *Msg {
	s.Steps = append(s.Steps, map[string]string{"action": action})
	s.Payloads = append(s.Payloads, dtmimp.MustMarshalString(postData))
	return s
}

// Prepare prepare the msg, msg will later be submitted
// 保存全局事务信息和分支信息，状态为 prepared
func (s *Msg) Prepare(queryPrepared string) error {
	s.QueryPrepared = dtmimp.OrString(queryPrepared, s.QueryPrepared)
	return dtmimp.TransCallDtm(&s.TransBase, s, "prepare")
}

// Submit submit the msg
// 保存全局事务信息和分支信息, 状态为 submitted, 并根据分支信息执行对应的分支调用来执行事务
func (s *Msg) Submit() error {
	return dtmimp.TransCallDtm(&s.TransBase, s, "submit")
}

// DoAndSubmitDB short method for Do on db type. please see DoAndSubmit
func (s *Msg) DoAndSubmitDB(queryPrepared string, db *sql.DB, busiCall BarrierBusiFunc) error {
	return s.DoAndSubmit(queryPrepared, func(bb *BranchBarrier) error {
		return bb.CallWithDB(db, busiCall)
	})
}

// DoAndSubmit one method for the entire prepare->busi->submit
// the error returned by busiCall will be returned
// if busiCall return ErrFailure, then abort is called directly
// if busiCall return not nil error other than ErrFailure, then DoAndSubmit will call queryPrepared to get the result
func (s *Msg) DoAndSubmit(queryPrepared string, busiCall func(bb *BranchBarrier) error) error {
	bb, err := BarrierFrom(s.TransType, s.Gid, "00", "msg") // a special barrier for msg QueryPrepared
	if err == nil {
		err = s.Prepare(queryPrepared) // barrier 未失败的话, 保存全局事务和回查 url, 状态为 prepare
	}
	if err == nil {
		errb := busiCall(bb) // 业务函数, 也就是第一阶段的业务/事务
		// 如果返回的不是 FAILURE, 则进行回查
		if errb != nil && !errors.Is(errb, ErrFailure) {
			// if busicall return an error other than failure, we will query the result
			// 这里有可能是业务执行成功, 但是进程挂了, 回查下是否执行成功(TODO 盲猜是查 barrier 表)
			_, err = dtmimp.TransRequestBranch(&s.TransBase, "GET", nil, bb.BranchID, bb.Op, queryPrepared)
		}
		// 这里应该是执行失败了, 调用 abort 进行回滚
		if errors.Is(errb, ErrFailure) || errors.Is(err, ErrFailure) {
			// msg 调用 abort 会直接修改全局事务为 failed
			_ = dtmimp.TransCallDtm(&s.TransBase, s, "abort")
		} else if err == nil {
			// 如果回查是 OK 的, 直接提交去执行后面的流程(后面的流程是不会回滚的, 执行失败的话直接重试)
			err = s.Submit()
		}
		// 其他情况直接返回 errb
		if errb != nil {
			return errb
		}
	}
	return err
}
