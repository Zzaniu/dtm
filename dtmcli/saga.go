/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmcli

import (
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
)

// Saga struct of saga
type Saga struct {
	dtmimp.TransBase
	orders     map[int][]int
	concurrent bool
}

// NewSaga create a saga
// 实例化一个 Saga
func NewSaga(server string, gid string) *Saga {
	return &Saga{TransBase: *dtmimp.NewTransBase(gid, "saga", server, ""), orders: map[int][]int{}}
}

// Add add a saga step
// saga 如果有 Payloads 的话, 会将 Payloads 赋给 BinPayloads, 执行的时候会判断 BinPayloads 有没有值, 有值用 post 请求
func (s *Saga) Add(action string, compensate string, postData interface{}) *Saga {
	s.Steps = append(s.Steps, map[string]string{"action": action, "compensate": compensate})
	// Payloads 给到 BinPayloads(TransFromContext)
	// BinPayloads 最终给到分支的 BinData(GenBranches)
	// 在请求的时候通过 body 传给事务 url(getURLResult)
	s.Payloads = append(s.Payloads, dtmimp.MustMarshalString(postData))
	return s
}

// AddBranchOrder specify that branch should be after preBranches. branch should is larger than all the element in preBranches
// 添加依赖规则, 比如说 AddBranchOrder(2, []int{0, 1}) 意思是分支 2 需要依赖 分支 0 和 1 执行完后才能执行
func (s *Saga) AddBranchOrder(branch int, preBranches []int) *Saga {
	s.orders[branch] = preBranches
	return s
}

// EnableConcurrent enable the concurrent exec of sub trans
// 设置并发执行
func (s *Saga) EnableConcurrent() *Saga {
	s.concurrent = true
	return s
}

// Submit submit the saga trans
// 提交, 保存全局事务和分支到数据库, 并运行分支
func (s *Saga) Submit() error {
	// 如果允许并发的话, 将 orders 和 concurrent 赋给 CustomData
	s.AddConcurrentContext()
	// 用 resty 请求 dtm 的 /api/dtmsvr/submit 接口
	return dtmimp.TransCallDtm(&s.TransBase, s, "submit")
}

// AddConcurrentContext adds concurrent options to the request context
// 如果允许并发的话, 将 orders 和 concurrent 赋给 CustomData, 在提交的时候自动执行的
func (s *Saga) AddConcurrentContext() {
	if s.concurrent {
		s.CustomData = dtmimp.MustMarshalString(map[string]interface{}{"orders": s.orders, "concurrent": s.concurrent})
	}
}
