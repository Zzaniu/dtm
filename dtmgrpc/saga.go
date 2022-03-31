/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmgrpc

import (
	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmgrpc/dtmgimp"
	"google.golang.org/protobuf/proto"
)

// SagaGrpc struct of saga
type SagaGrpc struct {
	dtmcli.Saga
}

// NewSagaGrpc create a saga
// 实例化一个 Saga
func NewSagaGrpc(server string, gid string) *SagaGrpc {
	return &SagaGrpc{Saga: *dtmcli.NewSaga(server, gid)}
}

// Add add a saga step
// 将分支添加到 Steps, 并将 payload 赋给 BinPayloads
func (s *SagaGrpc) Add(action string, compensate string, payload proto.Message) *SagaGrpc {
	s.Steps = append(s.Steps, map[string]string{"action": action, "compensate": compensate})
	// BinPayloads 最终给到分支的 BinData(GenBranches), 通过 dtmgpb.DtmRequest.BinPayloads 传给服务端
	s.BinPayloads = append(s.BinPayloads, dtmgimp.MustProtoMarshal(payload))
	return s
}

// AddBranchOrder specify that branch should be after preBranches. branch should is larger than all the element in preBranches
// 添加依赖规则
func (s *SagaGrpc) AddBranchOrder(branch int, preBranches []int) *SagaGrpc {
	s.Saga.AddBranchOrder(branch, preBranches)
	return s
}

// EnableConcurrent enable the concurrent exec of sub trans
// 设置并发
func (s *SagaGrpc) EnableConcurrent() *SagaGrpc {
	s.Saga.EnableConcurrent()
	return s
}

// Submit submit the saga trans
// 提交, 保存全局事务和分支到数据库, 并运行分支
func (s *SagaGrpc) Submit() error {
	// 如果允许并发的话, 将 orders 和 concurrent 赋给 CustomData
	s.Saga.AddConcurrentContext()
	// rpc 调用服务, 调用 Submit 服务来保存事务信息和分支信息，并开始执行
	return dtmgimp.DtmGrpcCall(&s.Saga.TransBase, "Submit")
}
