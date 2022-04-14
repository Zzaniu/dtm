/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
    "context"

    "github.com/dtm-labs/dtm/dtmcli"
    "github.com/dtm-labs/dtm/dtmgrpc"
    pb "github.com/dtm-labs/dtm/dtmgrpc/dtmgpb"
    "google.golang.org/protobuf/types/known/emptypb"
)

// dtmServer is used to implement dtmgimp.DtmServer.
type dtmServer struct {
    pb.UnimplementedDtmServer
}

// NewGid 生成全局事务 id
func (s *dtmServer) NewGid(ctx context.Context, in *emptypb.Empty) (*pb.DtmGidReply, error) {
    return &pb.DtmGidReply{Gid: GenGid()}, nil
}

// Submit 保存全局事务信息和分支信息，状态为 submitted 并根据分支信息执行对应的分支调用来执行事务
func (s *dtmServer) Submit(ctx context.Context, in *pb.DtmRequest) (*emptypb.Empty, error) {
    r := svcSubmit(TransFromDtmRequest(ctx, in))
    return &emptypb.Empty{}, dtmgrpc.DtmError2GrpcError(r)
}

// Prepare 保存全局事务信息和分支信息，状态为 prepared
func (s *dtmServer) Prepare(ctx context.Context, in *pb.DtmRequest) (*emptypb.Empty, error) {
    r := svcPrepare(TransFromDtmRequest(ctx, in))
    return &emptypb.Empty{}, dtmgrpc.DtmError2GrpcError(r)
}

// Abort msg 事务调用 svcAbort 来回滚事务消息(如果状态为 prepared 的话)
// TODO tcc 和 xa 应该是重试
func (s *dtmServer) Abort(ctx context.Context, in *pb.DtmRequest) (*emptypb.Empty, error) {
    r := svcAbort(TransFromDtmRequest(ctx, in))
    return &emptypb.Empty{}, dtmgrpc.DtmError2GrpcError(r)
}

// RegisterBranch 如果是 tcc 或者是 xa 的话, 尝试去注册事务的 取消 和 确认 分支信息, 此时全局事务 trans_global 中的状态必须为 prepared
func (s *dtmServer) RegisterBranch(ctx context.Context, in *pb.DtmBranchRequest) (*emptypb.Empty, error) {
    r := svcRegisterBranch(in.TransType, &TransBranch{
        Gid:      in.Gid,
        BranchID: in.BranchID,
        Status:   dtmcli.StatusPrepared,
        BinData:  in.BusiPayload,
    }, in.Data)
    return &emptypb.Empty{}, dtmgrpc.DtmError2GrpcError(r)
}
