/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmgimp

import (
	"fmt"
	"sync"

	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmgrpc/dtmgpb"
	grpc "google.golang.org/grpc"
)

type rawCodec struct{}

func (cb rawCodec) Marshal(v interface{}) ([]byte, error) {
	return v.([]byte), nil
}

func (cb rawCodec) Unmarshal(data []byte, v interface{}) error {
	ba, ok := v.(*[]byte)
	dtmimp.PanicIf(!ok, fmt.Errorf("please pass in *[]byte"))
	*ba = append(*ba, data...)

	return nil
}

func (cb rawCodec) Name() string { return "dtm_raw" }

var normalClients, rawClients sync.Map

// ClientInterceptors declares grpc.UnaryClientInterceptors slice
var ClientInterceptors = []grpc.UnaryClientInterceptor{}

// MustGetDtmClient 1
func MustGetDtmClient(grpcServer string) dtmgpb.DtmClient {
	return dtmgpb.NewDtmClient(MustGetGrpcConn(grpcServer, false))
}

// GetGrpcConn 1
// 去链接 grpc server
func GetGrpcConn(grpcServer string, isRaw bool) (conn *grpc.ClientConn, rerr error) {
	clients := &normalClients
	if isRaw {
		clients = &rawClients
	}
	grpcServer = dtmimp.MayReplaceLocalhost(grpcServer)
	v, ok := clients.Load(grpcServer)
	if !ok {
		opts := grpc.WithDefaultCallOptions()
		if isRaw {
			opts = grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{}))
		}
		logger.Debugf("grpc client connecting %s", grpcServer)
		interceptors := append(ClientInterceptors, GrpcClientLog)
		inOpt := grpc.WithChainUnaryInterceptor(interceptors...)
		conn, rerr := grpc.Dial(grpcServer, inOpt, grpc.WithInsecure(), opts)
		if rerr == nil {
			clients.Store(grpcServer, conn)
			v = conn
			logger.Debugf("grpc client inited for %s", grpcServer)
		}
	}
	return v.(*grpc.ClientConn), rerr
}

// MustGetGrpcConn 1
// 获取链接 grpc server 的 conn, 在 GetGrpcConn 中调用 grpc.Dial, 然后用 sync.Map 保存起来
func MustGetGrpcConn(grpcServer string, isRaw bool) *grpc.ClientConn {
	conn, err := GetGrpcConn(grpcServer, isRaw)
	dtmimp.E2P(err)
	return conn
}
