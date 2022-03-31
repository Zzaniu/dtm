/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"context"
	"fmt"
	"time"

	"github.com/dtm-labs/dtm2/dtmcli"
	"github.com/dtm-labs/dtm2/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm2/dtmcli/logger"
	"github.com/dtm-labs/dtm2/dtmgrpc/dtmgimp"
	"github.com/dtm-labs/dtm2/dtmgrpc/dtmgpb"
	"github.com/dtm-labs/dtm2/dtmsvr/storage"
	"github.com/gin-gonic/gin"
)

// TransGlobal global transaction
type TransGlobal struct {
	storage.TransGlobalStore
	lastTouched      time.Time // record the start time of process
	updateBranchSync bool
}

// TransBranch branch transaction
type TransBranch = storage.TransBranchStore

type transProcessor interface {
	GenBranches() []TransBranch
	ProcessOnce(branches []TransBranch) error
}

type processorCreator func(*TransGlobal) transProcessor

var processorFac = map[string]processorCreator{}

func registorProcessorCreator(transType string, creator processorCreator) {
	processorFac[transType] = creator
}

func (t *TransGlobal) getProcessor() transProcessor {
	return processorFac[t.TransType](t)
}

type cronType int

const (
	cronBackoff cronType = iota
	cronReset
	cronKeep
)

// TransFromContext TransFromContext
// 从 gin.Context 中获取信息 TransGlobal 信息
func TransFromContext(c *gin.Context) *TransGlobal {
	b, err := c.GetRawData()
	e2p(err)
	m := TransGlobal{}
	// 解析后的 m 格式
	// dtmsvr.TransGlobal{TransGlobalStore:storage.TransGlobalStore{ModelBase:dtmutil.ModelBase{ID:0x0, CreateTime:(*time.Time)(nil), UpdateTime:(*time.Time)(nil)}, Gid:"FFQXcgVkBxrukWAFFZpraY", TransType:"saga", Steps:[]map[string]string{map[string]strin
	// g{"action":"http://localhost:9911/genorder", "compensate":"http://localhost:9911/delorder"}, map[string]string{"action":"http://localhost:9912/reduce", "compensate":"http://localhost:9912/add"}}, Payloads:[]string{"{\"amount\":1000,\"goodsCode\":\"ZLq8w4KxH4huoX
	// pmWVP6qa\",\"goodsName\":\"菊花\",\"orderSo\":\"U9xJ9oAAZ6fFmWDCJiVqNM\"}", "{\"amount\":1000,\"goodsCode\":\"ZLq8w4KxH4huoXpmWVP6qa\",\"goodsName\":\"菊花\"}"}, BinPayloads:[][]uint8(nil), Status:"", QueryPrepared:"", Protocol:"", FinishTime:(*time.Time)(nil),
	// RollbackTime:(*time.Time)(nil), Options:"", CustomData:"{\"concurrent\":true,\"orders\":{}}", NextCronInterval:0, NextCronTime:(*time.Time)(nil), Owner:"", Ext:storage.TransGlobalExt{Headers:map[string]string(nil)}, ExtData:"", TransOptions:dtmimp.TransOptions{W
	// aitResult:true, TimeoutToFail:0, RetryInterval:5, PassthroughHeaders:[]string(nil), BranchHeaders:map[string]string(nil)}}, lastTouched:time.Time{wall:0x0, ext:0, loc:(*time.Location)(nil)}, updateBranchSync:false}
	dtmimp.MustUnmarshal(b, &m)
	fmt.Printf("========= m = %#v\n", m)
	logger.Debugf("creating trans in prepare")
	// Payloads will be store in BinPayloads, Payloads is only used to Unmarshal
	for _, p := range m.Payloads {
		m.BinPayloads = append(m.BinPayloads, []byte(p))
	}
	// Steps = []map[string]string{map[string]string{"action":"http://localhost:9911/genorder", "compensate":"http://localhost:9911/delorder"}, map[string]string{"action":"http://localhost:9912/reduce", "compensate":"http://localhost:9912/add"}}
	fmt.Printf("m.Steps = %#v\n", m.Steps)
	for _, d := range m.Steps {
		if d["data"] != "" {
			m.BinPayloads = append(m.BinPayloads, []byte(d["data"]))
		}
	}
	m.Protocol = "http"

	// 应该是获取一些请求头信息吧，TODO 这里需要结合调用的地方看
	m.Ext.Headers = map[string]string{}
	if len(m.PassthroughHeaders) > 0 {
		for _, h := range m.PassthroughHeaders { // saga 为空
			v := c.GetHeader(h)
			if v != "" {
				m.Ext.Headers[h] = v
			}
		}
	}
	return &m
}

// TransFromDtmRequest TransFromContext.
// grpc 专用.
// 从 context.Context 和 dtmgpb.DtmRequest 中获取信息 TransGlobal 信息
func TransFromDtmRequest(ctx context.Context, c *dtmgpb.DtmRequest) *TransGlobal {
	o := &dtmgpb.DtmTransOptions{}
	if c.TransOptions != nil {
		o = c.TransOptions
	}
	r := TransGlobal{TransGlobalStore: storage.TransGlobalStore{
		Gid:           c.Gid,           // 全局事务 id
		TransType:     c.TransType,     // 事务类型, 如 saga tcc xa msg
		QueryPrepared: c.QueryPrepared, // msg 回查 url
		Protocol:      "grpc",          // 协议类型为 grpc/http, 这里是 grpc
		BinPayloads:   c.BinPayloads,   // 请求体 body 信息
		CustomData:    c.CustomedData,  // 是否并发, 依赖关系
		TransOptions: dtmcli.TransOptions{
			WaitResult:         o.WaitResult,    // 是否异步
			TimeoutToFail:      o.TimeoutToFail, // 多久算失败吧
			RetryInterval:      o.RetryInterval, // 重试间隔
			PassthroughHeaders: o.PassthroughHeaders,
			BranchHeaders:      o.BranchHeaders,
		},
	}}
	if c.Steps != "" {
		dtmimp.MustUnmarshalString(c.Steps, &r.Steps)
	}
	if len(o.PassthroughHeaders) > 0 {
		r.Ext.Headers = map[string]string{}
		for _, h := range o.PassthroughHeaders {
			// 获取 metadata 信息
			v := dtmgimp.GetMetaFromContext(ctx, h)
			if v != "" {
				r.Ext.Headers[h] = v
			}
		}
	}
	return &r
}
