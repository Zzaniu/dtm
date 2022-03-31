/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package storage

import (
	"time"

	"github.com/dtm-labs/dtm2/dtmcli"
	"github.com/dtm-labs/dtm2/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm2/dtmsvr/config"
	"github.com/dtm-labs/dtm2/dtmutil"
)

// TransGlobalExt defines Header info
type TransGlobalExt struct {
	Headers map[string]string `json:"headers,omitempty" gorm:"-"`
}

// TransGlobalStore defines GlobalStore storage info
type TransGlobalStore struct {
	dtmutil.ModelBase
	Gid              string              `json:"gid,omitempty"`                // 分布式全局事务 id
	TransType        string              `json:"trans_type,omitempty"`         // 事务类型, 如 saga tcc xa msg
	Steps            []map[string]string `json:"steps,omitempty" gorm:"-"`     // TODO 这里面应该是存放的可在 TransFromContext 这里打印下看看
	Payloads         []string            `json:"payloads,omitempty" gorm:"-"`  // TODO 这里面应该是存放的可在 TransFromContext 这里打印下看看
	BinPayloads      [][]byte            `json:"-" gorm:"-"`                   // 就是上面那个 Payloads 的 byte 形式
	Status           string              `json:"status,omitempty"`             // 分布式事务的状态
	QueryPrepared    string              `json:"query_prepared,omitempty"`     // saga 为空
	Protocol         string              `json:"protocol,omitempty"`           // 协议, 是 http 还是 gRPC
	FinishTime       *time.Time          `json:"finish_time,omitempty"`        // 结束时间
	RollbackTime     *time.Time          `json:"rollback_time,omitempty"`      // 回滚时间(saga 一般为 null)
	Options          string              `json:"options,omitempty"`            // 一些 options 的信息, saga 的话一般是是否等待以及重试间隔
	CustomData       string              `json:"custom_data,omitempty"`        // TODO 不知, 可在 ProcessOnce 打印下看看
	NextCronInterval int64               `json:"next_cron_interval,omitempty"` // 下次执行的间隔
	NextCronTime     *time.Time          `json:"next_cron_time,omitempty"`     // 下次执行的时间
	Owner            string              `json:"owner,omitempty"`              // saga 为空
	Ext              TransGlobalExt      `json:"-" gorm:"-"`                   // TODO 这里面应该是存放的可在 TransFromContext 这里打印下看看
	// 上面那个的 string 形式
	ExtData string `json:"ext_data,omitempty"` // storage of ext. a db field to store many values. like Options
	dtmcli.TransOptions
}

// TableName TableName
func (g *TransGlobalStore) TableName() string {
	return config.Config.Store.TransGlobalTable
}

func (g *TransGlobalStore) String() string {
	return dtmimp.MustMarshalString(g)
}

// TransBranchStore branch transaction
type TransBranchStore struct {
	dtmutil.ModelBase
	Gid          string     `json:"gid,omitempty"`
	URL          string     `json:"url,omitempty"`
	BinData      []byte     // 这个是body传
	BranchID     string     `json:"branch_id,omitempty"`
	Op           string     `json:"op,omitempty"`
	Status       string     `json:"status,omitempty"`
	FinishTime   *time.Time `json:"finish_time,omitempty"`
	RollbackTime *time.Time `json:"rollback_time,omitempty"`
}

// TableName TableName
func (b *TransBranchStore) TableName() string {
	return config.Config.Store.TransBranchOpTable
}

func (b *TransBranchStore) String() string {
	return dtmimp.MustMarshalString(*b)
}
