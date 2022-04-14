/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package storage

import (
    "errors"
    "time"
)

// ErrNotFound defines the query item is not found in storage implement.
var ErrNotFound = errors.New("storage: NotFound")

// ErrUniqueConflict defines the item is conflict with unique key in storage implement.
var ErrUniqueConflict = errors.New("storage: UniqueKeyConflict")

// Store defines storage relevant interface
type Store interface {
    Ping() error
    PopulateData(skipDrop bool)
    FindTransGlobalStore(gid string) *TransGlobalStore
    ScanTransGlobalStores(position *string, limit int64) []TransGlobalStore
    FindBranches(gid string) []TransBranchStore
    // UpdateBranches 创建分支或更新分支状态，mysql这里是用的批处理  redis 的目前版本看是没有实现
    UpdateBranches(branches []TransBranchStore, updates []string) (int, error)
    LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore, branchStart int)
    MaySaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error
    ChangeGlobalStatus(global *TransGlobalStore, newStatus string, updates []string, finished bool)
    TouchCronTime(global *TransGlobalStore, nextCronInterval int64)
    LockOneGlobalTrans(expireIn time.Duration) *TransGlobalStore
}
