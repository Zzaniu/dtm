package sql

import (
	"fmt"
	"math"
	"time"

	"github.com/lithammer/shortuuid/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/dtm-labs/dtm2/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm2/dtmsvr/config"
	"github.com/dtm-labs/dtm2/dtmsvr/storage"
	"github.com/dtm-labs/dtm2/dtmutil"
)

var conf = &config.Config

// Store implements storage.Store, and storage with db
type Store struct {
}

// Ping execs ping cmd to db
func (s *Store) Ping() error {
	db, err := dtmimp.StandaloneDB(conf.Store.GetDBConf())
	dtmimp.E2P(err)
	_, err = db.Exec("select 1")
	return err
}

// PopulateData populates data to db
func (s *Store) PopulateData(skipDrop bool) {
	file := fmt.Sprintf("%s/dtmsvr.storage.%s.sql", dtmutil.GetSQLDir(), conf.Store.Driver)
	dtmutil.RunSQLScript(conf.Store.GetDBConf(), file, skipDrop)
}

// FindTransGlobalStore finds GlobalTrans data by gid
// 通过全局事务id获取 trans_global 中的记录并返回
func (s *Store) FindTransGlobalStore(gid string) *storage.TransGlobalStore {
	trans := &storage.TransGlobalStore{}
	// gid 是唯一索引，肯定只有一条记录
	dbr := dbGet().Model(trans).Where("gid=?", gid).First(trans)
	// 如果没有找到记录的话返回 nil
	if dbr.Error == gorm.ErrRecordNotFound {
		return nil
	}
	// 如果有错误，且错误不是未找到记录的话，直接 panic
	dtmimp.E2P(dbr.Error)
	// 返回
	return trans
}

// ScanTransGlobalStores lists GlobalTrans data.
// 查找全局事务信息, 也就是 trans_global 表中的记录
func (s *Store) ScanTransGlobalStores(position *string, limit int64) []storage.TransGlobalStore {
	globals := []storage.TransGlobalStore{}
	lid := math.MaxInt64
	if *position != "" {
		lid = dtmimp.MustAtoi(*position)
	}
	// 根据 id 来定位, 避免了随着 offset 增长性能下降的问题
	dbr := dbGet().Must().Where("id < ?", lid).Order("id desc").Limit(int(limit)).Find(&globals)
	// 说明数据已不足, 无下一页
	if dbr.RowsAffected < limit {
		*position = ""
	} else {
		// 返回下一页的 position
		*position = fmt.Sprintf("%d", globals[len(globals)-1].ID)
	}
	return globals
}

// FindBranches finds Branch data by gid
// 根据全局分布式事务id寻找该事务所有的分支，顺序是 事务1补偿->事务1正向->事务2补偿->事务2正向...
func (s *Store) FindBranches(gid string) []storage.TransBranchStore {
	branches := []storage.TransBranchStore{}
	dbGet().Must().Where("gid=?", gid).Order("id asc").Find(&branches)
	return branches
}

// UpdateBranches update branches info
func (s *Store) UpdateBranches(branches []storage.TransBranchStore, updates []string) (int, error) {
	// 如果唯一索引冲突的话，更新 updates 中的字段
	db := dbGet().Clauses(clause.OnConflict{
		OnConstraint: "trans_branch_op_pkey",
		DoUpdates:    clause.AssignmentColumns(updates),
	}).Create(branches)
	// 应该是返回更新了多少条数据
	return int(db.RowsAffected), db.Error
}

// LockGlobalSaveBranches creates branches
// 插入或者更新事务分支信息, 也就是往 trans_branch_op 表中插入记录或者更新记录
func (s *Store) LockGlobalSaveBranches(gid string, status string, branches []storage.TransBranchStore, branchStart int) {
	err := dbGet().Transaction(func(tx *gorm.DB) error {
		g := &storage.TransGlobalStore{}
		// 为了防止并发操作出现重复, 这里查找加了个写锁
		// 这里就是加锁, select ... for update 的意思
		dbr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Model(g).Where("gid=? and status=?", gid, status).First(g)
		// 如果根据全局事务 id 和传入的状态找到了, 就保存事务分支, 否则返回错误
		if dbr.Error == nil {
			dbr = tx.Save(branches)
		}
		return wrapError(dbr.Error)
	})
	// 事务失败直接 panic
	dtmimp.E2P(err)
}

// MaySaveNewTrans creates a new trans.
// saga:
// 往 trans_global 中插入全局分布式事务信息.
// 往 trans_branch_op 中插入该全局分布式事务分支正向反向操作信息
// tcc 这里在 try 的时候是不保存分支的, 但会保存全局分布式事务信息
func (s *Store) MaySaveNewTrans(global *storage.TransGlobalStore, branches []storage.TransBranchStore) error {
	return dbGet().Transaction(func(db1 *gorm.DB) error {
		db := &dtmutil.DB{DB: db1}
		// 如果冲突的话，什么也不做(冲突的话，说明已经存储过了)
		// gid 是唯一索引
		dbr := db.Must().Clauses(clause.OnConflict{
			DoNothing: true,
		}).Create(global)
		if dbr.RowsAffected <= 0 { // 如果这个不是新事务，返回错误
			return storage.ErrUniqueConflict
		}
		// tcc 这里在 try 的时候是不保存分支的
		if len(branches) > 0 {
			// 如果冲突的话，什么也不做
			// gid,branch_id,op 是联合的唯一索引
			db.Must().Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(&branches)
		}
		return nil
	})
}

// ChangeGlobalStatus changes global trans status.
// 修改全局状态, 也就是修改 trans_global 表的状态
// saga 只修改 status 和 update_time 字段
// 修改失败直接 panic
func (s *Store) ChangeGlobalStatus(global *storage.TransGlobalStore, newStatus string, updates []string, finished bool) {
	// old 数据库中记录的当前状态
	old := global.Status
	global.Status = newStatus
	// saga 只修改 status 和 update_time 字段
	// 其实应该还有 finish_time 或者 rollback_time 字段
	dbr := dbGet().Must().Model(global).Where("status=? and gid=?", old, global.Gid).Select(updates).Updates(global)
	if dbr.RowsAffected == 0 {
		dtmimp.E2P(storage.ErrNotFound)
	}
}

// TouchCronTime updates cronTime
// 更新下次执行时间
func (s *Store) TouchCronTime(global *storage.TransGlobalStore, nextCronInterval int64) {
	global.NextCronTime = dtmutil.GetNextTime(nextCronInterval)
	global.UpdateTime = dtmutil.GetNextTime(0)
	global.NextCronInterval = nextCronInterval
	dbGet().Must().Model(global).Where("status=? and gid=?", global.Status, global.Gid).
		Select([]string{"next_cron_time", "update_time", "next_cron_interval"}).Updates(global)
}

// LockOneGlobalTrans finds GlobalTrans
func (s *Store) LockOneGlobalTrans(expireIn time.Duration) *storage.TransGlobalStore {
	db := dbGet()
	getTime := func(second int) string {
		return map[string]string{
			"mysql":    fmt.Sprintf("date_add(now(), interval %d second)", second),
			"postgres": fmt.Sprintf("current_timestamp + interval '%d second'", second),
		}[conf.Store.Driver]
	}
	expire := int(expireIn / time.Second)
	whereTime := fmt.Sprintf("next_cron_time < %s", getTime(expire))
	// fmt.Println("whereTime = ", whereTime)
	owner := shortuuid.New()
	global := &storage.TransGlobalStore{}
	// 更新 next_cron_time < now() + expireIn 且状态为 'prepared', 'aborting', 'submitted' 的一条数据
	// 更新 owner 为新的 uuid, NextCronTime 为 now() + conf.RetryInterval  默认为 10 s
	dbr := db.Must().Model(global).
		Where(whereTime + "and status in ('prepared', 'aborting', 'submitted')").
		Limit(1).
		Select([]string{"owner", "next_cron_time"}).
		Updates(&storage.TransGlobalStore{
			Owner:        owner,
			NextCronTime: dtmutil.GetNextTime(conf.RetryInterval),
		})
	if dbr.RowsAffected == 0 {
		return nil
	}
	db.Must().Where("owner=?", owner).First(global)
	return global
}

// SetDBConn sets db conn pool
func SetDBConn(db *gorm.DB) {
	sqldb, _ := db.DB()
	sqldb.SetMaxOpenConns(int(conf.Store.MaxOpenConns))
	sqldb.SetMaxIdleConns(int(conf.Store.MaxIdleConns))
	sqldb.SetConnMaxLifetime(time.Duration(conf.Store.ConnMaxLifeTime) * time.Minute)
}

func dbGet() *dtmutil.DB {
	return dtmutil.DbGet(conf.Store.GetDBConf(), SetDBConn)
}

func wrapError(err error) error {
	if err == gorm.ErrRecordNotFound {
		return storage.ErrNotFound
	}
	dtmimp.E2P(err)
	return err
}
