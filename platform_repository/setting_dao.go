package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-platform-repository/platform_repository/zapsdb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// SysSettingDao - User DAO Repository
type SysSettingDao interface {
	// InitializeDao
	InitializeDao(client utils.Map)
	// List
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)
	// Count(filter string, sort string, skip int64, limit int64) (int64, int64, error)
	// Update - Update Collection
	Update(clientid string, indata utils.Map) (utils.Map, error)
	// Insert - Insert Collection
	Create(indata utils.Map) (string, error)
	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Delete - Delete Collection
	Delete(clientid string) (int64, error)

	Get(clientid string) (utils.Map, error)
}

// type sysSettingBaseDao struct {
// 	sys_db_repository.SysSettingDBDao
// 	instancename string
// }

// NewSysSettingDao - Contruct User Dao
func NewSysSettingDao(client utils.Map) SysSettingDao {
	var daoSysSetting SysSettingDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoSysSetting = &mongodb_repository.SettingMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		daoSysSetting = &zapsdb_repository.SettingZapsDBDao{}
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		daoSysSetting = nil
	}

	if daoSysSetting != nil {
		// Initialize the Dao
		daoSysSetting.InitializeDao(client)
	}

	return daoSysSetting
}
