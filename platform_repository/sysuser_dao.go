package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-platform-repository/platform_repository/zapsdb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// SysUserDao - User DAO Repository
type SysUserDao interface {
	// InitializeDao
	InitializeDao(client utils.Map)
	// List
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)
	// Count(filter string, sort string, skip int64, limit int64) (int64, int64, error)
	// Update - Update Collection
	Update(userid string, indata utils.Map) (utils.Map, error)
	// Find - Find by code
	Authenticate(auth_key string, auth_login string, auth_pwd string) (utils.Map, error)
	// Insert - Insert Collection
	Create(indata utils.Map) (utils.Map, error)
	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Delete - Delete Collection
	Delete(userid string) (int64, error)

	Get(userid string) (utils.Map, error)
}

// NewSysUserDao - Contruct User Dao
func NewSysUserDao(client utils.Map) SysUserDao {
	var daoSysUser SysUserDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoSysUser = &mongodb_repository.SysUserMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		daoSysUser = &zapsdb_repository.SysUserZapsDBDao{}
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		daoSysUser = nil
	}

	if daoSysUser != nil {
		// Initialize the Dao
		daoSysUser.InitializeDao(client)
	}

	return daoSysUser
}
