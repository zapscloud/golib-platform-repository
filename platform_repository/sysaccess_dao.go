package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// AccessDao - Access DAO Repository
type SysAccessDao interface {
	// InitializeDao
	InitializeDao(client utils.Map, businessid string)

	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)

	// Get - Get Access Details
	Get(accessid string) (utils.Map, error)

	// Find - Find by code
	Find(filter string) (utils.Map, error)

	GrantPermission(indata utils.Map) (utils.Map, error)

	// RevokePermission - RevokePermission Collection
	RevokePermission(accessid string) (int64, error)

	GetRoleDetails(roleid string) (utils.Map, error)
	GetSiteDetails(siteid string) (utils.Map, error)
	GetDepartmentDetails(departmentid string) (utils.Map, error)
}

// // accessBaseDao - Base DAO Structure
// type sysAccessBaseDao struct {
// 	sys_db_repository.SysAccessDBDao
// 	instancename string
// }

// NewaccessMongoDao - Contruct Access Dao
func NewSysAccessDao(client utils.Map, businessid string) SysAccessDao {
	var daoSysAccess SysAccessDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoSysAccess = &mongodb_repository.SysAccessMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		// *Not Implemented yet*
		daoSysAccess = nil
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		daoSysAccess = nil
	}

	if daoSysAccess != nil {
		daoSysAccess.InitializeDao(client, businessid)
	}

	return daoSysAccess
}
