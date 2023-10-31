package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// AppRoleDao - User DAO Repository
type AppRoleDao interface {
	InitializeDao(client utils.Map)
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)
	// Update - Update Collection
	Update(roleid string, indata utils.Map) (utils.Map, error)
	// Insert - Insert Collection
	Create(indata utils.Map) (utils.Map, error)
	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Delete - Delete Collection
	Delete(roleid string) (int64, error)

	Get(roleid string) (utils.Map, error)

	// Credentials
	AddCredentials(roleID string, indata utils.Map) (utils.Map, error)
	FindCredential(filter string) (utils.Map, error)
	GetCredentials(role_id string) (utils.Map, error)

	// Users
	AddUsers(role_id string, indata utils.Map) (utils.Map, error)
	FindUser(filter string) (utils.Map, error)
	GetUsers(role_id string) (utils.Map, error)

	BusinessList(roleid string, filter string, sort string, skip int64, limit int64) (utils.Map, error)
}

// NewAppRoleDao - Contruct Role Dao
func NewAppRoleDao(client utils.Map) AppRoleDao {
	var daoAppRole AppRoleDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoAppRole = &mongodb_repository.AppRoleMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		// *Not Implemented yet*
		daoAppRole = nil
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		daoAppRole = nil
	}

	if daoAppRole != nil {
		// Initialize the Dao
		daoAppRole.InitializeDao(client)
	}

	return daoAppRole
}
