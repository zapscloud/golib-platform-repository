package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-platform-repository/platform_repository/zapsdb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// BusinessDao - User DAO Repository
type BusinessDao interface {
	// InitializeDao
	InitializeDao(client utils.Map)

	// Business Details
	Get(businessid string) (utils.Map, error)

	// List Businesses
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)

	// Update - Update Collection
	Update(businessid string, indata utils.Map) (utils.Map, error)

	// Insert - Insert Collection
	Create(indata utils.Map) (utils.Map, error)

	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Delete - Delete Collection
	Delete(businessid string) (int64, error)

	// AddUser Business User
	AddUser(indata utils.Map) (utils.Map, error)

	// RevokePermission with Business User
	RemoveUser(accessid string) (string, error)

	//Get Permission details
	GetAccessDetails(accessid string) (utils.Map, error)

	// List Users in the business
	UserList(businessid string, filter string, sort string, skip int64, limit int64) (utils.Map, error)

	// List Business in the user
	BusinessList(userId string, filter string, sort string, skip int64, limit int64) (utils.Map, error)
}

// NewBusinessDao - Contruct User Dao
func NewBusinessDao(client utils.Map) BusinessDao {
	var daoAppBusiness BusinessDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoAppBusiness = &mongodb_repository.BusinessMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		daoAppBusiness = &zapsdb_repository.BusinessZapsDBDao{}
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		//daoAppBusiness = app_db_repository.SqlAppBusinessDBDao{}
		daoAppBusiness = nil
	}

	if daoAppBusiness != nil {
		// Initialize the Dao
		daoAppBusiness.InitializeDao(client)
	}

	return daoAppBusiness
}
