package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-platform-repository/platform_repository/zapsdb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// RegionDao - User DAO Repository
type RegionDao interface {
	InitializeDao(client utils.Map)
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)
	// Count(filter string, sort string, skip int64, limit int64) (int64, int64, error)
	// Update - Update Collection
	Update(userid string, indata utils.Map) (utils.Map, error)
	// Find - Find by code
	Authenticate(email string, password string) (utils.Map, error)
	// Insert - Insert Collection
	Create(indata utils.Map) (utils.Map, error)
	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Delete - Delete Collection
	Delete(userid string) (int64, error)

	Get(userid string) (utils.Map, error)
}

// NewRegionDao
func NewRegionDao(client utils.Map) RegionDao {
	var daoRegion RegionDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoRegion = &mongodb_repository.RegionMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		daoRegion = &zapsdb_repository.RegionZapsDBDao{}
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		daoRegion = nil
	}

	if daoRegion != nil {
		// Initialize the Dao
		daoRegion.InitializeDao(client)
	}

	return daoRegion
}
