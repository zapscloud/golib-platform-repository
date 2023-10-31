package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// IndustryDao - Industry DAO Repository
type IndustryDao interface {
	InitializeDao(client utils.Map)
	//List - List all Collections
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)
	GetIndustryById(industryid string) (utils.Map, error)
}

// NewIndustryDao - Contruct Industry Dao
func NewIndustryDao(client utils.Map) IndustryDao {
	var daoIndustry IndustryDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoIndustry = &mongodb_repository.IndustryMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		// *Not Implemented yet*
		daoIndustry = nil
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		daoIndustry = nil
	}

	if daoIndustry != nil {
		// Initialize the Dao
		daoIndustry.InitializeDao(client)
	}

	return daoIndustry
}
