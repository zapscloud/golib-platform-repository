package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// ClientsDao - User DAO Repository
type ClientsDao interface {
	InitializeDao(client utils.Map)
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)
	// Count(filter string, sort string, skip int64, limit int64) (int64, int64, error)
	// Update - Update Collection
	Update(clientid string, indata utils.Map) (utils.Map, error)
	// Find - Find by code
	Authenticate(clientId string, clientSecret string) (utils.Map, error)
	// Insert - Insert Collection
	Create(indata utils.Map) (string, error)
	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Delete - Delete Collection
	Delete(clientid string) (int64, error)

	Get(clientid string) (utils.Map, error)
}

// type appClientBaseDao struct {
// 	app_db_repository.AppClientDBDao
// 	instancename string
// }

// NewClientsDao - Contruct User Dao
func NewClientsDao(client utils.Map) ClientsDao {
	var daoAppClient ClientsDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoAppClient = &mongodb_repository.ClientsMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		// *Not Implemented yet*
		daoAppClient = nil
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
		daoAppClient = nil
	}

	if daoAppClient != nil {
		// Initialize the Dao
		daoAppClient.InitializeDao(client)
	}

	return daoAppClient
}
