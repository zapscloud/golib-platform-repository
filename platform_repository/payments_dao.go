package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// PaymentsDao - Contact DAO Repository
type PaymentsDao interface {
	// InitializeDao
	InitializeDao(client utils.Map)

	// List
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)

	// Get - Get Contact Details
	Get(PaymentId string) (utils.Map, error)

	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Create - Create Contact
	Create(indata utils.Map) (utils.Map, error)

	// Update - Update Collection
	Update(PaymentId string, indata utils.Map) (utils.Map, error)

	// Delete - Delete Collection
	Delete(PaymentId string) (int64, error)

	// DeleteAll - DeleteAll Collection
	DeleteAll() (int64, error)
}

// NewPaymentsDao - Contruct Holiday Dao
func NewPaymentsDao(client utils.Map) PaymentsDao {
	var daoClient PaymentsDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoClient = &mongodb_repository.PaymentsMongoDBDao{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		// *Not Implemented yet*
	case db_common.DATABASE_TYPE_MYSQLDB:
		// *Not Implemented yet*
	}

	if daoClient != nil {
		// Initialize the Dao
		daoClient.InitializeDao(client)
	}

	return daoClient
}
