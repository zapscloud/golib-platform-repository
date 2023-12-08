package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// Payment_txnDao - Contact DAO Repository
type Payment_txnDao interface {
	// InitializeDao
	InitializeDao(client utils.Map)

	// List
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)

	// Get - Get Contact Details
	Get(payment_txn_id string) (utils.Map, error)

	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Create - Create Contact
	Create(indata utils.Map) (utils.Map, error)

	// Update - Update Collection
	Update(payment_txn_id string, indata utils.Map) (utils.Map, error)

	// Delete - Delete Collection
	Delete(payment_txn_id string) (int64, error)

	// DeleteAll - DeleteAll Collection
	DeleteAll() (int64, error)
}

// NewPayment_txnDao - Contruct Holiday Dao
func NewPayment_txnDao(client utils.Map) Payment_txnDao {
	var daoClient Payment_txnDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoClient = &mongodb_repository.Payment_txnMongoDBDao{}
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
