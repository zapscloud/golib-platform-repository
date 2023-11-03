package platform_repository

import (
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-platform-repository/platform_repository/mongodb_repository"
	"github.com/zapscloud/golib-utils/utils"
)

// InvoiceDao - Contact DAO Repository
type InvoiceDao interface {
	// InitializeDao
	InitializeDao(client utils.Map)

	// List
	List(filter string, sort string, skip int64, limit int64) (utils.Map, error)

	// Get - Get Contact Details
	Get(clientId string) (utils.Map, error)

	// Find - Find by code
	Find(filter string) (utils.Map, error)

	// Create - Create Contact
	Create(indata utils.Map) (utils.Map, error)

	// Update - Update Collection
	Update(clientId string, indata utils.Map) (utils.Map, error)

	// Delete - Delete Collection
	Delete(clientId string) (int64, error)

	// DeleteAll - DeleteAll Collection
	DeleteAll() (int64, error)
}

// NewInvoiceDao - Contruct Holiday Dao
func NewInvoiceDao(client utils.Map) InvoiceDao {
	var daoClient InvoiceDao = nil

	// Get DatabaseType and no need to validate error
	// since the dbType was assigned with correct value after dbService was created
	dbType, _ := db_common.GetDatabaseType(client)

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		daoClient = &mongodb_repository.InvoiceMongoDBDao{}
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
