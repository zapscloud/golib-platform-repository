package zapsdb_repository

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/zapsdb_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
)

// AppClientZapsDBDao - User DAO Repository
type AppClientZapsDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (t *AppClientZapsDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize Zaps DAO")
	connection, _ := zapsdb_utils.GetConnection(t.client)

	collection, err := connection.GetCollection(platform_common.DbPlatformClients)
	if err != nil {
		log.Println("SysClient Table not found, createTable in ZapsDB")
		collection, err = connection.CreateCollection(platform_common.DbPlatformClients, platform_common.FLD_CLIENT_ID, "Application User Profile")
		if err != nil {
			log.Println("Failed to create SysClient Table in ZapsDB")
		}
	}
	log.Println("SysClient Collection", collection)

}

// List - List all Collections
func (t *AppClientZapsDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformClients)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	dataresponse, err := connection.GetMany(platform_common.DbPlatformClients, filter, sort, skip, limit)
	if err != nil {
		log.Println("Error:", err)
		return nil, err
	}

	log.Println("End - Find All Collection Dao", dataresponse)

	return dataresponse, nil
}

// GetDetails - Find by code
func (t *AppClientZapsDBDao) Get(clientid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("AppClientZapsDBDao::Find:: Begin ", clientid)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.GetOne(platform_common.DbPlatformClients, clientid, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("AppClientZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t *AppClientZapsDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("AppClientZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformClients, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("AppClientZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t *AppClientZapsDBDao) Authenticate(clientid string, clientsecret string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	filter := `{platform_common.FLD_CLIENT_ID:"` + clientid + `",platform_common.FLD_CLIENT_SECRET:"` + clientsecret + `"}`
	log.Println("AppClientZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformClients, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("AppClientZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Insert - Insert Collection
func (t *AppClientZapsDBDao) Create(indata utils.Map) (string, error) {

	log.Println("Business User Save - Begin", indata)
	connection, txnid := zapsdb_utils.GetConnection(t.client)

	indata[db_common.FLD_IS_DELETED] = false
	insertResult, err := connection.Insert(platform_common.DbPlatformClients, indata, txnid)
	if err != nil {
		log.Println("Error in insert ", err)
		return "", err
	}

	log.Println("Inserted a single document: ", insertResult[platform_common.FLD_CLIENT_ID])
	log.Println("Save - End", indata[platform_common.FLD_CLIENT_ID])

	return insertResult[platform_common.FLD_CLIENT_ID].(string), err
}

// Update - Update Collection
func (t *AppClientZapsDBDao) Update(clientid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	log.Printf("Update - Values %v", indata)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	updateResult, err := connection.UpdateOne(platform_common.DbPlatformClients, clientid, indata, txnid)
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult)

	log.Println("Update - End")
	return indata, nil
}

// Delete - Delete Collection
func (t *AppClientZapsDBDao) Delete(clientid string) (int64, error) {

	log.Println("AppClientZapsDBDao::Delete - Begin ", clientid)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	res, err := connection.DeleteOne(platform_common.DbPlatformClients, clientid, txnid)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("AppClientZapsDBDao::Delete - End deleted %v documents\n", res)
	return 1, nil
}
