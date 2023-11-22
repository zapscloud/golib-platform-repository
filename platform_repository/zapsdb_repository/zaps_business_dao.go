package zapsdb_repository

import (
	"encoding/json"
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/zapsdb_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
)

// BusinessZapsDBDao - User DAO Repository
type BusinessZapsDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (t BusinessZapsDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize Zaps DAO")
	connection, _ := zapsdb_utils.GetConnection(t.client)

	collection, err := connection.GetCollection(platform_common.DbPlatformBusinesses)
	if err != nil {
		log.Println("SysBusiness Table not found, createTable in ZapsDB")
		collection, err = connection.CreateCollection(platform_common.DbPlatformBusinesses, platform_common.FLD_BUSINESS_ID, "Application Business")
		if err != nil {
			log.Println("Failed to create SysBusiness Table in ZapsDB")
		}
	}
	log.Println("SysBusiness Collection", collection)

	collection, err = connection.GetCollection(platform_common.DbPlatformBusinessUser)
	if err != nil {
		log.Println("SysBusiness Table not found, createTable in ZapsDB")
		collection, err = connection.CreateCollection(platform_common.DbPlatformBusinessUser, platform_common.FLD_BUSINESS_USER_ID, "Application Business Users")
		if err != nil {
			log.Println("Failed to create SysBusinessUsers Table in ZapsDB")
		}
	}
	log.Println("SysBusinessUser Collection", collection)

}

// List - List all Collections
func (t BusinessZapsDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformBusinesses)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	response, err := connection.GetMany(platform_common.DbPlatformBusinesses, filter, sort, skip, limit)
	if err != nil {
		log.Println("Error:", err)
		return nil, err
	}
	return response, nil
}

// GetDetails - Find by code
func (t BusinessZapsDBDao) Get(businessid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("BusinessZapsDBDao::Find:: Begin ", businessid)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.GetOne(platform_common.DbPlatformBusinesses, businessid, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("BusinessZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t BusinessZapsDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("BusinessZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformBusinesses, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("BusinessZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Insert - Insert Collection
func (t BusinessZapsDBDao) Create(indata utils.Map) (utils.Map, error) {

	log.Println("Business User Save - Begin", indata)
	connection, txnid := zapsdb_utils.GetConnection(t.client)

	insertResult, err := connection.Insert(platform_common.DbPlatformBusinesses, indata, txnid)
	if err != nil {
		log.Println("Error in insert ", err)
		return indata, err
	}

	log.Println("Inserted a single document: ", insertResult[platform_common.FLD_BUSINESS_ID])
	log.Println("Save - End", indata[platform_common.FLD_BUSINESS_ID])

	return indata, err
}

// Update - Update Collection
func (t BusinessZapsDBDao) Update(businessid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	log.Printf("Update - Values %v", indata)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	updateResult, err := connection.UpdateOne(platform_common.DbPlatformBusinesses, businessid, indata, txnid)
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult)

	log.Println("Update - End")
	return indata, nil
}

// Delete - Delete Collection
func (t BusinessZapsDBDao) Delete(businessid string) (int64, error) {

	log.Println("BusinessZapsDBDao::Delete - Begin ", businessid)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	res, err := connection.DeleteOne(platform_common.DbPlatformBusinesses, businessid, txnid)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("BusinessZapsDBDao::Delete - End deleted %v documents\n", res)
	return 1, nil
}

// AddUser - Grand permission for the user to access the business
func (t BusinessZapsDBDao) AddUser(indata utils.Map) (utils.Map, error) {

	log.Println("Business User Save - Begin", indata)
	connection, txnid := zapsdb_utils.GetConnection(t.client)

	indata[db_common.FLD_IS_DELETED] = false
	insertResult, err := connection.Insert(platform_common.DbPlatformBusinessUser, indata, txnid)
	if err != nil {
		log.Println("Error in insert ", err)
		return indata, err

	}

	log.Println("Inserted a single document: ", insertResult)
	log.Println("Save - End", indata[platform_common.FLD_BUSINESS_USER_ID])

	return indata, err
}

// Update Business User
func (t *BusinessZapsDBDao) UpdateUser(accessid string, indata utils.Map) (utils.Map, error) {
	log.Println("Business User Save - Begin", indata)
	connection, txnid := zapsdb_utils.GetConnection(t.client)

	// Add Fields for Create
	indata = db_common.AmendFldsforUpdate(indata)

	updateResult, err := connection.UpdateOne(platform_common.DbPlatformBusinessUser, accessid, indata, txnid)
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult)

	log.Println("Update - End")
	return indata, nil

}

// RemoveUser - Remove permission for the user to access the business
func (t BusinessZapsDBDao) RemoveUser(accessid string) (string, error) {

	log.Println("Business User Save - Begin", accessid)
	connection, txnid := zapsdb_utils.GetConnection(t.client)
	res, err := connection.DeleteOne(platform_common.DbPlatformBusinessUser, accessid, txnid)
	if err != nil {
		log.Println("Error in delete ", err)
		return accessid, err
	}
	log.Printf("BusinessZapsDBDao::Delete - End deleted %v documents\n", res)
	return accessid, nil
}

// GetDetails - Find by code
func (t BusinessZapsDBDao) GetAccessDetails(accessid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("BusinessZapsDBDao::GetAccessDetails:: Begin ", accessid)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.GetOne(platform_common.DbPlatformBusinessUser, accessid, "")
	if err != nil {
		log.Println("GetAccessDetails:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("BusinessZapsDBDao::GetAccessDetails:: End Found a single document: %+v\n", result)
	return result, nil
}

// List - List all Collections
func (t BusinessZapsDBDao) UserList(businessid string, filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformBusinessUser)

	filterdoc := utils.Map{}
	if len(filter) > 0 {
		err := json.Unmarshal([]byte(filter), &filterdoc)
		if err != nil {
			log.Println("Unmarshal Ext JSON error", err)
			log.Println(filterdoc)
		}
	}

	filterdoc[platform_common.FLD_BUSINESS_ID] = businessid
	jsonString, err := json.Marshal(filterdoc)
	if err != nil {
		log.Println("Unmarshal Ext JSON error", err)
		log.Println(filterdoc)
	} else {
		filter = string(jsonString)
	}

	log.Println("Filter Value", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	response, err := connection.GetMany(platform_common.DbPlatformBusinessUser, filter, sort, skip, limit)
	if err != nil {
		log.Println("Error:", err)
		return nil, err
	}
	return response, nil
}

// List - List all Business assigned with userId
func (t BusinessZapsDBDao) BusinessList(userId string, filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformBusinessUser)

	filterdoc := utils.Map{}
	if len(filter) > 0 {
		err := json.Unmarshal([]byte(filter), &filterdoc)
		if err != nil {
			log.Println("Unmarshal Ext JSON error", err)
			log.Println(filterdoc)
		}
	}

	filterdoc[platform_common.FLD_APP_USER_ID] = userId
	jsonString, err := json.Marshal(filterdoc)
	if err != nil {
		log.Println("Unmarshal Ext JSON error", err)
		log.Println(filterdoc)
	} else {
		filter = string(jsonString)
	}

	log.Println("Filter Value", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	response, err := connection.GetMany(platform_common.DbPlatformBusinessUser, filter, sort, skip, limit)
	if err != nil {
		log.Println("Error:", err)
		return nil, err
	}
	return response, nil
}
