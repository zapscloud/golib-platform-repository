package zapsdb_repository

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/zapsdb_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
)

// RegionZapsDBDao - User DAO Repository
type RegionZapsDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (t *RegionZapsDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize Zaps DAO")
	connection, _ := zapsdb_utils.GetConnection(t.client)

	collection, err := connection.GetCollection(platform_common.DbPlatformRegions)
	if err != nil {
		log.Println("Region Table not found, createTable in ZapsDB")
		collection, err = connection.CreateCollection(platform_common.DbPlatformRegions, platform_common.FLD_REGION_ID, "Application Regions")
		if err != nil {
			log.Println("Failed to create Region Table in ZapsDB")
		}
	}
	log.Println("Region Collection", collection)

}

// List - List all Collections
func (t *RegionZapsDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformRegions)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	dataresponse, err := connection.GetMany(platform_common.DbPlatformRegions, filter, sort, skip, limit)
	if err != nil {
		log.Println("Error:", err)
		return nil, err
	}

	dataval, dataok := dataresponse[db_common.LIST_RESULT]
	if dataok {
		for _, value := range dataval.([]interface{}) {
			mapvalue := value.((map[string]interface{}))
			// Delete Password
			delete(mapvalue, platform_common.FLD_SYS_USER_PASSWORD)

		}
	}

	log.Println("End - Find All Collection Dao", dataresponse)

	return dataresponse, nil
}

// GetDetails - Find by code
func (t *RegionZapsDBDao) Get(regionid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("RegionZapsDBDao::Find:: Begin ", regionid)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.GetOne(platform_common.DbPlatformRegions, regionid, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Delete Password
	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("RegionZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t *RegionZapsDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("RegionZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformRegions, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Delete Password
	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("RegionZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t *RegionZapsDBDao) Authenticate(email string, password string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	filter := `{platform_common.FLD_SYS_USER_EMAIL:"` + email + `",platform_common.FLD_SYS_USER_PASSWORD:"` + password + `"}`
	log.Println("RegionZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformRegions, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Delete Password
	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("RegionZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Insert - Insert Collection
func (t *RegionZapsDBDao) Create(indata utils.Map) (utils.Map, error) {

	log.Println("Business User Save - Begin", indata)
	connection, txnid := zapsdb_utils.GetConnection(t.client)

	indata[db_common.FLD_IS_DELETED] = false

	insertResult, err := connection.Insert(platform_common.DbPlatformRegions, indata, txnid)
	if err != nil {
		log.Println("Error in insert ", err)
		return indata, err
	}

	log.Println("Inserted a single document: ", insertResult[platform_common.FLD_REGION_ID])
	log.Println("Save - End", indata[platform_common.FLD_REGION_ID])

	return indata, err
}

// Update - Update Collection
func (t *RegionZapsDBDao) Update(regionid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	log.Printf("Update - Values %v", indata)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	updateResult, err := connection.UpdateOne(platform_common.DbPlatformRegions, regionid, indata, txnid)
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult)

	log.Println("Update - End")
	return indata, err
}

// Delete - Delete Collection
func (t *RegionZapsDBDao) Delete(regionid string) (int64, error) {

	log.Println("RegionZapsDBDao::Delete - Begin ", regionid)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	res, err := connection.DeleteOne(platform_common.DbPlatformRegions, regionid, txnid)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("RegionZapsDBDao::Delete - End deleted %v documents\n", res)
	return 1, nil
}
