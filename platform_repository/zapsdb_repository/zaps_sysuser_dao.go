package zapsdb_repository

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/zapsdb_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
)

// SysUserZapsDBDao - User DAO Repository
type SysUserZapsDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (t *SysUserZapsDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize Zaps DAO")
	connection, _ := zapsdb_utils.GetConnection(t.client)

	collection, err := connection.GetCollection(platform_common.DbPlatformSysUsers)
	if err != nil {
		log.Println("SysUser Table not found, createTable in ZapsDB")
		collection, err = connection.CreateCollection(platform_common.DbPlatformSysUsers, platform_common.FLD_SYS_USER_ID, "Syslication User Profile")
		if err != nil {
			log.Println("Failed to create SysUser Table in ZapsDB")
		}
	}
	log.Println("SysUser Collection", collection)

}

// List - List all Collections
func (t *SysUserZapsDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformSysUsers)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	dataresponse, err := connection.GetMany(platform_common.DbPlatformSysUsers, filter, sort, skip, limit)
	if err != nil {
		log.Println("Error:", err)
		return nil, err
	}

	dataval, dataok := dataresponse[db_common.LIST_RESULT]
	if dataok {
		for _, value := range dataval.([]interface{}) {
			mapvalue := value.((map[string]interface{}))

			delete(mapvalue, platform_common.FLD_SYS_USER_PASSWORD)

		}
	}

	log.Println("End - Find All Collection Dao", dataresponse)

	return dataresponse, nil
}

// GetDetails - Find by code
func (t *SysUserZapsDBDao) Get(userid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SysUserZapsDBDao::Find:: Begin ", userid)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.GetOne(platform_common.DbPlatformSysUsers, userid, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("SysUserZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t *SysUserZapsDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SysUserZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformSysUsers, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("SysUserZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t *SysUserZapsDBDao) Authenticate(auth_key string, auth_login string, auth_pwd string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	filter := `{platform_common.FLD_SYS_USER_EMAIL:"` + auth_login + `",platform_common.FLD_SYS_USER_PASSWORD:"` + auth_pwd + `"}`
	log.Println("SysUserZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformSysUsers, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("SysUserZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Insert - Insert Collection
func (t *SysUserZapsDBDao) Create(indata utils.Map) (utils.Map, error) {

	log.Println("Business User Save - Begin", indata)
	connection, txnid := zapsdb_utils.GetConnection(t.client)

	indata[db_common.FLD_IS_DELETED] = false
	insertResult, err := connection.Insert(platform_common.DbPlatformSysUsers, indata, txnid)
	if err != nil {
		log.Println("Error in insert ", err)
		return indata, err
	}

	log.Println("Inserted a single document: ", insertResult[platform_common.FLD_SYS_USER_ID])
	log.Println("Save - End", indata[platform_common.FLD_SYS_USER_ID])

	return indata, err
}

// Update - Update Collection
func (t *SysUserZapsDBDao) Update(userid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	log.Printf("Update - Values %v", indata)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	updateResult, err := connection.UpdateOne(platform_common.DbPlatformSysUsers, userid, indata, txnid)
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult)

	log.Println("Update - End")
	return indata, nil
}

// Delete - Delete Collection
func (t *SysUserZapsDBDao) Delete(userid string) (int64, error) {

	log.Println("SysUserZapsDBDao::Delete - Begin ", userid)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	res, err := connection.DeleteOne(platform_common.DbPlatformSysUsers, userid, txnid)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("SysUserZapsDBDao::Delete - End deleted %v documents\n", res)
	return 1, nil
}
