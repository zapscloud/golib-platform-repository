package zapsdb_repository

import (
	"log"

	"github.com/zapscloud/golib-dbutils/zapsdb_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
)

// SettingZapsDBDao - User DAO Repository
type SettingZapsDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (t *SettingZapsDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize Zaps DAO")
	connection, _ := zapsdb_utils.GetConnection(t.client)

	collection, err := connection.GetCollection(platform_common.DbPlatformSettings)
	if err != nil {
		log.Println("Setting Table not found, createTable in ZapsDB")
		collection, err = connection.CreateCollection(platform_common.DbPlatformSettings, platform_common.FLD_SETTING_ID, "Application User Profile")
		if err != nil {
			log.Println("Failed to create Setting Table in ZapsDB")
		}
	}
	log.Println("Setting Collection", collection)

}

// List - List all Collections
func (t *SettingZapsDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformSettings)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	dataresponse, err := connection.GetMany(platform_common.DbPlatformSettings, filter, sort, skip, limit)
	if err != nil {
		log.Println("Error:", err)
		return nil, err
	}

	log.Println("End - Find All Collection Dao", dataresponse)

	return dataresponse, nil
}

// GetDetails - Find by code
func (t *SettingZapsDBDao) Get(settingid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SettingZapsDBDao::Find:: Begin ", settingid)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.GetOne(platform_common.DbPlatformSettings, settingid, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("SettingZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (t *SettingZapsDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SettingZapsDBDao::Find:: Begin ", filter)

	connection, _ := zapsdb_utils.GetConnection(t.client)
	singleResult, err := connection.FindOne(platform_common.DbPlatformSettings, filter, "")
	if err != nil {
		log.Println("Find:: Record not found ", err)
		return result, err
	}
	result = singleResult
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Printf("SettingZapsDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Insert - Insert Collection
func (t *SettingZapsDBDao) Create(indata utils.Map) (string, error) {

	log.Println("Business User Save - Begin", indata)
	connection, txnid := zapsdb_utils.GetConnection(t.client)

	insertResult, err := connection.Insert(platform_common.DbPlatformSettings, indata, txnid)
	if err != nil {
		log.Println("Error in insert ", err)
		return "", err
	}

	log.Println("Inserted a single document: ", insertResult[platform_common.FLD_SETTING_ID])
	log.Println("Save - End", indata[platform_common.FLD_SETTING_ID])

	return insertResult[platform_common.FLD_SETTING_ID].(string), err
}

// Update - Update Collection
func (t *SettingZapsDBDao) Update(settingid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	log.Printf("Update - Values %v", indata)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	updateResult, err := connection.UpdateOne(platform_common.DbPlatformSettings, settingid, indata, txnid)
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult)

	log.Println("Update - End")
	return indata, nil
}

// Delete - Delete Collection
func (t *SettingZapsDBDao) Delete(settingid string) (int64, error) {

	log.Println("SettingZapsDBDao::Delete - Begin ", settingid)

	connection, txnid := zapsdb_utils.GetConnection(t.client)
	res, err := connection.DeleteOne(platform_common.DbPlatformSettings, settingid, txnid)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("SettingZapsDBDao::Delete - End deleted %v documents\n", res)
	return 1, nil
}
