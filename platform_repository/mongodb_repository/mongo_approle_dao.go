package mongodb_repository

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/rs/xid"
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/mongo_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// AppRoleMongoDBDao - AppRole DAO Repository
type AppRoleMongoDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (p *AppRoleMongoDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize AppRoleMongoDBDao")
	p.client = client
}

// List - List all Collections
func (t *AppRoleMongoDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map

	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformAppRoles)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoles)
	if err != nil {
		return nil, err
	}

	log.Println("Get Collection - Find All Collection Dao", filter, len(filter), sort, len(sort))

	opts := options.Find()
	filterdoc := bson.D{}
	if len(filter) > 0 {
		// filters, _ := strconv.Unquote(string(filter))
		err = bson.UnmarshalExtJSON([]byte(filter), true, &filterdoc)
		if err != nil {
			log.Println("Unmarshal Ext JSON error", err)
			log.Println(filterdoc)
		}
	}

	if len(sort) > 0 {
		var sortdoc interface{}
		err = bson.UnmarshalExtJSON([]byte(sort), true, &sortdoc)
		if err != nil {
			log.Println("Sort Unmarshal Error ", sort)
		} else {
			opts.SetSort(sortdoc)
		}
	}

	if skip > 0 {
		log.Println(filterdoc)
		opts.SetSkip(skip)
	}

	if limit > 0 {
		log.Println(filterdoc)
		opts.SetLimit(limit)
	}

	filterdoc = append(filterdoc, bson.E{Key: db_common.FLD_IS_DELETED, Value: false})
	log.Println("Parameter values ", filterdoc, opts)
	cursor, err := collection.Find(ctx, filterdoc, opts)
	if err != nil {
		return nil, err
	}

	// get a list of all returned documents and print them out
	// see the mongo.Cursor documentation for more examples of using cursors
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	listdata := []utils.Map{}
	for idx, value := range results {
		log.Println("Item ", idx)
		delete(value, db_common.FLD_DEFAULT_ID)
		listdata = append(listdata, value)
	}

	log.Println("End - Find All Collection Dao", listdata)

	log.Println("Parameter values ", filterdoc)
	filtercount, err := collection.CountDocuments(ctx, filterdoc)
	if err != nil {
		return utils.Map{}, err
	}

	totalcount, err := collection.CountDocuments(ctx, bson.D{{Key: db_common.FLD_IS_DELETED, Value: false}})
	if err != nil {
		return utils.Map{}, err
	}

	response := utils.Map{
		db_common.LIST_SUMMARY: utils.Map{
			db_common.LIST_TOTALSIZE:    totalcount,
			db_common.LIST_FILTEREDSIZE: filtercount,
			db_common.LIST_RESULTSIZE:   len(listdata),
		},
		db_common.LIST_RESULT: listdata,
	}

	return response, nil
}

func (t *AppRoleMongoDBDao) Get(roleid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("AppRoleMongoDBDao::GetDetails:: Begin ", roleid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoles)
	log.Println("Find:: Got Collection ")

	filter := bson.D{{Key: platform_common.FLD_APP_ROLE_ID, Value: roleid}, {}}

	filter = append(filter, bson.E{Key: db_common.FLD_IS_DELETED, Value: false})
	log.Println("GetDetails:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("GetDetails:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("AppRoleMongoDBDao::GetDetails:: End Found a single document: %+v\n", result)
	return result, nil
}

// Update - Update Collection
func (t *AppRoleMongoDBDao) Update(roleid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoles)
	if err != nil {
		return utils.Map{}, err
	}
	// Modify Fields for Update
	indata = db_common.AmendFldsforUpdate(indata)

	// Update a single document
	log.Printf("Update - Values %v", indata)

	filter := bson.D{{Key: platform_common.FLD_APP_ROLE_ID, Value: roleid}}

	updateResult, err := collection.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: indata}})
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult.ModifiedCount)

	log.Println("Update - End")
	return indata, nil
}

// Insert - Insert Collection
func (t *AppRoleMongoDBDao) Create(indata utils.Map) (utils.Map, error) {

	log.Println("User Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoles)
	if err != nil {
		return indata, err
	}

	// Add Fields for Create
	indata = db_common.AmendFldsforCreate(indata)

	insertResult, err := collection.InsertOne(ctx, indata)
	if err != nil {
		log.Println("Error in insert ", err)
		return indata, err

	}
	log.Println("Inserted a single document: ", insertResult.InsertedID)
	log.Println("Save - End", indata[platform_common.FLD_APP_ROLE_ID])

	return indata, nil
}

// Find - Find by code
func (t *AppRoleMongoDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("AppRole::Find:: Begin ", filter)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoles)
	log.Println("Find:: Got Collection ", err)

	bfilter := bson.D{}
	err = bson.UnmarshalExtJSON([]byte(filter), true, &bfilter)
	if err != nil {
		fmt.Println("Error on filter Unmarshal", err)
	}
	bfilter = append(bfilter, bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	log.Println("Find:: Got filter ", bfilter)
	singleResult := collection.FindOne(ctx, bfilter)
	if singleResult.Err() != nil {
		log.Println("Find:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("AppRole::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Delete - Delete Collection
func (t *AppRoleMongoDBDao) Delete(roleid string) (int64, error) {

	log.Println("AppRoleMongoDBDao::Delete - Begin ", roleid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoles)
	if err != nil {
		return 0, err
	}
	opts := options.Delete().SetCollation(&options.Collation{
		Locale:    db_common.LOCALE,
		Strength:  1,
		CaseLevel: false,
	})

	filter := bson.D{{Key: platform_common.FLD_APP_ROLE_ID, Value: roleid}}

	//filter = append(filter, bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: businessID})

	res, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("AppRoleMongoDBDao::Delete - End deleted %v documents\n", res.DeletedCount)
	return res.DeletedCount, nil
}

// List - List all Collections
func (t *AppRoleMongoDBDao) BusinessList(roleid string, filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map

	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformBusinessUser)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformBusinessUser)
	if err != nil {
		return nil, err
	}

	log.Println("Get Collection - Find All Collection Dao", filter, len(filter), sort, len(sort))

	opts := options.Find()
	filterdoc := bson.D{}
	if len(filter) > 0 {
		// filters, _ := strconv.Unquote(string(filter))
		err = bson.UnmarshalExtJSON([]byte(filter), true, &filterdoc)
		if err != nil {
			log.Println("Unmarshal Ext JSON error", err)
			log.Println(filterdoc)
		}
	}

	if len(sort) > 0 {
		var sortdoc interface{}
		err = bson.UnmarshalExtJSON([]byte(sort), true, &sortdoc)
		if err != nil {
			log.Println("Sort Unmarshal Error ", sort)
		} else {
			opts.SetSort(sortdoc)
		}
	}

	if skip > 0 {
		log.Println(filterdoc)
		opts.SetSkip(skip)
	}

	if limit > 0 {
		log.Println(filterdoc)
		opts.SetLimit(limit)
	}

	filterdoc = append(filterdoc,
		bson.E{Key: platform_common.FLD_APP_ROLE_ID, Value: roleid},
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	log.Println("Parameter values ", filterdoc, opts)
	cursor, err := collection.Find(ctx, filterdoc, opts)
	if err != nil {
		return nil, err
	}

	// get a list of all returned documents and print them out
	// see the mongo.Cursor documentation for more examples of using cursors
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	listdata := []utils.Map{}
	for idx, value := range results {
		log.Println("Item ", idx)
		delete(value, db_common.FLD_DEFAULT_ID)
		listdata = append(listdata, value)
	}

	log.Println("End - Find All Collection Dao", listdata)

	log.Println("Parameter values ", filterdoc)
	filtercount, err := collection.CountDocuments(ctx, filterdoc)
	if err != nil {
		return utils.Map{}, err
	}

	totalcount, err := collection.CountDocuments(ctx, bson.D{{Key: db_common.FLD_IS_DELETED, Value: false}})
	if err != nil {
		return utils.Map{}, err
	}

	response := utils.Map{
		db_common.LIST_SUMMARY: utils.Map{
			db_common.LIST_TOTALSIZE:    totalcount,
			db_common.LIST_FILTEREDSIZE: filtercount,
			db_common.LIST_RESULTSIZE:   len(listdata),
		},
		db_common.LIST_RESULT: listdata,
	}

	return response, nil
}

// Insert - Insert Collection
func (t *AppRoleMongoDBDao) AddCredentials(roleID string, indata utils.Map) (utils.Map, error) {

	log.Println("User Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoleCreds)
	if err != nil {
		return indata, err
	}

	dataResponse := []utils.Map{}

	dataCreds := indata["credentials_valid"].(utils.Map)
	log.Println("validateAddCreds:: Parameter Value", indata["credentials_valid"], dataCreds, reflect.TypeOf(dataCreds))

	for keyCred, valueCred := range dataCreds {
		log.Println("Creds Data Type ", reflect.TypeOf(keyCred), valueCred)

		guid := xid.New()
		prefix := "rolcrd_"
		log.Println("Unique Role Cred ID", prefix, guid.String())

		credData := utils.Map{}
		credData[platform_common.FLD_APP_ROLE_CRED_ID] = prefix + guid.String()
		credData[platform_common.FLD_APP_ROLE_ID] = roleID
		credData[platform_common.FLD_APP_ROLE_CREDENTIAL] = keyCred
		credData[db_common.FLD_CREATED_AT] = time.Now()
		credData[db_common.FLD_UPDATED_AT] = time.Now()
		credData[db_common.FLD_IS_DELETED] = false
		insertResult, err := collection.InsertOne(ctx, credData)
		if err != nil {
			log.Println("Error in insert ", err, insertResult)
			return credData, err
		}
		dataResponse = append(dataResponse, credData)
	}

	log.Println("Save - End", dataResponse)

	indata["credentials_created"] = dataResponse

	return indata, nil
}

func (t *AppRoleMongoDBDao) FindCredential(filter string) (utils.Map, error) {

	var result utils.Map

	log.Println("CheckCredentials - Begin", filter)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoleCreds)
	if err != nil {
		return nil, err
	}

	bfilter := bson.D{}
	err = bson.UnmarshalExtJSON([]byte(filter), true, &bfilter)
	if err != nil {
		fmt.Println("Error on filter Unmarshal", err)
	}
	// Add FLS_IS_DELETED flag also
	bfilter = append(bfilter, bson.E{Key: db_common.FLD_IS_DELETED, Value: false})
	log.Println("FindCredential:: Got filter ", bfilter)

	singleResult := collection.FindOne(ctx, bfilter)
	if singleResult.Err() != nil {
		log.Println("FindCredential:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Println("Save - End", result)

	return result, nil
}

func (t *AppRoleMongoDBDao) GetCredentials(role_id string) (utils.Map, error) {

	var results []utils.Map

	log.Println("GetCredential - Begin", role_id)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoleCreds)
	if err != nil {
		return nil, err
	}

	filterdoc := bson.D{
		{Key: platform_common.FLD_APP_ROLE_ID, Value: role_id},
		{Key: db_common.FLD_IS_DELETED, Value: false}, {}}

	log.Println("FindCredential:: Got filter ", filterdoc)
	cursor, err := collection.Find(ctx, filterdoc)
	if err != nil {
		return nil, err
	}

	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	listdata := []utils.Map{}
	for idx, value := range results {
		log.Println("Item ", idx)
		delete(value, db_common.FLD_DEFAULT_ID)
		listdata = append(listdata, value)
	}

	log.Println("End - Find All Collection Dao", listdata)
	response := utils.Map{
		"credentials": listdata,
	}
	log.Println("Save - End", results)

	return response, nil
}

func (t *AppRoleMongoDBDao) FindUser(filter string) (utils.Map, error) {

	var result utils.Map

	log.Println("FindRoleUsers - Begin", filter)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoleUsers)
	if err != nil {
		return nil, err
	}

	bfilter := bson.D{}
	err = bson.UnmarshalExtJSON([]byte(filter), true, &bfilter)
	if err != nil {
		fmt.Println("Error on filter Unmarshal", err)
	}
	// Add FLS_IS_DELETED flag also
	bfilter = append(bfilter, bson.E{Key: db_common.FLD_IS_DELETED, Value: false})
	log.Println("FindRoleUsers:: Got filter ", bfilter)

	singleResult := collection.FindOne(ctx, bfilter)
	if singleResult.Err() != nil {
		log.Println("FindCredential:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	log.Println("FindRoleUsers: Save - End", result)

	return result, nil
}

// Insert - Insert Collection
func (t *AppRoleMongoDBDao) AddUsers(role_id string, indata utils.Map) (utils.Map, error) {

	log.Println("User Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoleUsers)
	if err != nil {
		return indata, err
	}

	dataResponse := []utils.Map{}

	dataUsers := indata["app_users_valid"].(utils.Map)
	log.Println("AddUsers:: Parameter Value", indata["app_users_valid"], dataUsers, reflect.TypeOf(dataUsers))

	for keyUser, valueUser := range dataUsers {
		log.Println("Users Data Type ", reflect.TypeOf(keyUser), valueUser)

		guid := xid.New()
		prefix := "rolusr_"
		log.Println("Unique Role Cred ID", prefix, guid.String())

		userData := utils.Map{}
		userData[platform_common.FLD_APP_ROLE_USER_ID] = prefix + guid.String()
		userData[platform_common.FLD_APP_ROLE_ID] = role_id
		userData[platform_common.FLD_SYS_USER_ID] = keyUser
		userData[db_common.FLD_CREATED_AT] = time.Now()
		userData[db_common.FLD_UPDATED_AT] = time.Now()
		userData[db_common.FLD_IS_DELETED] = false
		insertResult, err := collection.InsertOne(ctx, userData)
		if err != nil {
			log.Println("Error in insert ", err, insertResult)
			return userData, err
		}
		dataResponse = append(dataResponse, userData)
	}

	log.Println("Save - End", dataResponse)

	indata["app_users_created"] = dataResponse

	return indata, nil
}

func (t *AppRoleMongoDBDao) GetUsers(role_id string) (utils.Map, error) {

	var results []utils.Map

	log.Println("GetUsers - Begin", role_id)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformAppRoleUsers)
	if err != nil {
		return nil, err
	}

	filterdoc := bson.D{
		{Key: platform_common.FLD_APP_ROLE_ID, Value: role_id},
		{Key: db_common.FLD_IS_DELETED, Value: false}, {}}

	log.Println("GetUsers:: Got filter ", filterdoc)
	cursor, err := collection.Find(ctx, filterdoc)
	if err != nil {
		return nil, err
	}

	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	listdata := []utils.Map{}
	for idx, value := range results {
		log.Println("Item ", idx)
		delete(value, db_common.FLD_DEFAULT_ID)
		listdata = append(listdata, value)
	}

	log.Println("End - Find All Collection Dao", listdata)
	response := utils.Map{
		"app_users": results,
	}
	log.Println("Save - End", results)

	return response, nil
}
