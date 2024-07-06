package mongodb_repository

import (
	"fmt"
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/mongo_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BusinessMongoDBDao - User DAO Repository
type BusinessMongoDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (p *BusinessMongoDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize BusinessMongoDBDao")
	p.client = client
}

// List - List all Collections
func (p *BusinessMongoDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map

	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformBusinesses)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinesses)
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

	// Add FLD_IS_DELETE also in filter
	filterdoc = append(filterdoc, bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

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
		value = db_common.AmendFldsForGet(value)
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
	log.Println("Total Documents ", totalcount)

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

func (p *BusinessMongoDBDao) Get(business_id string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("BusinessMongoDBDao::Find:: Begin ", business_id)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinesses)
	log.Println("Find:: Got Collection ")

	filter := bson.D{
		{Key: platform_common.FLD_BUSINESS_ID, Value: business_id},
		{Key: db_common.FLD_IS_DELETED, Value: false}, {}}

	log.Println("Find:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("Find:: Record not found ", singleResult.Err())
		return utils.Map{}, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return utils.Map{}, err
	}

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("BusinessMongoDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Update - Update Collection
func (p *BusinessMongoDBDao) Update(userid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinesses)
	if err != nil {
		return utils.Map{}, err
	}
	// Modify Fields for Update
	indata = db_common.AmendFldsforUpdate(indata)

	// Update a single document
	log.Printf("Update - Values %v", indata)

	filter := bson.D{{Key: platform_common.FLD_BUSINESS_ID, Value: userid}}

	updateResult, err := collection.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: indata}})
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult.ModifiedCount)

	log.Println("Update - End")
	return indata, nil
}

// Insert - Insert Collection
func (p *BusinessMongoDBDao) Create(indata utils.Map) (utils.Map, error) {

	log.Println("User Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinesses)
	if err != nil {
		return indata, err
	}

	// Add Fields for Create
	indata = db_common.AmendFldsforCreate(indata)

	// Also add following fields for SysBusiness
	indata[db_common.FLD_IS_SUSPENDED] = false
	indata[db_common.FLD_IS_ACTIVATED] = false

	insertResult, err := collection.InsertOne(ctx, indata)
	if err != nil {
		log.Println("Error in insert ", err)
		return indata, err

	}
	log.Println("Inserted a single document: ", insertResult.InsertedID)
	log.Println("Save - End", indata[platform_common.FLD_BUSINESS_ID])

	return indata, nil
}

// Find - Find by code
func (p *BusinessMongoDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SysBusiness::Find:: Begin ", filter)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinesses)
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

	log.Printf("SysBusiness::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Delete - Delete Collection
func (p *BusinessMongoDBDao) Delete(userid string) (int64, error) {

	log.Println("BusinessMongoDBDao::Delete - Begin ", userid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinesses)
	if err != nil {
		return 0, err
	}
	opts := options.Delete().SetCollation(&options.Collation{
		Locale:    db_common.LOCALE,
		Strength:  1,
		CaseLevel: false,
	})

	filter := bson.D{{Key: platform_common.FLD_BUSINESS_ID, Value: userid}}

	//filter = append(filter, bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: businessID})

	res, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("BusinessMongoDBDao::Delete - End deleted %v documents\n", res.DeletedCount)
	return res.DeletedCount, nil
}

// AddUser - Grand Business access to user
func (p *BusinessMongoDBDao) AddUser(indata utils.Map) (utils.Map, error) {

	log.Println("User Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinessUser)
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
	log.Println("Save - End", indata[platform_common.FLD_BUSINESS_USER_ID])

	return indata, err
}

// Update Business User
func (p *BusinessMongoDBDao) UpdateUser(accessid string, indata utils.Map) (utils.Map, error) {
	log.Println("UpdateUser - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinessUser)
	if err != nil {
		return indata, err
	}

	// Add Fields for Create
	indata = db_common.AmendFldsforUpdate(indata)

	filter := bson.D{{Key: platform_common.FLD_BUSINESS_USER_ID, Value: accessid}}
	updateResult, err := collection.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: indata}})
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("UpdateUser a single document: ", updateResult.ModifiedCount)

	log.Println("UpdateUser - End")
	return indata, nil
}

// Delete - Delete Collection
func (p *BusinessMongoDBDao) RemoveUser(accessid string) (string, error) {

	log.Println("BusinessMongoDBDao::RemoveUser - Begin ", accessid)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinessUser)
	if err != nil {
		return accessid, err
	}
	opts := options.Delete().SetCollation(&options.Collation{
		Locale:    db_common.LOCALE,
		Strength:  1,
		CaseLevel: false,
	})

	filter := bson.D{{Key: platform_common.FLD_BUSINESS_USER_ID, Value: accessid}}
	res, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		log.Println("Error in delete ", err)
		return accessid, err
	}
	log.Printf("BusinessMongoDBDao::RemoveUser - End deleted %v documents\n", res.DeletedCount)
	return accessid, nil
}

func (p *BusinessMongoDBDao) GetAccessDetails(accessid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("BusinessMongoDBDao::GetAccessDetails:: Begin ", accessid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinessUser)
	log.Println("GetAccessDetails:: Got Collection ")

	filter := bson.D{
		{Key: platform_common.FLD_BUSINESS_USER_ID, Value: accessid},
		{Key: db_common.FLD_IS_DELETED, Value: false}, {}}

	log.Println("Find:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("GetAccessDetails:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("BusinessMongoDBDao::GetAccessDetails:: End Found a single document: %+v\n", result)
	return result, nil
}

// UserList - List all Users registered in the give business
func (p *BusinessMongoDBDao) UserList(businessid string, filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map

	log.Println("Begin - Find All Users Dao", platform_common.DbPlatformBusinessUser)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinessUser)
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
		bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: businessid},
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
	user_collection, _, usr_err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformAppUsers)
	if usr_err != nil {
		return nil, usr_err
	}

	listdata := []utils.Map{}
	var result utils.Map
	for idx, value := range results {
		log.Println("Item ", idx)
		delete(value, db_common.FLD_DEFAULT_ID)

		userid := value[platform_common.FLD_APP_USER_ID].(string)
		filter := bson.D{
			{Key: platform_common.FLD_APP_USER_ID, Value: userid},
			{Key: db_common.FLD_IS_DELETED, Value: false}, {}}
		log.Println("Find:: Got filter ", filter)

		singleResult := user_collection.FindOne(ctx, filter)
		if singleResult.Err() != nil {
			log.Println("GetAccessDetails:: Record not found ", singleResult.Err())
			return nil, singleResult.Err()
		}
		err = singleResult.Decode(&result)
		if err != nil {
			log.Println("Error in decode", err)
			//return result, err
		} else {
			value[platform_common.FLD_APP_USER_EMAILID] = result[platform_common.FLD_APP_USER_EMAILID].(string)
			value[platform_common.FLD_APP_USER_PHONE] = result[platform_common.FLD_APP_USER_PHONE].(string)
		}
		// Remove fields from value
		value = db_common.AmendFldsForGet(value)
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

// BusinessList - List all Business registered with the give userId
func (p *BusinessMongoDBDao) BusinessList(userId string, filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map

	log.Println("Begin - Find All Users Dao", platform_common.DbPlatformBusinessUser)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinessUser)
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
		bson.E{Key: platform_common.FLD_APP_USER_ID, Value: userId},
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
	business_collection, _, usr_err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformBusinesses)
	if usr_err != nil {
		return nil, usr_err
	}

	listdata := []utils.Map{}
	var result utils.Map
	for idx, value := range results {
		log.Println("Item ", idx)
		delete(value, db_common.FLD_DEFAULT_ID)

		bussId := value[platform_common.FLD_BUSINESS_ID].(string)
		filter := bson.D{
			{Key: platform_common.FLD_BUSINESS_ID, Value: bussId},
			{Key: db_common.FLD_IS_DELETED, Value: false}, {}}
		log.Println("Find:: Got filter ", filter)

		singleResult := business_collection.FindOne(ctx, filter)
		if singleResult.Err() != nil {
			log.Println("GetAccessDetails:: Record not found ", singleResult.Err())
			return nil, singleResult.Err()
		}
		err = singleResult.Decode(&result)
		if err != nil {
			log.Println("Error in decode", err)
			//return result, err
		} else {
			value[platform_common.FLD_BUSINESS_NAME] = result[platform_common.FLD_BUSINESS_NAME].(string)
			value[platform_common.FLD_BUSINESS_EMAILID] = result[platform_common.FLD_BUSINESS_EMAILID].(string)
		}
		// Remove fields from value
		value = db_common.AmendFldsForGet(value)
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
