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

// SysUserMongoDBDao - User DAO Repository
type SysUserMongoDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (p *SysUserMongoDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize SysUserMongoDBDao")
	p.client = client
}

// List - List all Collections
func (t *SysUserMongoDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map

	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformSysUsers)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformSysUsers)
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
	// Add FLS_IS_DELETED flag also
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

		_, dataok := value[platform_common.FLD_SYS_USER_PASSWORD]
		if dataok {
			delete(value, platform_common.FLD_SYS_USER_PASSWORD)
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

func (t *SysUserMongoDBDao) Get(userid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SysUserMongoDBDao::Find:: Begin ", userid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformSysUsers)
	log.Println("Find:: Got Collection ")

	filter := bson.D{
		{Key: platform_common.FLD_SYS_USER_ID, Value: userid},
		{Key: db_common.FLD_IS_DELETED, Value: false}, {}}
	log.Println("Find:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("Find:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("SysUserMongoDBDao::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Update - Update Collection
func (t *SysUserMongoDBDao) Update(userid string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformSysUsers)
	if err != nil {
		return utils.Map{}, err
	}
	// Modify Fields for Update
	indata = db_common.AmendFldsforUpdate(indata)

	// Update a single document
	log.Printf("Update - Values %v", indata)

	filter := bson.D{{Key: platform_common.FLD_SYS_USER_ID, Value: userid}}

	updateResult, err := collection.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: indata}})
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult.ModifiedCount)

	log.Println("Update - End")
	return indata, nil
}

// Find - Find by code
func (t *SysUserMongoDBDao) Authenticate(auth_key string, auth_login string, auth_pwd string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SysUserMongoDBDao::Authenticate:: Begin ", auth_key, auth_login)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformSysUsers)
	log.Println("Find:: Got Collection ")

	filter := bson.M{auth_key: auth_login, platform_common.FLD_SYS_USER_PASSWORD: auth_pwd, db_common.FLD_IS_DELETED: false}

	log.Println("Find:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)

	if singleResult.Err() != nil {
		log.Println("Find:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("SysUserMongoDBDao::Authenticate:: End Found a single document: %+v\n", result)
	return result, nil
}

// Insert - Insert Collection
func (t *SysUserMongoDBDao) Create(indata utils.Map) (utils.Map, error) {

	log.Println("User Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformSysUsers)
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

	userid := indata[platform_common.FLD_SYS_USER_ID].(string)
	log.Println("Inserted a single document: ", insertResult.InsertedID)
	log.Println("Save - End", userid)

	return t.Get(userid)
}

// Find - Find by code
func (t *SysUserMongoDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("SysUser::Find:: Begin ", filter)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformSysUsers)
	log.Println("Find:: Got Collection ", err)

	bfilter := bson.D{}
	err = bson.UnmarshalExtJSON([]byte(filter), true, &bfilter)
	if err != nil {
		fmt.Println("Error on filter Unmarshal", err)
	}

	// Add IS_DELETE flag filter
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

	delete(result, platform_common.FLD_SYS_USER_PASSWORD)

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("SysUser::Find:: End Found a single document: %+v\n", result)
	return result, nil
}

// Delete - Delete Collection
func (t *SysUserMongoDBDao) Delete(userid string) (int64, error) {

	log.Println("SysUserMongoDBDao::Delete - Begin ", userid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformSysUsers)
	if err != nil {
		return 0, err
	}
	opts := options.Delete().SetCollation(&options.Collation{
		Locale:    db_common.LOCALE,
		Strength:  1,
		CaseLevel: false,
	})

	filter := bson.D{{Key: platform_common.FLD_SYS_USER_ID, Value: userid}}

	//filter = append(filter, bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: businessID})

	res, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("SysUserMongoDBDao::Delete - End deleted %v documents\n", res.DeletedCount)
	return res.DeletedCount, nil
}
