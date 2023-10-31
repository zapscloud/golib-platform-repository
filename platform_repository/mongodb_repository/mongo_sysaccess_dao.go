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

// SysAccessMongoDBDao - Access DAO Repository
type SysAccessMongoDBDao struct {
	client     utils.Map
	businessID string
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (p *SysAccessMongoDBDao) InitializeDao(client utils.Map, businessId string) {
	log.Println("Initialize Access Mongodb DAO")
	p.client = client
	p.businessID = businessId
}

// List - List all Collections
func (p *SysAccessMongoDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map

	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformSysUserAccess)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformSysUserAccess)
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
		bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: p.businessID},
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

	log.Println("End - Find All Collection Dao", results)

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
		return nil, err
	}

	// Add base business filter
	totalcount, err := collection.CountDocuments(ctx, bson.D{{Key: db_common.FLD_IS_DELETED, Value: false}})
	if err != nil {
		return nil, err
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

// Get - Get access details
func (p *SysAccessMongoDBDao) Get(accessid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("accessMongoDao::Get:: Begin ", accessid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformSysUserAccess)
	log.Println("Find:: Got Collection ")

	filter := bson.D{{Key: platform_common.FLD_SYS_ACCESS_ID, Value: accessid}, {}}

	filter = append(filter,
		bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: p.businessID},
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	log.Println("Get:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("Get:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}
	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Println("accessMongoDao::Get:: End Found a single document: \n", err)
	return result, nil
}

// Find - Find by code
func (p *SysAccessMongoDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("accessMongoDao::Find:: Begin ", filter)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformSysUserAccess)
	log.Println("Find:: Got Collection ", err)

	bfilter := bson.D{}
	err = bson.UnmarshalExtJSON([]byte(filter), true, &bfilter)
	if err != nil {
		fmt.Println("Error on filter Unmarshal", err)
	}
	bfilter = append(bfilter,
		bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: p.businessID},
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

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

	log.Println("accessMongoDao::Find:: End Found a single document: \n", err)
	return result, nil
}

// GrantPermission - GrantPermission Collection
func (p *SysAccessMongoDBDao) GrantPermission(indata utils.Map) (utils.Map, error) {

	log.Println("Business Access Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformSysUserAccess)
	if err != nil {
		return utils.Map{}, err
	}

	// Add Fields for Create
	indata = db_common.AmendFldsforCreate(indata)

	// Insert a single document
	insertResult, err := collection.InsertOne(ctx, indata)
	if err != nil {
		log.Println("Error in insert ", err)
		return utils.Map{}, err

	}
	log.Println("Inserted a single document: ", insertResult.InsertedID)
	log.Println("Save - End", indata[platform_common.FLD_SYS_ACCESS_ID])

	return indata, err
}

// RevokePermission - RevokePermission Collection
func (p *SysAccessMongoDBDao) RevokePermission(accessid string) (int64, error) {

	log.Println("accessMongoDao::RevokePermission - Begin ", accessid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformSysUserAccess)
	if err != nil {
		return 0, err
	}
	opts := options.Delete().SetCollation(&options.Collation{
		Locale:    db_common.LOCALE,
		Strength:  1,
		CaseLevel: false,
	})

	filter := bson.D{{Key: platform_common.FLD_SYS_ACCESS_ID, Value: accessid}}

	//filter = append(filter, bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: businessID})

	res, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("accessMongoDao::Delete - End deleted %v documents\n", res.DeletedCount)
	return res.DeletedCount, nil
}

// Get - Get role details
func (p *SysAccessMongoDBDao) GetRoleDetails(roleid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("roleMongoDao::Get:: Begin ", roleid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformSysRoles)
	log.Println("Find:: Got Collection ")

	filter := bson.D{{Key: platform_common.FLD_SYS_ACCESS_ROLE_ID, Value: roleid}, {}}

	filter = append(filter,
		bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: p.businessID},
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	log.Println("Get:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("Get:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}
	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Println("roleMongoDao::Get:: End Found a single document: \n", err)
	return result, nil
}

// Get - Get site details
func (p *SysAccessMongoDBDao) GetSiteDetails(siteid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("siteMongoDao::Get:: Begin ", siteid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbSites)
	log.Println("Find:: Got Collection ")

	filter := bson.D{{Key: platform_common.FLD_SYS_ACCESS_SITE_ID, Value: siteid}, {}}

	filter = append(filter,
		bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: p.businessID},
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	log.Println("Get:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("Get:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}
	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Println("siteMongoDao::Get:: End Found a single document: \n", err)
	return result, nil
}

// Get - Get department details
func (p *SysAccessMongoDBDao) GetDepartmentDetails(departmentid string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("departmentMongoDao::Get:: Begin ", departmentid)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbDepartments)
	log.Println("Find:: Got Collection ")

	filter := bson.D{{Key: platform_common.FLD_SYS_ACCESS_DEPARTMENT_ID, Value: departmentid}, {}}

	filter = append(filter,
		bson.E{Key: platform_common.FLD_BUSINESS_ID, Value: p.businessID},
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	log.Println("Get:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("Get:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}
	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Println("departmentMongoDao::Get:: End Found a single document: \n", err)
	return result, nil
}
