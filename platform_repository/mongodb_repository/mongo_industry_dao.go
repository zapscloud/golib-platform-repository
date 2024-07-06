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

// IndustryMongoDBDao - Industry DAO Repository
type IndustryMongoDBDao struct {
	client utils.Map
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (p *IndustryMongoDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize IndustryMongoDBDao")
	p.client = client
}

// List - List all Collections
func (t *IndustryMongoDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var collections []utils.Map

	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformIndustries)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformIndustries)
	if err != nil {
		fmt.Println("ERror MongoDB Collection :", err)
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
		fmt.Println("Collection.Find Error :", err)
		return nil, err
	}

	// get a list of all returned documents and print them out
	// see the mongo.Cursor documentation for more examples of using cursors
	if err = cursor.All(ctx, &collections); err != nil {
		return nil, err
	}

	listdata := []utils.Map{}
	for idx, value := range collections {
		log.Println("Item ", idx)
		value = db_common.AmendFldsForGet(value)
		listdata = append(listdata, value)
	}

	log.Println("End - Find All Collection Dao", listdata)

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
			db_common.LIST_RESULTSIZE:   len(collections),
		},
		db_common.LIST_RESULT: collections,
	}

	return response, nil
}

func (t *IndustryMongoDBDao) GetIndustryById(industryid string) (utils.Map, error) {

	// Find a single document
	var result utils.Map

	log.Println("GetIndustryById::Find:: Begin ", industryid)

	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformIndustries)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(t.client, platform_common.DbPlatformIndustries)
	if err != nil {
		fmt.Println("ERror MongoDB Collection :", err)
		return nil, err
	}

	filter := bson.D{{Key: platform_common.FLD_INDUSTRY_ID, Value: industryid},
		{Key: db_common.FLD_IS_DELETED, Value: false}, {}}

	log.Println("Find:: Got filter ", filter)

	singleResult := collection.FindOne(ctx, filter)
	if singleResult.Err() != nil {
		log.Println("Find:: Record not found ", singleResult.Err())
		return result, singleResult.Err()
	}
	err = singleResult.Decode(&result)
	if err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("sysUserMongoDao::Find:: End Found a single document: %+v\n", result)
	return result, nil

}
