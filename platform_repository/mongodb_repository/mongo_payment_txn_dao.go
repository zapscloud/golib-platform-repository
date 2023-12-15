package mongodb_repository

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/mongo_utils"
	"github.com/zapscloud/golib-platform-repository/platform_common"
	"github.com/zapscloud/golib-utils/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// / PaymentTxnMongoDBDao - Client DAO Repository
type PaymentTxnMongoDBDao struct {
	client utils.Map
}

// DeleteAll implements platform_repository.PaymentTxnDao.
func (*PaymentTxnMongoDBDao) DeleteAll() (int64, error) {
	panic("unimplemented")
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func (p *PaymentTxnMongoDBDao) InitializeDao(client utils.Map) {
	log.Println("Initialize Client Mongodb DAO")
	p.client = client

}

// List - List all Collections
func (p *PaymentTxnMongoDBDao) List(filter string, sort string, skip int64, limit int64) (utils.Map, error) {
	var results []utils.Map
	var bFilter bool = false
	log.Println("Begin - Find All Collection Dao", platform_common.DbPlatformPaymentTxns)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformPaymentTxns)
	if err != nil {
		return nil, err
	}

	log.Println("Get Collection - Find All Collection Dao", filter, len(filter), sort, len(sort))

	filterdoc := bson.D{}
	if len(filter) > 0 {
		// filters, _ := strconv.Unquote(string(filter))
		err = bson.UnmarshalExtJSON([]byte(filter), true, &filterdoc)
		if err != nil {
			log.Println("Unmarshal Ext JSON error", err)
		}
		bFilter = true
	}

	// All Stages
	stages := []bson.M{}

	// Remove unwanted fields
	unsetStage := bson.M{platform_common.MONGODB_UNSET: db_common.FLD_DEFAULT_ID}
	stages = append(stages, unsetStage)

	// Match Stage
	filterdoc = append(filterdoc,
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	filterdoc = append(filterdoc, bson.E{Key: db_common.FLD_IS_DELETED, Value: false})
	matchStage := bson.M{platform_common.MONGODB_MATCH: filterdoc}
	stages = append(stages, matchStage)

	if len(sort) > 0 {
		var sortdoc interface{}
		err = bson.UnmarshalExtJSON([]byte(sort), true, &sortdoc)
		if err != nil {
			log.Println("Sort Unmarshal Error ", sort)
		} else {
			sortStage := bson.M{platform_common.MONGODB_SORT: sortdoc}
			stages = append(stages, sortStage)
		}
	}

	var filtercount int64 = 0
	if bFilter {
		// Prepare Filter Stages
		filterStages := stages

		// Add Count aggregate
		countStage := bson.M{platform_common.MONGODB_COUNT: platform_common.FLD_FILTERED_COUNT}
		filterStages = append(filterStages, countStage)

		//log.Println("Aggregate for Count ====>", filterStages, stages)

		// Execute aggregate to find the count of filtered_size
		cursor, err := collection.Aggregate(ctx, filterStages)
		if err != nil {
			log.Println("Error in Aggregate", err)
			return nil, err
		}
		var countResult []utils.Map
		if err = cursor.All(ctx, &countResult); err != nil {
			log.Println("Error in cursor.all", err)
			return nil, err
		}

		//log.Println("Count Filter ===>", countResult)
		if len(countResult) > 0 {
			if dataVal, dataOk := countResult[0][platform_common.FLD_FILTERED_COUNT]; dataOk {
				filtercount = int64(dataVal.(int32))
			}
		}
		// log.Println("Count ===>", filtercount)

	} else {
		filtercount, err = collection.CountDocuments(ctx, filterdoc)
		if err != nil {
			return nil, err
		}
	}

	if skip > 0 {
		skipStage := bson.M{platform_common.MONGODB_SKIP: skip}
		stages = append(stages, skipStage)
	}

	if limit > 0 {
		limitStage := bson.M{platform_common.MONGODB_LIMIT: limit}
		stages = append(stages, limitStage)
	}

	cursor, err := collection.Aggregate(ctx, stages)
	if err != nil {
		return nil, err
	}

	// get a list of all returned documents and print them out
	// see the mongo.Cursor documentation for more examples of using cursors
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	basefilterdoc := bson.D{
		{Key: db_common.FLD_IS_DELETED, Value: false}}

	totalcount, err := collection.CountDocuments(ctx, basefilterdoc)
	if err != nil {
		return utils.Map{}, err
	}

	if results == nil {
		results = []utils.Map{}
	}

	response := utils.Map{
		db_common.LIST_SUMMARY: utils.Map{
			db_common.LIST_TOTALSIZE:    totalcount,
			db_common.LIST_FILTEREDSIZE: filtercount,
			db_common.LIST_RESULTSIZE:   len(results),
		},
		db_common.LIST_RESULT: results,
	}

	return response, nil
}

// Get - Get account details
func (p *PaymentTxnMongoDBDao) Get(PaymentTxn_id string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("accountMongoDao::Get:: Begin ", PaymentTxn_id)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformPaymentTxns)
	if err != nil {
		return nil, err
	}
	log.Println("Find:: Got Collection ")

	log.Println("Find:: Got Collection ")
	stages := []bson.M{}
	filter := bson.D{
		{Key: platform_common.FLD_PAYMENT_TXN_ID, Value: PaymentTxn_id},

		{Key: db_common.FLD_IS_DELETED, Value: false}}
	log.Println("Get:: Got filter ", filter)

	matchStage := bson.M{"$match": filter}
	stages = append(stages, matchStage)

	log.Println("GetDetails:: Got filter ", filter)

	// Aggregate the stages
	singleResult, err := collection.Aggregate(ctx, stages)
	if err != nil {
		log.Println("GetDetails:: Error in aggregation: ", err)
		return result, err
	}

	if !singleResult.Next(ctx) {
		// No matching document found
		err := &utils.AppError{ErrorCode: "S30102", ErrorMsg: "Record Not Found", ErrorDetail: "Given PaymentTxn is not found"}
		log.Println("GetDetails:: Record not found")
		return result, err
	}

	if err := singleResult.Decode(&result); err != nil {
		log.Println("Error in decode", err)
		return result, err
	}

	// Remove fields from result
	result = db_common.AmendFldsForGet(result)

	log.Printf("TxnMongoDBDao::GetDetails:: End Found a single document: %+v\n", result)
	return result, nil
}

// Find - Find by code
func (p *PaymentTxnMongoDBDao) Find(filter string) (utils.Map, error) {
	// Find a single document
	var result utils.Map

	log.Println("accountMongoDao::Find:: Begin ", filter)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformPaymentTxns)
	log.Println("Find:: Got Collection ", err)

	bfilter := bson.D{}
	err = bson.UnmarshalExtJSON([]byte(filter), true, &bfilter)
	if err != nil {
		log.Println("Error on filter Unmarshal", err)
	}
	bfilter = append(bfilter,
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

	log.Println("accountMongoDao::Find:: End Found a single document: \n", err)
	return result, nil
}

// Create - Create Collection
func (p *PaymentTxnMongoDBDao) Create(indata utils.Map) (utils.Map, error) {

	log.Println("Business Client Save - Begin", indata)
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformPaymentTxns)
	if err != nil {
		return indata, err
	}
	// Add Fields for Create
	indata = db_common.AmendFldsforCreate(indata)

	// Insert a single document
	insertResult, err := collection.InsertOne(ctx, indata)
	if err != nil {
		log.Println("Error in insert ", err)
		return indata, err

	}
	log.Println("Inserted a single document: ", insertResult.InsertedID)
	log.Println("Save - End", indata[platform_common.FLD_PAYMENT_TXN_ID])

	return indata, err
}

// Update - Update Collection
func (p *PaymentTxnMongoDBDao) Update(PaymentTxn_id string, indata utils.Map) (utils.Map, error) {

	log.Println("Update - Begin")
	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformPaymentTxns)
	if err != nil {
		return utils.Map{}, err
	}
	// Modify Fields for Update
	indata = db_common.AmendFldsforUpdate(indata)

	log.Printf("Update - Values %v", indata)

	filter := bson.D{
		{Key: platform_common.FLD_PAYMENT_TXN_ID, Value: PaymentTxn_id}}

	updateResult, err := collection.UpdateOne(ctx, filter, bson.D{{Key: platform_common.MONGODB_SET, Value: indata}})
	if err != nil {
		return utils.Map{}, err
	}
	log.Println("Update a single document: ", updateResult.ModifiedCount)

	log.Println("Update - End")
	return indata, nil
}

// Delete - Delete Collection
func (p *PaymentTxnMongoDBDao) Delete(PaymentTxn_id string) (int64, error) {

	log.Println("accountMongoDao::Delete - Begin ", PaymentTxn_id)

	collection, ctx, err := mongo_utils.GetMongoDbCollection(p.client, platform_common.DbPlatformPaymentTxns)
	if err != nil {
		return 0, err
	}
	opts := options.Delete().SetCollation(&options.Collation{
		Locale:    db_common.LOCALE,
		Strength:  1,
		CaseLevel: false,
	})

	filter := bson.D{
		{Key: platform_common.FLD_PAYMENT_TXN_ID, Value: PaymentTxn_id},
	}

	res, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		log.Println("Error in delete ", err)
		return 0, err
	}
	log.Printf("accountMongoDao::Delete - End deleted %v documents\n", res.DeletedCount)
	return res.DeletedCount, nil
}
