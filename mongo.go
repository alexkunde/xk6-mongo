package xk6_mongo

import (
	"context"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.k6.io/k6/js/modules"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/mongo".
func init() {
	modules.Register("k6/x/mongo", new(Mongo))
}

// Mongo is the k6 extension for a Mongo client.
type Mongo struct{}

// Client is the Mongo client wrapper.
type Client struct {
	client *mongo.Client
}

type UpsertOneModel struct {
	Query  interface{} `json:"query"`
	Update interface{} `json:"update"`
}

// NewClient represents the Client constructor (i.e. `new mongo.Client()`) and
// returns a new Mongo client object.
// The `connURI` parameter in the `NewClient` function is used to specify the connection URI for the
// MongoDB client. It typically follows the format
// `mongodb://username:password@address:port/db?connect=direct`. This URI contains information such
// as the username, password, address, port, database name, and connection options.
// connURI -> mongodb://username:password@address:port/db?connect=direct
// connURI -> mongodb+srv://username:password@address:port/db?authSource=admin
func (*Mongo) NewClient(connURI string) *Client{} {
	log.Print("start creating new client")

	clientOptions := options.Client().ApplyURI(connURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Printf("Error while establishing a connection to MongoDB: %v", err)
		return nil
	}

		log.Print("created new client")
	return &Client{client: client}
}

func (c *Client) InsertOne(database string, collection string, doc string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)

	var bson_doc bson.D
	bson_err := bson.UnmarshalExtJSON([]byte(strings.TrimSpace(doc)), true, &bson_doc)
	if bson_err != nil {
		log.Printf("UnmarshalExtJSON: %+v", bson_err)
		return nil
	}

	_, err := col.InsertOne(context.Background(), bson_doc)
	if err != nil {
		log.Printf("InsertOne: %+v", err)
		return err
	}
	return nil
}

func (c *Client) InsertMany(database string, collection string, docs []any) error {
	log.Printf("Insert multiple documents")
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertMany(context.Background(), docs)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Upsert(database string, collection string, filter interface{}, update interface{}) error {
	db := c.client.Database(database)
	col := db.Collection(collection)

	opts := options.Update().SetUpsert(true)
	_, err := col.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		log.Printf("Error while upserting: %v", err)
		return err
	}
	return nil
}



func (c *Client) Aggregate(database string, collection string, pipeline interface{}) ([]bson.M, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	cur, err := col.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Printf("Error while aggregating: %v", err)
		return nil, err
	}
	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return nil, err
	}
	return results, nil
}

func (c *Client) FindMany(database string, collection string, filter string, sort interface{}, limit int64) []bson.M, error {
	db := c.client.Database(database)
	col := db.Collection(collection)

	opts := options.Find().SetSort(sort).SetLimit(limit)


	var bson_filter bson.D
	if filter != "" {
		log.Printf("MongoDB Query is %+v", filter)
		bson_err := bson.UnmarshalExtJSON([]byte(strings.TrimSpace(filter)), true, &bson_filter)
		if bson_err != nil {
			log.Printf("%+v", bson_err)
			return nil
		}
	} else {
		log.Printf("Setting filter to match all documents")
		bson_filter = bson.D{}
	}

	cur, err := col.Find(context.Background(), bson_filter, opts)
	if err != nil {
		log.Printf("Error while finding documents: %v", err)
		return nil, err
	}
	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return nil, err
	}
	return results, nil
}

func (c *Client) FindOne(database string, collection string, filter string) (bson.M, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	log.Printf("MongoDB Query is %+v", filter)

	var bson_filter bson.D
	err := bson.UnmarshalExtJSON([]byte(strings.TrimSpace(filter)), true, &bson_filter)
	if err != nil {
		log.Printf("UnmarshalExtJSON: %+v", err)
		return nil
	}

	var result bson.M
	opts := options.FindOne().SetSort(bson.D{{"_id", 1}})
	err = col.FindOne(context.Background(), bson_filter, opts).Decode(&result)
	if err == mongo.ErrNoDocuments {
		log.Printf("No document was found for filter %v", filter)
		return nil
	}
	if err != nil {
		log.Printf("Error while finding the document: %v", err)
		return nil
	}
	return result, nil
}

func (c *Client) UpdateOne(database string, collection string, filter interface{}, data map[string]string) (*mongo.UpdateResult, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	update := bson.D{{"$set", data}}
	result, err := col.UpdateOne(context.Background(), filter, update)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("found document %v", result)
	return result, nil
}

func (c *Client) UpdateMany(database string, collection string, filter interface{}, data bson.D) (*mongo.UpdateResult, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	update := bson.D{{"$set", data}}
	result, err := col.UpdateMany(context.Background(), filter, update)
	if err != nil {
		log.Printf("Error while updating the documents: %v", err)
		return err
	}
	log.Printf("found document %v", result)
	return result, nil
}

func (c *Client) FindAll(database string, collection string) ([]bson.M, error) {
	log.Printf("Find all documents")
	db := c.client.Database(database)
	col := db.Collection(collection)
	cur, err := col.Find(context.Background(), bson.D{{}})
	if err != nil {
		log.Printf("Error while finding documents: %v", err)
		return nil, err
	}

	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return nil, err
	}
	return results, nil
}

func (c *Client) DeleteOne(database string, collection string, filter map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)

	result, err := col.DeleteOne(context.Background(), filter)

	if err != nil {
		log.Printf("Error while deleting the document: %v", err)
		return err
	}

	log.Printf("Deleted documents %v", result)
	return nil
}

func (c *Client) DeleteMany(database string, collection string, filter map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)

	result, err := col.DeleteMany(context.Background(), filter)

	if err != nil {
		log.Printf("Error while deleting the documents: %v", err)
		return err
	}

	log.Printf("Deleted documents %v", result)
	return nil
}

func (c *Client) Distinct(database string, collection string, field string, filter interface{}) ([]interface{}, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	result, err := col.Distinct(context.Background(), field, filter)
	if err != nil {
		log.Printf("Error while getting distinct values: %v", err)
		return nil, err
	}

	return result, nil
}

func (c *Client) DropCollection(database string, collection string) error {
	log.Printf("Delete collection if present")
	db := c.client.Database(database)
	col := db.Collection(collection)

	err := col.Drop(context.Background())
	if err != nil {
		log.Printf("Error while dropping the collection: %v", err)
		return err
	}

	return nil
}

func (c *Client) CountDocuments(database string, collection string, filter interface{}) (int64, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)

	count, err := col.CountDocuments(context.Background(), filter)
	if err != nil {
		log.Printf("Error while counting documents: %v", err)
		return 0, err
	}

	return count, nil
}

func (c *Client) FindOneAndUpdate(database string, collection string, filter interface{}, update interface{}) (*mongo.SingleResult, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	result := col.FindOneAndUpdate(context.Background(), filter, update, opts)
	if result.Err() != nil {
		log.Printf("Error while finding and updating document: %v", result.Err())
		return nil, result.Err()
	}
	return result, nil
}

func (c *Client) Disconnect() error {
	err := c.client.Disconnect(context.Background())
	if err != nil {
		log.Printf("Error while disconnecting from the database: %v", err)
		return err
	}

	return nil
}

func (*Mongo) MongoEncode(input string) string {
	// https://www.mongodb.com/docs/manual/reference/connection-string/
	r := strings.NewReplacer("%", "%25", "$", "%24", ":", "%3A", "/", "%2F", "?", "%3F", "#", "%23", "[", "%5B", "]", "%5D", "@", "%40")
	res := r.Replace(input)
	return res
}
