package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"mjchi7/mogen/config"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func readFile(path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func buildConn(ctx context.Context, host string, port string, tlsEnabled bool, databaseName string, maxPoolSize uint64) *mongo.Client {
	// instantiate a timeout context
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tlsConfig := tls.Config{
		InsecureSkipVerify: true,
	}
	uri := "mongodb://" + host + ":" + port + "/" + databaseName
	log.Printf("Mongo uri: %v", uri)
	mongoConfig := options.Client().ApplyURI(uri).SetMaxPoolSize(maxPoolSize).SetDirect(true)
	if tlsEnabled {
		uri = uri + "/?tls=true"
		mongoConfig.SetTLSConfig(&tlsConfig)
	}
	conn, err := mongo.Connect(ctx, mongoConfig)
	if err != nil {
		panic(err)
	}
	return conn
}

func verifyConn(ctx context.Context, c *mongo.Client) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err := c.Ping(ctx, readpref.Primary())
	return err
}

func main() {
	rand.Seed(time.Now().UnixNano())
	path := "./config.yaml"

	log.Printf("Initializing")
	nrows := uint64(2000000)
	log.Printf("nrows %v", nrows)
	raw := readFile(path)
	cnf, err := config.Parse(raw)
	if len(err) != 0 {
		fmt.Println("Validation error")
		for _, err := range err {
			fmt.Println(err)
		}
		panic("Exit")
	}
	data := []interface{}{}
	for i := uint64(0); i < nrows; i++ {
		rowdata := bson.M{}
		for _, generator := range cnf.Generators {
			rowdata[generator.Name()] = generator.Generate()
		}
		data = append(data, rowdata)
	}

	log.Printf("Size of mongoData: %v", len(data))
	// build connection and get collection
	ctx, cancelConn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelConn()
	conn := buildConn(ctx, cnf.Host, cnf.Port, false, cnf.DbName, 30)
	ctx, cancelVerify := context.WithTimeout(context.Background(), 5*time.Second)
	connErr := verifyConn(ctx, conn)
	defer cancelVerify()
	if connErr != nil {
		panic(connErr)
	}
	// get collection
	collection := conn.Database(cnf.DbName).Collection(cnf.CollectionName)
	ctx, cancelInsert := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelInsert()
	insertResult, insertErr := collection.InsertMany(ctx, data)
	if insertErr != nil {
		panic(insertErr)
	}
	log.Printf("Total documents inserted: %v", len(insertResult.InsertedIDs))

	dcErr := conn.Disconnect(context.TODO())
	if dcErr != nil {
		log.Fatal(err)
	}
}
