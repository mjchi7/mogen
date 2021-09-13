package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"mjchi7/mogen/config"
	"reflect"
	"time"

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
	config, err := config.Parse(raw)
	if len(err) != 0 {
		fmt.Println("Validation error")
		for _, err := range err {
			fmt.Println(err)
		}
		panic("Exit")
	}
	fieldsData := make(map[string]interface{})
	for _, field := range config.Fields {
		log.Printf("Building data for field: %v", field.Name)
		if field.Generator == "bool" {
			bools := generateBool(field, nrows)
			fieldsData[field.Name] = bools
		}
		if field.Generator == "name" {
			names := generateName(field, nrows)
			fieldsData[field.Name] = names
		}
	}
	log.Printf("Total fields: %v", len(fieldsData))

	mongoData := make([]interface{}, 0)
	fields := []string{}
	for k := range fieldsData {
		fields = append(fields, k)
	}
	// convert map of array to array of map
	for i := uint64(0); i < nrows; i++ {
		currentRow := make(map[string]interface{})
		for _, key := range fields {
			fieldType := reflect.TypeOf(fieldsData[key]).String()
			if fieldType == "[]bool" {
				fieldData := fieldsData[key].([]bool)[i]
				currentRow[key] = fieldData
			} else if fieldType == "[]string" {
				fieldData := fieldsData[key].([]string)[i]
				currentRow[key] = fieldData
			} else {
				panic("Unrecognized type: " + fieldType)
			}
		}
		mongoData = append(mongoData, currentRow)
	}
	log.Printf("Size of mongoData: %v", len(mongoData))
	// build connection and get collection
	ctx, cancelConn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelConn()
	conn := buildConn(ctx, config.Host, config.Port, false, config.DbName, 30)
	ctx, cancelVerify := context.WithTimeout(context.Background(), 5*time.Second)
	connErr := verifyConn(ctx, conn)
	defer cancelVerify()
	if connErr != nil {
		panic(connErr)
	}
	// get collection
	collection := conn.Database(config.DbName).Collection(config.CollectionName)
	ctx, cancelInsert := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelInsert()
	insertResult, insertErr := collection.InsertMany(ctx, mongoData)
	if insertErr != nil {
		panic(insertErr)
	}
	log.Printf("Total documents inserted: %v", len(insertResult.InsertedIDs))

	dcErr := conn.Disconnect(context.TODO())
	if dcErr != nil {
		log.Fatal(err)
	}
}
