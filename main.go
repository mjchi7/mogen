package main

import (
	"context"
	"crypto/tls"
	"io/ioutil"
	"log"
	"math/rand"
	"mjchi7/mogen/config"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}
}

func readFile(path string) string {
	logger.Info(
		"Reading config file",
		zap.String("path", path),
	)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Error(
			"Error reading config file",
			zap.String("msg", err.Error()),
		)
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
	logger.Info(
		"Mongo URI Built",
		zap.String("uri", uri),
	)
	mongoConfig := options.Client().ApplyURI(uri).SetMaxPoolSize(maxPoolSize).SetDirect(true)
	if tlsEnabled {
		uri = uri + "/?tls=true"
		mongoConfig.SetTLSConfig(&tlsConfig)
	}
	conn, err := mongo.Connect(ctx, mongoConfig)
	if err != nil {
		logger.Error(
			"Error connecting to mongoDb",
			zap.String("msg", err.Error()),
		)
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

	logger.Info("Initializing")
	nrows := uint64(2000000)
	logger.Info(
		"Obtained nrows: "+string(nrows),
		zap.Uint64("nrows", nrows),
	)
	raw := readFile(path)
	cnf, err := config.Parse(raw)
	if len(err) != 0 {
		logger.Error("Config Validation error")
		for i, err := range err {
			logger.Error(
				"Error "+string(i),
				zap.String("msg", err.Error()),
			)
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

	logger.Info("Mongo Data generated.", zap.Int("size", len(data)))
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
	logger.Info("Insert successful", zap.Int("length", len(insertResult.InsertedIDs)))

	dcErr := conn.Disconnect(context.TODO())
	if dcErr != nil {
		log.Fatal(err)
	}
}
