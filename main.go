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
	"sync"
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

func batchGenerator(cnf *config.Config, ch chan []interface{}) {
	// dice into batches
	batchSize := 10_000
	var batchCounts int
	if cnf.NRows <= batchSize {
		batchCounts = 1
	} else {
		batchCounts = cnf.NRows / batchSize
		// add 1 to batchCounts if there's leftover
		if cnf.NRows%batchSize != 0 {
			batchCounts += 1
		}
	}

	for batch := 0; batch < batchCounts; batch++ {
		batchData := []interface{}{}
		batchStartIdx := batch * batchSize
		batchEndIdx := batchStartIdx + batchSize
		for i := batchStartIdx; i < batchEndIdx; i++ {
			rowData := bson.M{}
			for _, generator := range cnf.Generators {
				rowData[generator.Name()] = generator.Generate()
			}
			batchData = append(batchData, rowData)
		}
		ch <- batchData
	}
	close(ch)
}

// TODO: convert to limit maximum in queue worker to prevent memory from growing out of control
func batchInserter(cnf *config.Config, conn *mongo.Client, ch chan []interface{}, done chan bool) {
	var wg sync.WaitGroup
	batchN := 0
	for batchData := range ch {
		logger.Info(
			"Obtained data",
			zap.Int("size", len(batchData)),
			zap.String("dataType", reflect.TypeOf(batchData).String()),
		)
		wg.Add(1)
		batchN++
		go insertToMongo(&wg, cnf, conn, batchData, batchN)
	}

	wg.Wait()
	done <- true
}

func insertToMongo(
	wg *sync.WaitGroup,
	cnf *config.Config,
	conn *mongo.Client,
	data []interface{},
	batchN int,
) {
	collection := conn.Database(cnf.DbName).Collection(cnf.CollectionName)
	ctx, cancelInsert := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelInsert()
	insertResult, insertErr := collection.InsertMany(ctx, data)
	if insertErr != nil {
		panic(insertErr)
	}
	logger.Info(fmt.Sprintf("Insert successful for batch %d", batchN), zap.Int("length", len(insertResult.InsertedIDs)))
	wg.Done()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	path := "./config.yaml"

	logger.Info("Initializing")

	raw := readFile(path)
	cnf, err := config.Parse(raw)
	if len(err) != 0 {
		logger.Error("Config Validation error")
		for i, err := range err {
			logger.Error(
				"Error "+fmt.Sprint(i),
				zap.String("msg", err.Error()),
			)
		}
		panic("Exit")
	}
	/*
		data := []interface{}{}
		for i := uint64(0); i < uint64(cnf.NRows); i++ {
			if i%500 == 0 {
				logger.Info(fmt.Sprintf("Generating data. currently at: %d/%d", i, cnf.NRows))
			}
			rowdata := bson.M{}
			for _, generator := range cnf.Generators {
				rowdata[generator.Name()] = generator.Generate()
			}
			data = append(data, rowdata)
		}
	*/
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

	dataChannel := make(chan []interface{}, 30)
	doneChannel := make(chan bool)

	go batchGenerator(&cnf, dataChannel)
	go batchInserter(&cnf, conn, dataChannel, doneChannel)

	<-doneChannel
	logger.Info(
		"Data pump completed successfully. Terminating program",
	)

	/*
		// get collection
		collection := conn.Database(cnf.DbName).Collection(cnf.CollectionName)
		ctx, cancelInsert := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancelInsert()
		insertResult, insertErr := collection.InsertMany(ctx, data)
		if insertErr != nil {
			panic(insertErr)
		}
		logger.Info("Insert successful", zap.Int("length", len(insertResult.InsertedIDs)))
	*/

	defer func() {
		dcErr := conn.Disconnect(context.TODO())
		if dcErr != nil {
			log.Fatal(err)
		}
	}()
}
