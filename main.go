package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math/rand"
	"mjchi7/mogen/config"
	"strconv"
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
	uri := "mongodb://" + host + ":" + port + "/databaseName"
	if tlsEnabled {
		uri = uri + "/?tls=true"
	}
	mongoConfig := options.Client().ApplyURI(uri).SetMaxPoolSize(maxPoolSize).SetTLSConfig(&tlsConfig).SetDirect(true)
	conn, err := mongo.Connect(ctx, mongoConfig)
	if err != nil {
		panic(err)
	}
	return conn
}

func verifyConn(ctx context.Context, c *mongo.Client) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := c.Ping(ctx, readpref.Primary())
	return err
}

func generateBool(field config.Field, nrows uint64) []bool {
	// TODO: Generate based on trueWeight
	truePct := int64(field.Data["trueWeight"].(int))
	data := []bool{}
	for i := uint64(0); i < nrows; i++ {
		random := rand.Int63n(100)
		if random < truePct {
			data = append(data, true)
		} else {
			data = append(data, false)
		}
	}
	return data
}

func main() {
	rand.Seed(time.Now().UnixNano())
	path := "./config.yaml"

	nrows := uint64(20000)
	raw := readFile(path)
	config, err := config.Parse(raw)
	if len(err) != 0 {
		fmt.Println("Validation error")
		for _, err := range err {
			fmt.Println(err)
		}
		panic("Exit")
	}
	dataInsert := []interface{}{}
	for _, field := range config.Fields {
		if field.Generator == "bool" {
			bools := generateBool(field, nrows)
			validateBoolPct(bools)
			dataInsert = append(dataInsert, bools)
		}
	}
}

func validateBoolPct(data []bool) {
	total := len(data)
	nTrue := 0
	nFalse := 0
	for _, d := range data {
		if d {
			nTrue++
		} else {
			nFalse++
		}
	}

	truePct := (float64(nTrue) / float64(total)) * 100
	fmt.Println("Total in slices: " + strconv.Itoa(total))
	fmt.Println("Number of true: " + strconv.Itoa(nTrue))
	fmt.Println("Number of false: " + strconv.Itoa(nFalse))
	fmt.Println("Percentage of true: " + strconv.FormatFloat(truePct, 'f', 2, 64))
}
