package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"mjchi7/mogen/config"
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

func main() {
	path := "./config.yaml"

	raw := readFile(path)
	config := config.Parse(raw)

	fmt.Println(config)
}
