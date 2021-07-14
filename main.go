package main

import (
	"fmt"
	"io/ioutil"
	"mjchi7/mogen/config"
)

func readFile(path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func main() {
	path := "./config.yaml"

	raw := readFile(path)
	config := config.Parse(raw)

	fmt.Println(config)
}
