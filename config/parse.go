package config

import "gopkg.in/yaml.v2"

type Config struct {
	Host string
	Port string
}

func Parse(raw string) Config {
	config := Config{}
	err := yaml.Unmarshal([]byte(raw), &config)
	if err != nil {
		panic(err)
	}
	return config
}
