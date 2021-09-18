package config

import (
	"fmt"
	"mjchi7/mogen/generator"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}
}

type ValidationError struct {
	message string
}

func (v *ValidationError) Error() string {
	return "Validation error: " + v.message
}

type Config struct {
	Host           string  `yaml:"host"`
	Port           string  `yaml:"port"`
	DbName         string  `yaml:"dbName"`
	CollectionName string  `yaml:"collectionName"`
	NRows          int     `yaml:"nRows"`
	Fields         []Field `yaml:"fields"`
	Generators     []generator.Generator
}

type Field struct {
	Generator string
	Name      string
	Data      map[string]interface{}
}

// Name generator
// Data required:
// 	numberOfWords
func (f *Field) parseName() (error, generator.Generator) {
	if f.Name == "" {
		err := ValidationError{message: "name cannot be empty"}
		return &err, nil
	}
	options, ok := f.Data["options"]
	if !ok {
		err := ValidationError{message: "data.options cannot be empty"}
		return &err, nil
	}
	optionsConverted := convertSliceOfInterfacesToSliceOfString(options.([]interface{}))
	return nil, &generator.NameGenerator{
		Options:    optionsConverted,
		ColumnName: f.Name,
	}
}

func convertSliceOfInterfacesToSliceOfString(interfaces []interface{}) []string {
	result := []string{}
	for _, inter := range interfaces {
		result = append(result, inter.(string))
	}
	return result
}

// Boolean generator
// Data required:
// 	trueWeight
func (f *Field) parseBool() (error, generator.Generator) {
	if f.Name == "" {
		err := ValidationError{message: "name cannot be empty"}
		return &err, nil
	}
	val, ok := f.Data["trueWeight"]
	if !ok {
		err := ValidationError{message: "data.trueWeight cannot be empty"}
		return &err, nil
	}
	valInt, ok := val.(int)
	if !ok {
		err := ValidationError{message: "trueWeight must be an integer of range 0 - 100"}
		return &err, nil
	}

	fmt.Println(valInt)
	if valInt < 0 || valInt > 100 {
		err := ValidationError{message: "trueWeight must be an integer of range 0 - 100"}
		return &err, nil
	}
	return nil, &generator.BoolGenerator{
		ColumnName: f.Name,
		TrueWeight: f.Data["trueWeight"].(int),
	}
}

func Parse(raw string) (Config, []error) {
	config := Config{}
	err := yaml.Unmarshal([]byte(raw), &config)
	if err != nil {
		panic(err)
	}
	if config.Host == "" {
		logger.Warn("No host information set. default to localhost")
		config.Host = "localhost"
	}
	if config.Port == "" {
		logger.Warn("No port information set. default to 27017")
		config.Port = "27017"
	}
	if config.DbName == "" {
		logger.Warn("No dbName information set. default to mogen")
		config.DbName = "mogen"
	}
	if config.CollectionName == "" {
		logger.Warn("No collectionName information set. default to mogenDocuments")
		config.CollectionName = "mogenDocuments"
	}
	if config.NRows == 0 {
		logger.Warn("No nRows configured. Default to 20_000")
		config.NRows = 20_000
	}
	validationErrors := []error{}
	config.Generators = []generator.Generator{}
	for _, field := range config.Fields {
		var err error
		var generator generator.Generator
		switch field.Generator {
		case "name":
			err, generator = field.parseName()
		case "bool":
			err, generator = field.parseBool()
		default:
			err = &ValidationError{message: "Generator '" + field.Generator + "' is invalid"}
		}

		if generator != nil {
			config.Generators = append(config.Generators, generator)
		}
		if err != nil {
			validationErrors = append(validationErrors, err)
		}

	}
	return config, validationErrors
}
