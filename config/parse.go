package config

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

type ValidationError struct {
	message string
}

func (v *ValidationError) Error() string {
	return "Validation error: " + v.message
}

type Config struct {
	Host   string
	Port   string
	Fields []Field
}

type Field struct {
	Generator string
	Name      string
	Data      map[string]interface{}
}

// Name generator
// Data required:
// 	numberOfWords
func (f *Field) verifyName() error {
	if f.Name == "" {
		err := ValidationError{message: "name cannot be empty"}
		return &err
	}
	_, ok := f.Data["numberOfWords"]
	if !ok {
		err := ValidationError{message: "data.numberOfWords cannot be empty"}
		return &err
	}
	return nil
}

// Boolean generator
// Data required:
// 	trueWeight
func (f *Field) verifyBool() error {
	if f.Name == "" {
		err := ValidationError{message: "name cannot be empty"}
		return &err
	}
	val, ok := f.Data["trueWeight"]
	if !ok {
		err := ValidationError{message: "data.trueWeight cannot be empty"}
		return &err
	}
	valInt, ok := val.(int)
	if !ok {
		err := ValidationError{message: "trueWeight must be an integer of range 0 - 100"}
		return &err
	}

	fmt.Println(valInt)
	if valInt < 0 || valInt > 100 {
		err := ValidationError{message: "trueWeight must be an integer of range 0 - 100"}
		return &err
	}
	return nil
}

func Parse(raw string) (Config, []error) {
	config := Config{}
	err := yaml.Unmarshal([]byte(raw), &config)
	if err != nil {
		panic(err)
	}
	validationErrors := []error{}
	for _, field := range config.Fields {
		var err error
		if field.Generator == "name" {
			err = field.verifyName()
		} else if field.Generator == "bool" {
			err = field.verifyBool()
		} else {
			err = &ValidationError{message: "Generator '" + field.Generator + "' is invalid"}
		}
		if err != nil {
			validationErrors = append(validationErrors, err)
		}
	}
	return config, validationErrors
}
