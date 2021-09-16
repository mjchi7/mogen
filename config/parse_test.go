package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	raw := `
host: localhost
port: 27017
`
	config, _ := Parse(raw)
	assert.Equal(t, config.Host, "localhost", "Should be equal")
	assert.Equal(t, config.Port, "27017", "Should be equal")
	assert.Equal(t, config.DbName, "mogen", "Default db name if not specified")
	assert.Equal(t, config.CollectionName, "mogenDocuments", "Default collection name if not specified")
}

func TestParseGenerators(t *testing.T) {
	raw := `
host: localhost
port: 27017
fields:
  - generator: name
    name: firstName
    data:
      numberOfWords: 3
`

	config, _ := Parse(raw)

	assert.Equal(t, len(config.Generators), 1)
	assert.Equal(t, config.Generators[0].Name(), "firstName")
}
