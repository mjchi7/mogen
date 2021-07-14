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
	config := Parse(raw)
	assert.Equal(t, config.Host, "localhost", "Should be equal")
	assert.Equal(t, config.Port, "27017", "Should be equal")
}
