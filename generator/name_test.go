package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomName(t *testing.T) {
	name := RandomName()

	assert.Equal(t, name, "Bentley", "Random name")
}
