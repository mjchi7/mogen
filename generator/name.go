package generator

import (
	"math/rand"
)

type Generator interface {
	Generate() interface{}
	Name() string
}

type BoolGenerator struct {
	TrueWeight int
	ColumnName string
}

type NameGenerator struct {
	ColumnName string
	Options    []string
}

func (g *BoolGenerator) Generate() interface{} {
	dice := rand.Intn(100)
	var res bool
	if dice < g.TrueWeight {
		res = true
	} else {
		res = false
	}
	return res
}

func (g *BoolGenerator) Name() string {
	return g.ColumnName
}

func (g *NameGenerator) Generate() interface{} {
	totalOptionsCount := len(g.Options)
	dice := rand.Intn(totalOptionsCount)
	return g.Options[dice]
}

func (g *NameGenerator) Name() string {
	return g.ColumnName
}
