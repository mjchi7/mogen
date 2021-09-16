package generator

import (
	"math/rand"

	"go.mongodb.org/mongo-driver/bson"
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
	NumberOfWords int
	ColumnName    string
	Options       []string
}

func (g *BoolGenerator) Generate() interface{} {
	dice := rand.Intn(100)
	var res bool
	if dice < g.TrueWeight {
		res = true
	} else {
		res = false
	}
	return bson.M{g.ColumnName: res}
}

func (g *BoolGenerator) Name() string {
	return g.ColumnName
}

func (g *NameGenerator) Generate() interface{} {
	totalOptionsCount := len(g.Options)
	dice := rand.Intn(totalOptionsCount)
	return bson.M{g.ColumnName: g.Options[dice]}
}

func (g *NameGenerator) Name() string {
	return g.ColumnName
}
