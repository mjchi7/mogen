package generator

import (
	"math/rand"

	"go.mongodb.org/mongo-driver/bson"
)

type Generator interface {
	Generate() bson.M
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

func (g *BoolGenerator) Generate() bson.M {
	dice := rand.Intn(100)
	var res bool
	if dice < g.TrueWeight {
		res = true
	} else {
		res = false
	}
	return bson.M{g.ColumnName: res}
}

func (g *NameGenerator) Generate() bson.M {
	totalOptionsCount := len(g.Options)
	dice := rand.Intn(totalOptionsCount)
	return bson.M{g.ColumnName: g.Options[dice]}
}
