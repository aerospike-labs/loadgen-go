package main

import (
	"github.com/aerospike/aerospike-client-go"
	"math/rand"
	"sync/atomic"
)

type RecordGenerator interface {
	GetRecord() []*aerospike.Bin
}

type PooledRecordGenerator struct {
	Size     int64
	Capacity int64
	Records  [][]*aerospike.Bin
	Model    *DataModel
}

func NewPooledRecordGenerator(model *DataModel, n int64) *PooledRecordGenerator {
	g := &PooledRecordGenerator{
		Size:     0,
		Capacity: n,
		Records:  make([][]*aerospike.Bin, n),
		Model:    model,
	}
	return g
}

func (g *PooledRecordGenerator) generate() {
	var i int64
	b := len(g.Model.Bins)
	for i = 0; i < g.Capacity; i++ {
		g.Records[i] = make([]*aerospike.Bin, b)
		for j, c := range g.Model.Bins {
			g.Records[i][j] = aerospike.NewBin(c.Name, GenerateValue(&c.Value))
		}
		atomic.AddInt64(&g.Size, 1)
	}
}

func (g *PooledRecordGenerator) GetRecord() []*aerospike.Bin {
	n := g.Size
	if n > 0 {
		i := rand.Int63() % n
		return g.Records[i]
	} else {
		return nil
	}
}
