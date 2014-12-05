package main

import (
	"github.com/aerospike/aerospike-client-go"
	"math/rand"
	"sync/atomic"
)

type RecordGenerator interface {
	GenerateRecord() []*aerospike.Bin
}

type PooledRecordGenerator struct {
	Size     int64
	Capacity int64
	Records  [][]*aerospike.Bin
	Load     *LoadModel
	Data     *DataModel
}

func NewPooledRecordGenerator(load *LoadModel, data *DataModel) *PooledRecordGenerator {
	n := int64(load.Keys)
	g := &PooledRecordGenerator{
		Size:     0,
		Capacity: n,
		Records:  make([][]*aerospike.Bin, n),
		Load:     load,
		Data:     data,
	}
	go g.generate()
	return g
}

func (g *PooledRecordGenerator) generate() {
	var i int64
	b := len(g.Data.Bins)
	for i = 0; i < g.Capacity; i++ {
		g.Records[i] = make([]*aerospike.Bin, b)
		for j, c := range g.Data.Bins {
			g.Records[i][j] = aerospike.NewBin(c.Name, GenerateValue(&c.Value))
		}
		atomic.AddInt64(&g.Size, 1)
	}
}

func (g *PooledRecordGenerator) GenerateRecord() []*aerospike.Bin {
	n := g.Size
	if n > 0 {
		i := rand.Int63() % n
		return g.Records[i]
	} else {
		return nil
	}
}
