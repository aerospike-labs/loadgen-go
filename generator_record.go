package main

import (
	as "github.com/aerospike/aerospike-client-go"
	"sync/atomic"
)

type RecordGenerator interface {
	GenerateRecord() []as.Bin
}

type PooledRecordGenerator struct {
	Size     int64
	Capacity int64
	Records  [][]as.Bin
	Load     *LoadModel
	Data     *DataModel
}

func NewPooledRecordGenerator(load *LoadModel, data *DataModel) *PooledRecordGenerator {
	n := int64(load.Keys)
	g := &PooledRecordGenerator{
		Size:     0,
		Capacity: n,
		Records:  make([][]as.Bin, n),
		Load:     load,
		Data:     data,
	}
	go g.generate()
	return g
}

func (g *PooledRecordGenerator) generate() {
	var i int64
	for i = 0; i < g.Capacity; i++ {
		atomic.AddInt64(&g.Size, 1)
		g.Records[i] = GenerateBins(g.Data.Bins)
	}
}

func (g *PooledRecordGenerator) GenerateRecord() []as.Bin {
	n := atomic.LoadInt64(&g.Size)
	if n > 0 {
		i := RANDOM.Int63n(n)
		return g.Records[i]
	}
	return nil
}
