package main

import (
	as "github.com/aerospike/aerospike-client-go"
	"sync/atomic"
)

type KeyGenerator interface {
	GenerateKey() *as.Key
}

type PooledKeyGenerator struct {
	Size     int64
	Capacity int64
	Keys     []*as.Key
	Load     *LoadModel
	Data     *DataModel
}

func NewPooledKeyGenerator(load *LoadModel, data *DataModel) *PooledKeyGenerator {
	n := int64(load.Keys)
	g := &PooledKeyGenerator{
		Size:     0,
		Capacity: n,
		Keys:     make([]*as.Key, n),
		Load:     load,
		Data:     data,
	}
	go g.generate()
	return g
}

func (g *PooledKeyGenerator) generate() {
	var i int64
	for i = 0; i < g.Capacity; i++ {
		atomic.AddInt64(&g.Size, 1)
		g.Keys[i] = GenerateKey(&g.Data.Keys)
	}
}

func (g *PooledKeyGenerator) GenerateKey() *as.Key {
	n := int64(atomic.LoadInt64(&g.Size))
	if n > 0 {
		i := RANDOM.Int63n(n)
		return g.Keys[i]
	}
	return nil
}
