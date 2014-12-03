package main

import (
	"github.com/aerospike/aerospike-client-go"
	"math/rand"
	"sync/atomic"
)

type KeyGenerator interface {
	GenerateKey() *aerospike.Key
}

type PooledKeyGenerator struct {
	Size     int64
	Capacity int64
	Keys     []*aerospike.Key
	Load     *LoadModel
	Data     *DataModel
}

func NewPooledKeyGenerator(load *LoadModel, data *DataModel) *PooledKeyGenerator {
	n := int64(load.Keys)
	g := &PooledKeyGenerator{
		Size:     0,
		Capacity: n,
		Keys:     make([]*aerospike.Key, n),
		Load:     load,
		Data:     data,
	}
	go g.generate()
	return g
}

func (g *PooledKeyGenerator) generate() {
	var i int64
	for i = 0; i < g.Capacity; i++ {
		if key, err := aerospike.NewKey(g.Data.Keys.Namespace, g.Data.Keys.Set, GenerateValue(&g.Data.Keys.Key)); err == nil {
			g.Keys[i] = key
			atomic.AddInt64(&g.Size, 1)
		}
	}
}

func (g *PooledKeyGenerator) GenerateKey() *aerospike.Key {
	n := atomic.LoadInt64(&g.Size)
	if n > 0 {
		i := rand.Int63() % n
		return g.Keys[i]
	} else {
		return nil
	}
}
