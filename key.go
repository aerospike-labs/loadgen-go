package main

import (
	"github.com/aerospike/aerospike-client-go"
	"math/rand"
	"sync/atomic"
)

type KeyGenerator interface {
	GetKey() *aerospike.Key
}

type PooledKeyGenerator struct {
	Size     int64
	Capacity int64
	Keys     []*aerospike.Key
	Model    *DataModel
}

func NewPooledKeyGenerator(model *DataModel, n int64) *PooledKeyGenerator {
	g := &PooledKeyGenerator{
		Size:     0,
		Capacity: n,
		Keys:     make([]*aerospike.Key, n),
		Model:    model,
	}
	return g
}

func (g *PooledKeyGenerator) generate() {
	var i int64
	for i = 0; i < g.Capacity; i++ {
		if key, err := aerospike.NewKey(g.Model.Keys.Namespace, g.Model.Keys.Set, GenerateValueSeed(&g.Model.Keys.Key, i)); err == nil {
			g.Keys[i] = key
			atomic.AddInt64(&g.Size, 1)
		}
	}
}

func (g *PooledKeyGenerator) GetKey() *aerospike.Key {
	n := g.Size
	if n > 0 {
		i := rand.Int63() % n
		return g.Keys[i]
	} else {
		return nil
	}
}
