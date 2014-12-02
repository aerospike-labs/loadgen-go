package main

import (
	as "github.com/aerospike/aerospike-client-go"
)

type LoadGenerator struct {
	Keys    KeyGenerator
	Records RecordGenerator
	Client  *as.Client
	Model   *LoadModel
	Data    *DataModel
	Done    chan struct{}
}

func NewLoadGenerator(model *LoadModel, keys KeyGenerator, records RecordGenerator, client *as.Client) *LoadGenerator {
	return &LoadGenerator{
		Model:   model,
		Keys:    keys,
		Records: records,
		Client:  client,
		Done:    make(chan struct{}),
	}
}

func (g *LoadGenerator) generate() {
	for {
		k := g.Keys.GenerateKey()
		if k != nil {
			println(dumpKey(k, 0))
		}
	}
}

func (g *LoadGenerator) Start() {
	go g.generate()
}

func (g *LoadGenerator) Stop() {
}

func (g *LoadGenerator) Wait() {
	<-g.Done
}
