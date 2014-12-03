package main

import (
	"github.com/aerospike/aerospike-client-go"
)

type OpGenerator struct {
	Client  *aerospike.Client
	Keys    KeyGenerator
	Records RecordGenerator
}

func NewOpGenerator(client *aerospike.Client, keys KeyGenerator, records RecordGenerator) *OpGenerator {
	g := &OpGenerator{
		Client:  client,
		Keys:    keys,
		Records: records,
	}
	return g
}

func (g *OpGenerator) GenerateGet() func() {

	var err error
	policy := aerospike.NewPolicy()

	return func() {
		if k := g.Keys.GenerateKey(); k != nil {
			_, err = g.Client.Get(policy, k)
			atomicStat(OPGET, err)
		}
	}
}

func (g *OpGenerator) GeneratePut() func() {

	var err error
	policy := aerospike.NewWritePolicy(0, 0)
	policy.SendKey = true

	return func() {
		if k := g.Keys.GenerateKey(); k != nil {
			if b := g.Records.GenerateRecord(); b != nil {
				err = g.Client.PutBins(policy, k, b...)
				atomicStat(OPPUT, err)
			}
		}
	}
}

func (g *OpGenerator) GenerateDelete() func() {

	var err error
	policy := aerospike.NewWritePolicy(0, 0)

	return func() {
		if k := g.Keys.GenerateKey(); k != nil {
			_, err = g.Client.Delete(policy, k)
			atomicStat(OPDELETE, err)
		}
	}
}