package main

import (
	"github.com/aerospike/aerospike-client-go"
)

func ReadGenerator(client *aerospike.Client, keys KeyGenerator) func() {

	var err error
	policy := aerospike.NewPolicy()

	return func() {
		if k := keys.GenerateKey(); k != nil {
			_, err = client.Get(policy, k)
			statUpdate(&CURRENT_STATS.Writes, err)
		}
	}
}

func WriteGenerator(client *aerospike.Client, keys KeyGenerator, records RecordGenerator) func() {

	var err error
	policy := aerospike.NewWritePolicy(0, 0)
	policy.SendKey = true

	return func() {
		if k := keys.GenerateKey(); k != nil {
			if b := records.GenerateRecord(); b != nil {
				err = client.PutBins(policy, k, b...)
				statUpdate(&CURRENT_STATS.Reads, err)
			}
		}
	}
}
