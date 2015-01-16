package main

import (
	"fmt"
	"github.com/aerospike/aerospike-client-go"
)

func ReadGenerator(client *aerospike.Client, keys KeyGenerator) func() {

	var err error
	policy := aerospike.NewPolicy()

	return func() {
		if k := keys.GetKey(); k != nil {
			fmt.Printf("fee %v\n", k.Value())
			_, err = client.Get(policy, k)
			statUpdate(&CURRENT_STATS.Reads, err)
		}
	}
}

func WriteGenerator(client *aerospike.Client, keys KeyGenerator, records RecordGenerator) func() {

	var err error
	policy := aerospike.NewWritePolicy(0, 0)
	policy.SendKey = true

	return func() {
		if k := keys.GetKey(); k != nil {
			if b := records.GetRecord(); b != nil {
				err = client.PutBins(policy, k, b...)
				statUpdate(&CURRENT_STATS.Writes, err)
			}
		}
	}
}
