package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/aerospike/aerospike-client-go"
)

const (
	OPERATIONS = "operations"
	LOAD       = "load"

	OPTYPE   = "optype"
	OPGET    = "get"
	OPPUT    = "put"
	OPDELETE = "delete"
	OPSCAN   = "scan"
	OPQUERY  = "query"

	NAMESPACE = "namespace"
	SET       = "set"

	KEY  = "key"
	BINS = "bins"
	NAME = "name"

	USE_EXISTING        = "use_existing"
	VERIFY              = "verify"
	PERCENT             = "percent"
	WAIT_FOR_MIGRATIONS = "wait_for_migrations"

	VALTYPE  = "type"
	VALRANGE = "range"

	STATEMENT    = "statement"
	FILTER_EQ    = "equal"
	FILTER_RANGE = "range"
	BIN_NAME     = "bin_name"
)

func makeOp(client *aerospike.Client, op map[interface{}]interface{}) func() {
	opType := readOption(op, OPTYPE, nil).(string)
	switch opType {
	case OPGET:
		return makeGet(client, op)
	case OPPUT:
		return makePut(client, op)
	case OPDELETE:
		return makeDelete(client, op)
	case OPSCAN:
		return makeScan(client, op)
	case OPQUERY:
		return makeQuery(client, op)
	}

	return nil
}

func binBuilder(binDesc map[interface{}]interface{}) func() *aerospike.Bin {
	name := readOption(binDesc, NAME, "").(string)
	binType := readOption(binDesc, VALTYPE, nil).(string)
	binRange := readOption(binDesc, VALRANGE, nil).([]interface{})
	if len(binRange) != 2 {
		log.Fatalf("range values should be an array with exactly 2 elemets in `%v`", binDesc)
	}
	valueBuilder := NewValueBuilder(binType, binRange[0].(int), binRange[1].(int))

	return func() *aerospike.Bin {
		return aerospike.NewBin(name, valueBuilder())
	}
}

func keyBuilder(ns, set string, keyDesc map[interface{}]interface{}) func() *aerospike.Key {
	keyType := keyDesc[VALTYPE].(string)
	keyRange := keyDesc[VALRANGE].([]interface{})
	valueBuilder := NewValueBuilder(keyType, keyRange[0].(int), keyRange[1].(int))

	use_existing := readOption(keyDesc, USE_EXISTING, false).(bool)
	var key *aerospike.Key
	var err error

	return func() *aerospike.Key {
		// if we have already created enough keys, use an existing key
		use_existing = keySet.IsFull()
		key = nil

		if use_existing {
			// map access is randomized in go per specification of the language
			key = keySet.GetRandomKey()
		} else {
			key, err = aerospike.NewKey(ns, set, valueBuilder())
			if err != nil {
				panic(err)
			}
		}

		return key
	}
}

func makeGet(client *aerospike.Client, op map[interface{}]interface{}) func() {
	namespace := readOption(op, NAMESPACE, nil).(string)
	set := readOption(op, SET, "").(string)
	keybuilder := keyBuilder(namespace, set, op[KEY].(map[interface{}]interface{}))

	var err error
	policy := aerospike.NewPolicy()

	return func() {
		// key will be nil if existing only is requested and there are
		// no existing keys yet
		if k := keybuilder(); k != nil {
			_, err = client.Get(policy, k)
			atomicStat(OPGET, err)
		}
	}
}

func makePut(client *aerospike.Client, op map[interface{}]interface{}) func() {
	namespace := readOption(op, NAMESPACE, nil).(string)
	set := readOption(op, SET, "").(string)
	keybuilder := keyBuilder(namespace, set, op[KEY].(map[interface{}]interface{}))

	binsDesc := op[BINS].([]interface{})
	binCount := len(binsDesc)

	binBuilders := make([]func() *aerospike.Bin, binCount)
	for i, binDesc := range binsDesc {
		binBuilders[i] = binBuilder(binDesc.(map[interface{}]interface{}))
	}

	bins := make([]*aerospike.Bin, binCount)

	var err error
	policy := aerospike.NewWritePolicy(0, 0)
	policy.SendKey = true

	return func() {
		if k := keybuilder(); k != nil {

			binMap := make(aerospike.BinMap, binCount)
			for i := 0; i < binCount; i++ {
				bins[i] = binBuilders[i]()
				binMap[bins[i].Name] = bins[i].Value
			}

			err = client.PutBins(policy, k, bins...)
			atomicStat(OPPUT, err)

			if err == nil {
				keySet.AddKey(k, &aerospike.Record{Key: k, Bins: binMap})
			}
		}
	}
}

func makeDelete(client *aerospike.Client, op map[interface{}]interface{}) func() {
	namespace := readOption(op, NAMESPACE, nil).(string)
	set := readOption(op, SET, "").(string)
	keybuilder := keyBuilder(namespace, set, op[KEY].(map[interface{}]interface{}))

	var err error
	policy := aerospike.NewWritePolicy(0, 0)
	return func() {
		k := keybuilder()
		if k == nil {
			return
		}
		_, err = client.Delete(policy, k)
		atomicStat(OPDELETE, err)
		if err != nil {
			keySet.DropKey(k)
		}
	}
}

func makeScan(client *aerospike.Client, op map[interface{}]interface{}) func() {
	namespace := readOption(op, NAMESPACE, nil).(string)
	set := readOption(op, SET, "").(string)

	verify := readOption(op, VERIFY, false).(bool)
	policy := aerospike.NewScanPolicy()
	policy.ScanPercent = op[PERCENT].(int)
	policy.WaitUntilMigrationsAreOver = readOption(op, WAIT_FOR_MIGRATIONS, false).(bool)

	var origRec *aerospike.Record

	return func() {
		scanResult, err := client.ScanAll(policy, namespace, set)
		atomicStat(OPSCAN, err)

		if err != nil {
			fmt.Errorf("%v", err)
			return
		}

		for rec := range scanResult.Records {
			if verify {
				// check for bin values
				origRec = nil
				if rec.Key != nil && rec.Key != nil {
					origRec = keySet.GetKey(rec.Key)
				}

				if rec != nil && origRec != nil && rec.Bins != nil && origRec.Bins != nil && !binMapsEqual(rec.Bins, origRec.Bins) {
					atomicStat(OPSCAN, errors.New("Wrong scan result"))
					continue
				}
			}
		}

		for err := range scanResult.Errors {
			atomicStat(OPSCAN, err)
		}
	}
}

func makeQuery(client *aerospike.Client, op map[interface{}]interface{}) func() {
	namespace := readOption(op, NAMESPACE, nil).(string)
	set := readOption(op, SET, "").(string)

	verify := readOption(op, VERIFY, false).(bool)
	policy := aerospike.NewQueryPolicy()
	policy.WaitUntilMigrationsAreOver = readOption(op, WAIT_FOR_MIGRATIONS, false).(bool)

	statementDesc := op[STATEMENT].(map[interface{}]interface{})
	statement := aerospike.NewStatement(namespace, set)
	rangeFilter := readOption(statementDesc, FILTER_RANGE, []int{}).([]interface{})
	binName := readOption(statementDesc, BIN_NAME, nil).(string)

	if len(rangeFilter) == 2 {
		statement.Addfilter(aerospike.NewRangeFilter(binName, int64(rangeFilter[0].(int)), int64(rangeFilter[1].(int))))
	} else {
		eqFilter := readOption(statementDesc, FILTER_EQ, nil)
		statement.Addfilter(aerospike.NewEqualFilter(binName, eqFilter))
	}

	var origRec *aerospike.Record

	return func() {
		queryResult, err := client.Query(policy, statement)
		atomicStat(OPQUERY, err)

		if err != nil {
			fmt.Errorf("%v", err)
			return
		}

		for rec := range queryResult.Records {
			if verify {
				// check for bin values
				origRec = nil
				if rec.Key != nil && rec.Key != nil {
					origRec = keySet.GetKey(rec.Key)
				}

				// if rec != nil && origRec != nil && rec.Bins != nil && origRec.Bins != nil && !binMapsEqual(rec.Bins, origRec.Bins) {
				// 	atomicStat(OPQUERY, errors.New("Wrong Query result"))
				// 	continue
				// }
			}
		}

		for err := range queryResult.Errors {
			atomicStat(OPQUERY, err)
		}
	}
}

// this function will exit with a fatal log if the default is nil and the
// opName doesn't exists in the map
func readOption(ops map[interface{}]interface{}, opName string, defaultValue interface{}) interface{} {
	if v, exists := ops[opName]; exists {
		return v
	}
	if defaultValue == nil {
		log.Fatalf("Expected Value `%s` was not found in `%v`", opName, ops)
	}
	return defaultValue
}

func binMapsEqual(m1, m2 aerospike.BinMap) bool {
	for k1, v1 := range m1 {
		if v2, exists := m2[k1]; exists {
			if v2 != v1 {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func sameValue(v1, v2 aerospike.Value) bool {
	res := false
	if v1 == v2 {
		res = true
	}
	if v1.GetType() == v2.GetType() && v1.GetObject() == v2.GetObject() {
		res = true
	}

	return res
}
