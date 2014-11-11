package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/aerospike/aerospike-client-go"
)

const (
	ID         = "Id"
	OPERATIONS = "Operations"
	LOAD       = "Load"

	OPTYPE     = "OpType"
	OPGET      = "Get"
	OPPUT      = "Put"
	OPDELETE   = "Delete"
	OPSCAN     = "Scan"
	OPQUERY    = "Query"
	OPEXEC_UDF = "ExecUDF"

	NAMESPACE = "Namespace"
	SET       = "Set"

	KEY  = "Key"
	BINS = "Bins"
	NAME = "Name"

	USE_EXISTING        = "UseExisting"
	VERIFY              = "Verify"
	PERCENT             = "Percent"
	WAIT_FOR_MIGRATIONS = "WaitForMigrations"

	VALTYPE = "Type"
	// VALRANGE = "range"
	VALMIN   = "Min"
	VALMAX   = "Max"
	VALCONST = "Val"

	STATEMENT    = "Statement"
	FILTER_EQ    = "Equal"
	FILTER_RANGE = "Range"
	BIN_NAME     = "BinName"
	CREATE_INDEX = "CreateIndex"

	PACKAGE_NAME = "Package"
	FUNC_NAME    = "Function"
	ARGS         = "Args"
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
	case OPEXEC_UDF:
		return makeExecUDF(client, op)
	}

	return nil
}

func binBuilder(binDesc map[interface{}]interface{}) func() *aerospike.Bin {
	name := readOption(binDesc, NAME, "").(string)
	binType := readOption(binDesc, VALTYPE, nil).(string)
	binMin := readOption(binDesc, VALMIN, nil).(int)
	binMax := readOption(binDesc, VALMAX, nil).(int)

	valueBuilder := NewValueBuilder(binType, binMin, binMax)

	return func() *aerospike.Bin {
		return aerospike.NewBin(name, valueBuilder())
	}
}

func keyBuilder(ns, set string, keyDesc map[interface{}]interface{}) func() *aerospike.Key {
	keyType := keyDesc[VALTYPE].(string)
	keyMin := readOption(keyDesc, VALMIN, nil).(int)
	keyMax := readOption(keyDesc, VALMAX, nil).(int)
	valueBuilder := NewValueBuilder(keyType, keyMin, keyMax)

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
	createIndex := readOption(op, CREATE_INDEX, false).(bool)

	verify := readOption(op, VERIFY, false).(bool)
	policy := aerospike.NewQueryPolicy()
	policy.WaitUntilMigrationsAreOver = readOption(op, WAIT_FOR_MIGRATIONS, false).(bool)

	statementDesc := op[STATEMENT].(map[interface{}]interface{})
	statement := aerospike.NewStatement(namespace, set)
	rangeFilter := readOption(statementDesc, FILTER_RANGE, []interface{}{}).([]interface{})
	binName := readOption(statementDesc, BIN_NAME, nil).(string)

	if len(rangeFilter) == 2 {
		statement.Addfilter(aerospike.NewRangeFilter(binName, int64(rangeFilter[0].(int)), int64(rangeFilter[1].(int))))

		if createIndex {
			client.CreateIndex(nil, namespace, set, set+binName, binName, aerospike.NUMERIC)
		}
	} else {
		eqFilter := readOption(statementDesc, FILTER_EQ, nil)
		statement.Addfilter(aerospike.NewEqualFilter(binName, eqFilter))

		indexType := aerospike.NUMERIC
		switch eqFilter.(type) {
		case string:
			indexType = aerospike.STRING
		}

		if createIndex {
			client.CreateIndex(nil, namespace, set, set+binName, binName, indexType)
		}
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

func makeExecUDF(client *aerospike.Client, op map[interface{}]interface{}) func() {
	packageName := readOption(op, PACKAGE_NAME, nil).(string)
	funcName := readOption(op, FUNC_NAME, nil).(string)
	args := readOption(op, ARGS, []interface{}{}).([]interface{})

	namespace := readOption(op, NAMESPACE, nil).(string)
	set := readOption(op, SET, "").(string)
	keybuilder := keyBuilder(namespace, set, op[KEY].(map[interface{}]interface{}))

	var err error
	policy := aerospike.NewWritePolicy(0, 0)

	parameters := []aerospike.Value{}
	for param := range args {
		parameters = append(parameters, aerospike.NewValue(param))
	}

	return func() {
		// key will be nil if existing only is requested and there are
		// no existing keys yet
		if k := keybuilder(); k != nil {
			_, err = client.Execute(policy, k, packageName, funcName, parameters...)
			if err != nil {
				log.Println(err.Error())
			}
			atomicStat(OPEXEC_UDF, err)
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

func createDefaultUDF(client *aerospike.Client) {
	const udfBody = `function lgTestFunc1(rec, div, str)
	   local ret = map()                     -- Initialize the return value (a map)

	   local x = rec['bin2']                 -- Get the value from record bin named "bin1"

	   rec['bin2'] = x               -- Set the value in record bin named "bin2"

	   aerospike:update(rec)                 -- Update the main record

	   ret['status'] = 'OK'                   -- Populate the return status
	   ret['value'] = (0 / div)                   -- Populate the return status
	   return ret                             -- Return the Return value and/or status
	end`

	regTask, err := client.RegisterUDF(nil, []byte(udfBody), "loadGenTest.lua", aerospike.LUA)
	panicOnError(err)

	// wait until UDF is created
	err = <-regTask.OnComplete()
	panicOnError(err)
}
