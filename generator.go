package main

import (
	// "fmt"
	as "github.com/aerospike/aerospike-client-go"
	// "math"
	"math/rand"
	"time"
)

var (
	RANDOM                  = rand.New(rand.NewSource(time.Now().UnixNano()))
	GENERATOR_CHARSET_ALPHA = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func randomInRange(min int64, max int64) int64 {
	if min == max {
		return min
	} else if min < max {
		var i int64 = 0
		for i = RANDOM.Int63n(max); i < min; i = RANDOM.Int63n(max) {
		}
		return i
	}
	return max
}

func GenerateInteger(c *IntegerConstraints) int64 {
	return randomInRange(c.Min, c.Max)
}

func generateString(min int64, max int64) string {
	n := randomInRange(min, max)
	b := make([]rune, n)
	for i := range b {
		b[i] = GENERATOR_CHARSET_ALPHA[RANDOM.Intn(len(GENERATOR_CHARSET_ALPHA))]
	}
	return string(b)
}

func GenerateString(c *StringConstraints) string {
	return generateString(c.Min, c.Max)
}

func GenerateBytes(c *BytesConstraints) []byte {
	n := randomInRange(c.Min, c.Max)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(RANDOM.Intn(256))
	}
	return b
}

func GenerateList(c *ListConstraints) []interface{} {
	n := randomInRange(c.Min, c.Max)
	l := make([]interface{}, n)
	for i := range l {
		v := GenerateValue(&c.Value)
		l[i] = v
	}
	return l
}

func GenerateMap(c *MapConstraints) map[string]interface{} {
	n := randomInRange(c.Min, c.Max)
	m := make(map[string]interface{}, n)
	for _ = range m {
		k := generateString(c.Min, c.Max)
		v := GenerateValue(&c.Value)
		m[k] = v
	}
	return m
}

func GenerateValue(c *Constraints) interface{} {
	if c.Integer != nil {
		return GenerateInteger(c.Integer)
	} else if c.String != nil {
		return GenerateString(c.String)
	} else if c.Bytes != nil {
		return GenerateBytes(c.Bytes)
	} else if c.List != nil {
		return GenerateList(c.List)
	} else if c.Map != nil {
		return GenerateMap(c.Map)
	}
	return nil
}

func GenerateBin(c *BinConstraints) *as.Bin {
	b := as.NewBin(c.Name, GenerateValue(&c.Value))
	return b
}

func GenerateBins(l BinConstraintsList) []*as.Bin {
	bins := make([]*as.Bin, len(l))
	for i, c := range l {
		bin := GenerateBin(c)
		bins[i] = bin
	}
	return bins
}

func GenerateKey(c *KeyConstraints) *as.Key {
	key, _ := as.NewKey(c.Namespace, c.Set, GenerateValue(&c.Key))
	return key
}

func GenerateKeys(c *KeyConstraints, n int) []as.Key {
	keys := make([]as.Key, n)
	for i := range keys {
		key, _ := as.NewKey(c.Namespace, c.Set, GenerateValue(&c.Key))
		keys[i] = *key
	}
	return keys
}
