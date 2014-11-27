package main

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"math"
	"math/rand"
)

var (
	GENERATOR_RAND          = rand.New(rand.NewSource(math.MaxInt64))
	GENERATOR_CHARSET_ALPHA = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func randomIn(min int, max int) int {
	i := GENERATOR_RAND.Intn(min + max + 1)
	if i > min {
		return i - min
	}
	return i
}

func GenerateInteger(c *IntegerConstraints) int {
	return randomIn(int(c.Min), int(c.Max))
}

func generateString(min int, max int) string {
	n := randomIn(min, max)
	b := make([]rune, n)
	for i := range b {
		b[i] = GENERATOR_CHARSET_ALPHA[GENERATOR_RAND.Intn(len(GENERATOR_CHARSET_ALPHA))]
	}
	return string(b)
}

func GenerateString(c *StringConstraints) string {
	return generateString(int(c.Min), int(c.Max))
}

func GenerateBytes(c *BytesConstraints) []byte {
	n := randomIn(int(c.Min), int(c.Max))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(GENERATOR_RAND.Intn(256))
	}
	return b
}

func GenerateList(c *ListConstraints) []interface{} {
	n := randomIn(int(c.Min), int(c.Max))
	l := make([]interface{}, n)
	for i := range l {
		v := GenerateValue(&c.Value)
		l[i] = v
	}
	return l
}

func GenerateMap(c *MapConstraints) map[string]interface{} {
	n := randomIn(int(c.Min), int(c.Max))
	m := make(map[string]interface{}, n)
	for _ = range m {
		k := generateString(int(c.Min), int(c.Max))
		v := GenerateValue(&c.Value)
		m[k] = v
	}
	return m
}

func GenerateValue(c *Constraints) interface{} {
	fmt.Printf("c := %#v\n", c)
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
	fmt.Printf("c := %s\n", c.Name)
	return as.NewBin(c.Name, GenerateValue(&c.Value))
}

func GenerateRecord(l BinConstraintsList) []as.Bin {
	bins := make([]as.Bin, len(l))
	for i, c := range l {
		bin := GenerateBin(c)
		bins[i] = *bin
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
