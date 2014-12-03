package main

import (
	// "fmt"
	as "github.com/aerospike/aerospike-client-go"
	// "math"
	"math/rand"
	// "time"
	"strconv"
	"strings"
)

var (
	// RANDOM                  = rand.New(rand.NewSource(time.Now().UnixNano()))
	GENERATOR_CHARSET_ALPHA = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func randomInRange(min int64, max int64) int64 {
	if min == max {
		return min
	} else if min < max {
		var i int64 = 0
		for i = rand.Int63n(max); i < min; i = rand.Int63n(max) {
		}
		return i
	}
	return max
}

func GenerateInteger(c *IntegerConstraints) int64 {
	return randomInRange(c.Min, c.Max)
}

func GenerateIntegerSeed(c *IntegerConstraints, seed int64) int64 {
	return c.Min + seed
}

func generateString(min int64, max int64) string {
	n := randomInRange(min, max)
	b := make([]rune, n)
	for i := range b {
		j := rand.Int() % len(GENERATOR_CHARSET_ALPHA)
		b[i] = GENERATOR_CHARSET_ALPHA[j]
	}
	return string(b)
}

func GenerateString(c *StringConstraints) string {
	return generateString(c.Min, c.Max)
}

func generateStringSeed(min int64, max int64, seed int64) string {
	s := strconv.FormatInt(seed, 16)
	l := int64(len(s))
	if l < min {
		return strings.Repeat("0", int(min-l)) + s
	} else if l > max {
		return s[0:max]
	} else {
		return s
	}
}

func GenerateStringSeed(c *StringConstraints, seed int64) string {
	return generateStringSeed(c.Min, c.Max, seed)
}

func GenerateBytes(c *BytesConstraints) []byte {
	n := randomInRange(c.Min, c.Max)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(256))
	}
	return b
}

func GenerateBytesSeed(c *BytesConstraints, seed int64) []byte {
	s := generateStringSeed(c.Min, c.Max, seed)
	return []byte(s)
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

func GenerateListSeed(c *ListConstraints, seed int64) []interface{} {
	n := c.Min + seed
	if n > c.Max {
		n = c.Max
	}

	l := make([]interface{}, n)
	for i := range l {
		v := GenerateValueSeed(&c.Value, seed)
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

func GenerateMapSeed(c *MapConstraints, seed int64) map[string]interface{} {
	n := c.Min + seed
	if n > c.Max {
		n = c.Max
	}
	m := make(map[string]interface{}, n)
	for _ = range m {
		k := generateString(c.Min, c.Max)
		v := GenerateValueSeed(&c.Value, seed)
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

func GenerateValueSeed(c *Constraints, seed int64) interface{} {
	if c.Integer != nil {
		return GenerateIntegerSeed(c.Integer, seed)
	} else if c.String != nil {
		return GenerateStringSeed(c.String, seed)
	} else if c.Bytes != nil {
		return GenerateBytesSeed(c.Bytes, seed)
	} else if c.List != nil {
		return GenerateListSeed(c.List, seed)
	} else if c.Map != nil {
		return GenerateMapSeed(c.Map, seed)
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
