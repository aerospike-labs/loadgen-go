package main

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"strings"
)

var (
	INDENT_PREFIX    string = " "
	INDENT_INCREMENT int    = 3
)

func dumpIntegerConstraints(c *IntegerConstraints, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("IntegerConstraints {\n")
	out += fmt.Sprintf("%s    Min: %d\n", prefix, c.Min)
	out += fmt.Sprintf("%s    Max: %d\n", prefix, c.Max)
	out += fmt.Sprintf("%s }", prefix)
	return out
}

func dumpStringConstraints(c *StringConstraints, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("StringConstraints {\n")
	out += fmt.Sprintf("%s    Min: %d\n", prefix, c.Min)
	out += fmt.Sprintf("%s    Max: %d\n", prefix, c.Max)
	out += fmt.Sprintf("%s }", prefix)
	return out
}

func dumpBytesConstraints(c *BytesConstraints, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("BytesConstraints {\n")
	out += fmt.Sprintf("%s    Min: %d\n", prefix, c.Min)
	out += fmt.Sprintf("%s    Max: %d\n", prefix, c.Max)
	out += fmt.Sprintf("%s }", prefix)
	return out
}

func dumpListConstraints(c *ListConstraints, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("ListConstraints {\n")
	out += fmt.Sprintf("%s    Min: %d\n", prefix, c.Min)
	out += fmt.Sprintf("%s    Max: %d\n", prefix, c.Max)
	out += fmt.Sprintf("%s    Value: %s\n", prefix, dumpConstraints(&c.Value, indent+INDENT_INCREMENT))
	out += fmt.Sprintf("%s }", prefix)
	return out
}

func dumpMapConstraints(c *MapConstraints, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("MapConstraints {\n")
	out += fmt.Sprintf("%s    Min: %d\n", prefix, c.Min)
	out += fmt.Sprintf("%s    Max: %d\n", prefix, c.Max)
	out += fmt.Sprintf("%s    Key: %s\n", prefix, dumpConstraints(&c.Key, indent+INDENT_INCREMENT))
	out += fmt.Sprintf("%s    Value: %s\n", prefix, dumpConstraints(&c.Value, indent+INDENT_INCREMENT))
	out += fmt.Sprintf("%s }", prefix)
	return out
}

func dumpConstraints(c *Constraints, indent int) string {
	if c == nil {
		return ""
	} else if c.Integer != nil {
		return dumpIntegerConstraints(c.Integer, indent)
	} else if c.String != nil {
		return dumpStringConstraints(c.String, indent)
	} else if c.Bytes != nil {
		return dumpBytesConstraints(c.Bytes, indent)
	} else if c.List != nil {
		return dumpListConstraints(c.List, indent)
	} else if c.Map != nil {
		return dumpMapConstraints(c.Map, indent)
	}
	return ""
}

func dumpBinConstraints(c *BinConstraints, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("%s BinConstraints {\n", prefix)
	out += fmt.Sprintf("%s    Name: %s\n", prefix, c.Name)
	out += fmt.Sprintf("%s    Value: %s\n", prefix, dumpConstraints(&c.Value, indent+INDENT_INCREMENT))
	out += fmt.Sprintf("%s    Optional: %v\n", prefix, c.Optional)
	out += fmt.Sprintf("%s    Indexed: %v\n", prefix, c.Indexed)
	out += fmt.Sprintf("%s }\n", prefix)
	return out
}

func dumpBin(b *as.Bin, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("%s Bin {\n", prefix)
	out += fmt.Sprintf("%s    Name: %s\n", prefix, b.Name)
	out += fmt.Sprintf("%s    Value: %#v\n", prefix, b.Value)
	out += fmt.Sprintf("%s }\n", prefix)
	return out
}

func dumpBins(bs []as.Bin, indent int) string {
	out := ""
	for _, b := range bs {
		out += dumpBin(&b, indent)
	}
	return out
}

func dumpKey(k *as.Key, indent int) string {
	prefix := strings.Repeat(INDENT_PREFIX, indent)
	out := ""
	out += fmt.Sprintf("%s Key {\n", prefix)
	out += fmt.Sprintf("%s    Namespace: %s\n", prefix, k.Namespace())
	out += fmt.Sprintf("%s    SetName: %s\n", prefix, k.SetName())
	out += fmt.Sprintf("%s    Value: %s\n", prefix, k.Value())
	out += fmt.Sprintf("%s    Digest: %#v\n", prefix, k.Digest())
	out += fmt.Sprintf("%s }\n", prefix)
	return out
}

func dumpKeys(ks []as.Key, indent int) string {
	out := ""
	for _, k := range ks {
		out += dumpKey(&k, indent)
	}
	return out
}
