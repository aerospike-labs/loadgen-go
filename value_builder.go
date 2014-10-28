// from: https://github.com/dustin/randbo/blob/master/randbo.go

package main

import (
	"log"
	"math/rand"
	"time"
)

const (
	TINT    = "integer"
	TSTRING = "string"
	TBYTES  = "bytes"
)

// Randbo creates a stream of non-crypto quality random bytes
type ValueBuilder struct {
	rnd      *rand.Rand
	from, to int
	bytes    []byte
}

// New creates a new random reader with a time source.
func NewValueBuilder(vtype string, from, to int) (f func() interface{}) {
	vb := NewFrom(rand.NewSource(time.Now().UnixNano()), from, to)
	switch vtype {
	case TINT:
		f = vb.ReadInt
	case TSTRING:
		f = vb.ReadString
	case TBYTES:
		f = vb.ReadBytes
	default:
		log.Fatal("key or value type `" + vtype + "` not recognized.")
	}

	return f
}

// NewFrom creates a new reader from your own rand.Source
func NewFrom(src rand.Source, from, to int) *ValueBuilder {
	return &ValueBuilder{
		rand.New(src),
		from,
		to,
		make([]byte, to, to),
	}
}

// Read satisfies io.Reader
func (r *ValueBuilder) ReadBytes() interface{} {
	todo := r.ReadInt().(int)
	totalCount := todo
	offset := 0
	for {
		val := int64(r.rnd.Int63())
		for i := 0; i < 8; i++ {
			r.bytes[offset] = byte(val)
			todo--
			if todo == 0 {
				return r.bytes[:totalCount]
			}
			offset++
			val >>= 8
		}
	}

	return r.bytes[:todo]
}

func (r *ValueBuilder) ReadInt() interface{} {
	var val int
	if r.from == 0 {
		val = r.rnd.Intn(r.to)
	} else {
		val = r.rnd.Intn(r.from) + (r.to - r.from)
	}

	return val
}

func (r *ValueBuilder) ReadString() interface{} {
	return string(r.ReadBytes().([]byte))
}
