package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/aerospike/aerospike-client-go"
)

// allKeys keeps track of already inserted keys
// It is used in scan and query operations to verify results
type KeySet struct {
	allKeys      map[string]*aerospike.Record
	allKeysMutex sync.RWMutex
	maxSize      int
}

var keySet *KeySet

func NewKeySet(maxSize int) *KeySet {
	return &KeySet{
		allKeys: make(map[string]*aerospike.Record, maxSize),
		maxSize: maxSize,
	}
}

func (ks *KeySet) AddKey(key *aerospike.Key, rec *aerospike.Record) {
	ks.allKeysMutex.Lock()
	defer ks.allKeysMutex.Unlock()
	ks.allKeys[string(key.Digest())] = rec
}

func (ks *KeySet) GetKey(key *aerospike.Key) *aerospike.Record {
	ks.allKeysMutex.RLock()
	defer ks.allKeysMutex.RUnlock()

	return ks.allKeys[string(key.Digest())]
}

func (ks *KeySet) GetRandomKey() (key *aerospike.Key) {
	ks.allKeysMutex.RLock()
	defer ks.allKeysMutex.RUnlock()

	for _, rec := range ks.allKeys {
		key = rec.Key
		if key == nil {
			panic("key is nil")
		}
		break
	}
	return key
}

func (ks *KeySet) DropKey(key *aerospike.Key) {
	ks.allKeysMutex.Lock()
	defer ks.allKeysMutex.Unlock()
	delete(ks.allKeys, string(key.Digest()))
}

func (ks *KeySet) IsFull() bool {
	ks.allKeysMutex.RLock()
	defer ks.allKeysMutex.RUnlock()
	return len(ks.allKeys) >= ks.maxSize
}

func (ks *KeySet) Copy(verify bool) map[string]*aerospike.Record {
	ks.allKeysMutex.RLock()
	defer ks.allKeysMutex.RUnlock()

	if !verify {
		return nil
	}

	newMap := make(map[string]*aerospike.Record, len(ks.allKeys))
	for k, v := range ks.allKeys {
		newMap[k] = v
	}
	return newMap
}

func (ks *KeySet) PrintKeys(key *aerospike.Key) {
	buffer := bytes.Buffer{}

	ks.allKeysMutex.RLock()
	defer ks.allKeysMutex.RUnlock()

	buffer.WriteString(fmt.Sprintf("%v => %#v\t\t ", key, ks.allKeys[string(key.Digest())]))
	for k, _ := range ks.allKeys {
		buffer.WriteString(fmt.Sprintf("%v, ", k))
	}
	log.Println(buffer.String())
}
