package main

import (
	"errors"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
)

// ----------------------------------------------------------------------------
//
// Types
//
// ----------------------------------------------------------------------------

var (
	ErrModelNotFound  = errors.New("Model not found")
	ErrModelFound     = errors.New("Model found")
	ErrModelIdInvalid = errors.New("Model Id invalid")
)

type IntegerConstraints struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

type StringConstraints struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

type BytesConstraints struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

type ListConstraints struct {
	Min   int64       `json:"min"`
	Max   int64       `json:"max"`
	Value Constraints `json:"value"`
}

type MapConstraints struct {
	Min   int64       `json:"min"`
	Max   int64       `json:"max"`
	Key   Constraints `json:"key,omitempty"`
	Value Constraints `json:"value,omitempty"`
}

type Constraints struct {
	Integer *IntegerConstraints `json:"integer,omitempty"`
	String  *StringConstraints  `json:"string,omitempty"`
	Bytes   *BytesConstraints   `json:"bytes,omitempty"`
	List    *ListConstraints    `json:"list,omitempty"`
	Map     *MapConstraints     `json:"map,omitempty"`
}

type BinConstraints struct {
	Name     string      `json:"name"`
	Value    Constraints `json:"value"`
	Optional bool        `json:"optional,omitempty"`
	Indexed  bool        `json:"indexed,omitempty"`
}

type KeyConstraints struct {
	Namespace string      `json:"namespace"`
	Set       string      `json:"set"`
	Key       Constraints `json:"key"`
}

type DataModel struct {
	Keys KeyConstraints    `json:"keys"`
	Bins []*BinConstraints `json:"bins"`
}

type LoadModel struct {
	Keys    int64 `json:"keys"`
	Reads   int64 `json:"reads"`
	Writes  int64 `json:"writes"`
	Deletes int64 `json:"deletes"`
	Queries int64 `json:"queries"`
	Scans   int64 `json:"scans"`
}

type HostSpec struct {
	Addr string `json:"addr"`
	Port int    `json:"port"`
}

type Config struct {
	Hosts     []HostSpec `json:"hosts" yml:"hosts"`
	LoadModel LoadModel  `json:"load" yml:"load"`
	DataModel DataModel  `json:"data" yml:"data"`
}

// ----------------------------------------------------------------------------
//
// Models Methods
//
// ----------------------------------------------------------------------------

func NewConfig() *Config {
	return &Config{
		Hosts:     []HostSpec{},
		LoadModel: LoadModel{},
		DataModel: DataModel{},
	}
}

func (c *Config) Load(filepath string) error {

	var err error

	raw, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(raw, c)
	if err != nil {
		return err
	}

	return nil
}
