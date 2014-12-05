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
	Min int64 `Min`
	Max int64 `Max`
}

type StringConstraints struct {
	Min int64 `Min`
	Max int64 `Max`
}

type BytesConstraints struct {
	Min int64 `Min`
	Max int64 `Max`
}

type ListConstraints struct {
	Min   int64       `Min`
	Max   int64       `Max`
	Value Constraints `Value`
}

type MapConstraints struct {
	Min   int64       `Min`
	Max   int64       `Max`
	Key   Constraints `json:"Key,omitempty" yaml:"Key,omitempty"`
	Value Constraints `json:"Value,omitempty" yaml:"Value,omitempty"`
}

type Constraints struct {
	Integer *IntegerConstraints `json:"Integer,omitempty" yaml:"Integer,omitempty"`
	String  *StringConstraints  `json:"String,omitempty" yaml:"String,omitempty"`
	Bytes   *BytesConstraints   `json:"Bytes,omitempty" yaml:"Bytes,omitempty"`
	List    *ListConstraints    `json:"List,omitempty" yaml:"List,omitempty"`
	Map     *MapConstraints     `json:"Map,omitempty" yaml:"Map,omitempty"`
}

type BinConstraints struct {
	Name     string      `Name`
	Value    Constraints `Value`
	Optional bool        `json:"Optional,omitempty" yaml:"Optional,omitempty"`
	Indexed  bool        `json:"Indexed,omitempty" yaml:"Indexed,omitempty"`
}

type BinConstraintsList []*BinConstraints

type KeyConstraints struct {
	Namespace string      `Namespace`
	Set       string      `Set`
	Key       Constraints `Key`
}

type DataModel struct {
	Id   string             `Id`
	Keys KeyConstraints     `Keys`
	Bins BinConstraintsList `Bins`
}

type DataModelList []*DataModel

type LoadModel struct {
	Id      string `Id`
	Keys    int64  `Keys`
	Reads   int64  `Reads`
	Writes  int64  `Writes`
	Deletes int64  `Deletes`
	Queries int64  `Queries`
	Scans   int64  `Scans`
}

type LoadModelList []*LoadModel

type HostSpec struct {
	Addr string `Addr`
	Port int    `Port`
}

type Config struct {
	Hosts      []HostSpec    `Hosts`
	LoadModels LoadModelList `Load`
	DataModels DataModelList `Data`
}

// ----------------------------------------------------------------------------
//
// Models Methods
//
// ----------------------------------------------------------------------------

func NewConfig() *Config {
	return &Config{
		Hosts:      []HostSpec{},
		LoadModels: LoadModelList{},
		DataModels: DataModelList{},
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
