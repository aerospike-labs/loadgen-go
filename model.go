package main

import (
	"errors"
	// "fmt"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	// "strings"
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
	Min uint `Min`
	Max uint `Max`
}

type StringConstraints struct {
	Min int `Min`
	Max int `Max`
}

type BytesConstraints struct {
	Min uint `Min`
	Max uint `Max`
}

type ListConstraints struct {
	Min   uint        `Min`
	Max   uint        `Max`
	Value Constraints `Value`
}

type MapConstraints struct {
	Min   uint        `Min`
	Max   uint        `Max`
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
	Keys    uint   `Keys`
	Reads   uint   `Reads`
	Writes  uint   `Writes`
	Deletes uint   `Deletes`
	Queries uint   `Queries`
	Scans   uint   `Scans`
}

type LoadModelList []*LoadModel

type Models struct {
	LoadModels LoadModelList `Load`
	DataModels DataModelList `Data`
}

// ----------------------------------------------------------------------------
//
// Models Methods
//
// ----------------------------------------------------------------------------

func NewModels() *Models {
	return &Models{
		LoadModels: LoadModelList{},
		DataModels: DataModelList{},
	}
}

func (m *Models) Load(filepath string) error {

	var err error

	raw, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(raw, m)
	if err != nil {
		return err
	}

	return nil
}

func (m *Models) Store(filepath string) error {

	var err error

	raw, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filepath, raw, os.FileMode(0644))
	if err != nil {
		return err
	}

	return nil
}

func (m *Models) Marshal() ([]byte, error) {

	raw, err := yaml.Marshal(m)
	if err != nil {
		return nil, err
	}

	return raw, nil
}

// ----------------------------------------------------------------------------
//
// DataModelList Methods
//
// ----------------------------------------------------------------------------

func (l DataModelList) Len() int           { return len(l) }
func (l DataModelList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l DataModelList) Less(i, j int) bool { return l[i].Id < l[j].Id }

// Index of Host by Id
func (l DataModelList) IndexById(id string) (int, error) {

	if len(id) == 0 {
		return -1, ErrModelIdInvalid
	}

	for j, a := range l {
		if a.Id == id {
			return j, nil
			break
		}
	}

	return -1, ErrModelNotFound
}

// Find a Host by Id
func (l DataModelList) FindById(id string) (*DataModel, error) {

	i, err := l.IndexById(id)
	if err != nil {
		return nil, err
	}

	return l[i], nil
}

// Remove a DataModel
func (l DataModelList) RemoveById(id string) error {

	i, err := l.IndexById(id)
	if err != nil {
		return err
	}

	l[i] = nil
	l = append(l[:i], l[i+1:]...)

	return nil
}

// ----------------------------------------------------------------------------
//
// LoadModelList Methods
//
// ----------------------------------------------------------------------------

func (l LoadModelList) Len() int           { return len(l) }
func (l LoadModelList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l LoadModelList) Less(i, j int) bool { return l[i].Id < l[j].Id }

// Index of LoadModel by Id
func (l LoadModelList) IndexById(id string) (int, error) {

	if len(id) == 0 {
		return -1, ErrModelIdInvalid
	}

	for j, a := range l {
		if a.Id == id {
			return j, nil
			break
		}
	}

	return -1, ErrModelNotFound
}

// Find a LoadModel by Id
func (l LoadModelList) FindById(id string) (*LoadModel, error) {

	i, err := l.IndexById(id)
	if err != nil {
		return nil, err
	}

	return l[i], nil
}

// Remove a LoadModel
func (l LoadModelList) RemoveById(id string) error {

	i, err := l.IndexById(id)
	if err != nil {
		return err
	}

	l[i] = nil
	l = append(l[:i], l[i+1:]...)

	return nil
}
