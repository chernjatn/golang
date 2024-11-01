package types

import (
	"database/sql/driver"
	"encoding/json"
	"strconv"
)

const (
	InventorySourceTypeMain      = uint(0)
	InventorySourceTypeDarkstore = uint(3)
)

type Price struct {
	String string
	Int    uint64
	Float  float64
}

type Quantity struct {
	String string
	Int    uint
}

func (p *Price) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &p.Float)
	if err != nil {
		return err
	}

	p.Int = uint64(p.Float * 100)
	p.String = strconv.FormatFloat(p.Float, 'f', 2, 64)
	return nil
}

func (p *Price) MarshalJSON() ([]byte, error) {
	return []byte(p.String), nil
}

func (p *Price) IsEmpty() bool {
	return p.Int == 0
}

func (p *Price) ToDecimal() string {
	return p.String
}

func (f *Price) Value() (driver.Value, error) {
	return f.ToDecimal(), nil
}

func (f *Price) Scan(value interface{}) error {
	var data = []byte(value.([]uint8))
	return f.UnmarshalJSON(data)
}

func (p *Quantity) UnmarshalJSON(data []byte) error {
	var val float64
	err := json.Unmarshal(data, &val)
	if err != nil {
		return err
	}

	p.Int = uint(val)
	p.String = strconv.FormatUint(uint64(p.Int), 10)
	return nil
}

func (p *Quantity) MarshalJSON() ([]byte, error) {
	return []byte(p.String), nil
}

func (p *Quantity) IsEmpty() bool {
	return p.Int == 0
}

func (p *Quantity) Value() (driver.Value, error) {
	return p.String, nil
}

func (p *Quantity) Scan(value interface{}) error {
	var data = []byte(value.([]uint8))
	return p.UnmarshalJSON(data)
}

func (p *Quantity) Add(v Quantity) {
	p.Int += v.Int
	p.String = strconv.FormatFloat(float64(p.Int), 'f', 2, 64)
}
