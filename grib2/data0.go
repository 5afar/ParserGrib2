package grib2

import (
	"io"
	"math"

	"gribV2.com/grib2/reader"
)

const INT_MAX = 9223372036854775807

type Data0 struct {
	Reference    float32 `json:"reference"`
	BinaryScale  uint16  `json:"binaryScale"`
	DecimalScale uint16  `json:"decimalScale"`
	Bits         uint8   `json:"bits"`
	Type         uint8   `json:"type"`
}

func (template Data0) getRefScale() (float64, float64) {
	mask := uint16(1 << 15) // Маска для наиболее значимого бита
	switch (template.BinaryScale & mask) != 0 {
	case true:
		// есть единица
		m := template.BinaryScale & 0x7FFF
		bscale := math.Pow(2.0, -float64(m))
		dscale := math.Pow(10.0, -float64(template.DecimalScale))
		scale := bscale * dscale
		ref := dscale * float64(template.Reference)
		return ref, scale

	case false:
		// нет
		bscale := math.Pow(2.0, float64(template.BinaryScale))
		dscale := math.Pow(10.0, float64(template.DecimalScale))
		scale := bscale * dscale
		ref := dscale * float64(template.Reference)
		return ref, scale
	}

	return 0, 0
}

func (template Data0) scaleFunc() func(uintValue int64) float64 {

	ref, scale := template.getRefScale()
	return func(value int64) float64 {
		signed := int64(value)
		return ref + float64(signed)*scale
	}
}

func ParseData0(dataReader io.Reader, dataLength int, template *Data0) ([]float64, error) {

	fld := []float64{}
	if dataLength == 0 {
		return fld, nil
	}
	scaleStrategy := template.scaleFunc()
	bitReader, err := reader.New(dataReader, dataLength)
	if err != nil {
		return fld, err
	}
	//количество данных
	dataSize := int64(math.Floor(
		float64(8*dataLength) / float64(template.Bits),
	))
	uintDataSlice, errRead := bitReader.ReadUintsBlock(int(template.Bits), dataSize, false)
	if errRead != nil {
		return []float64{}, errRead
	}
	for _, uintValue := range uintDataSlice {
		fld = append(fld, scaleStrategy(int64(uintValue)))
	}
	return fld, nil
}
