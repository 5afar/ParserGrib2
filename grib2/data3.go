package grib2

import (
	"fmt"
	"io"

	"gribV2.com/grib2/reader"
)

type Data3 struct {
	Data2
	SpatialOrderDifference uint8 `json:"spatialOrderDifference"`
	OctetsNumber           uint8 `json:"octetsNumber"`
}

func (template *Data3) applySpacialDifferencing(section7Data []int64, minsd int64, ival1 int64, ival2 int64) {
	switch template.SpatialOrderDifference {
	case 1:
		section7Data[0] = ival1

		for n := int(1); n < len(section7Data); n++ {
			section7Data[n] = section7Data[n] + section7Data[n-1] + minsd
		}
	case 2:

		section7Data[0] = ival1
		section7Data[1] = ival2

		for n := int(2); n < len(section7Data); n++ {
			section7Data[n] = section7Data[n] + (2 * section7Data[n-1]) - section7Data[n-2] + minsd
		}
	}
}

func (template *Data3) extractSpacingDifferentialValues(bitReader *reader.BitReader) (int64, int64, int64, error) {
	var ival1 int64
	var ival2 int64
	var minsd int64

	rc := int(template.OctetsNumber) * 8
	if rc != 0 {
		var err error
		ival1, err = bitReader.ReadInt(rc)
		if err != nil {
			return minsd, ival1, ival2, fmt.Errorf("Spacial differencing Value 1: %s", err.Error())
		}

		if template.SpatialOrderDifference == 2 {
			ival2, err = bitReader.ReadInt(rc)
			if err != nil {
				return minsd, ival1, ival2, fmt.Errorf("Spacial differencing Value 2: %s", err.Error())
			}
		}

		minsd, err = bitReader.ReadInt(rc)
		if err != nil {
			return minsd, ival1, ival2, fmt.Errorf("Spacial differencing Reference: %s", err.Error())
		}
	}

	return minsd, ival1, ival2, nil
}

func ParseData3(dataReader io.Reader, dataLength int, template *Data3) ([]float64, error) {

	bitReader, err := reader.New(dataReader, dataLength)
	if err != nil {
		return nil, err
	}

	minsd, ival1, ival2, err := template.extractSpacingDifferentialValues(bitReader)
	if err != nil {
		return nil, fmt.Errorf("Spacial differencing Value 1: %s", err.Error())
	}

	bitGroups, err := template.extractBitGroupParameters(bitReader)
	if err != nil {
		return nil, fmt.Errorf("Groups: %s", err.Error())
	}

	if err := checkLengths(bitGroups, dataLength); err != nil {
		return nil, fmt.Errorf("Check length: %s", err.Error())
	}

	section7Data, ifldmiss, err := template.extractData(bitReader, bitGroups)
	if err != nil {
		return nil, fmt.Errorf("Data extract: %s", err.Error())
	}

	template.applySpacialDifferencing(section7Data, minsd, ival1, ival2)

	return template.scaleValues(section7Data, ifldmiss), nil
}
