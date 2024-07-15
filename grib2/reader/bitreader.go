package reader

import (

	"bytes"
	"io"
)

type BitReader struct {
	reader io.ByteReader
	byte   byte
	offset byte
}

func (r *BitReader) ResetOffset() {
	r.offset = 0
}

func New(dataReader io.Reader, dataLength int) (*BitReader, error) {
	rawData := make([]byte, dataLength)
	_, err := dataReader.Read(rawData)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(rawData)
	return newReader(buffer), nil
}

func (r *BitReader) ReadInt(bits int) (int64, error) {
	var result int64
	var negative int64 = 1
	for i := bits - 1; i >= 0; i-- {
		bit, err := r.readBit()

		if err != nil {
			return 0, err
		}
		if i == (bits-1) && bit == 1 {
			negative = -1
			continue
		}
		result |= int64(bit << uint(i))
	}
	return negative * result, nil
}

func (r *BitReader) ReadUintsBlock(bits int, count int64, resetOffset bool) ([]uint64, error) {
	result := make([]uint64, count)

	if resetOffset {
		r.ResetOffset()
	}

	if bits != 0 {
		for i := int64(0); i != count; i++ {
			data, err := r.readUint(bits)
			if err != nil {
				return result, err
			}
			result[i] = data
		}
	}

	return result, nil
}

func newReader(r io.ByteReader) *BitReader {
	return &BitReader{r, 0, 0}
}

func (r *BitReader) currentBit() byte {
	return (r.byte >> (7 - r.offset)) & 0x01
}

func (r *BitReader) readBit() (uint, error) {
	if r.offset == 8 || r.offset == 0 {
		r.offset = 0
		if b, err := r.reader.ReadByte(); err == nil {
			r.byte = b
		} else {
			return 0, err
		}
	}
	bit := uint((r.byte >> (7 - r.offset)) & 0x01)

	r.offset++
	return bit, nil
}

func (r *BitReader) readUint(nbits int) (uint64, error) {
	var result uint64
	for i := nbits - 1; i >= 0; i-- {
		if bit, err := r.readBit(); err == nil {
			result |= uint64(bit << uint(i))
		} else {
			return 0, err
		}
	}

	return result, nil
}
