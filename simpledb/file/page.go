package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Page struct {
	buffer *bytes.Buffer
}

func NewPage(blockSize int) *Page {
	return &Page{
		buffer: bytes.NewBuffer(make([]byte, blockSize)),
	}
}

func NewPageFromBytes(b []byte) *Page {
	return &Page{
		buffer: bytes.NewBuffer(b),
	}
}

func (p *Page) GetInt(offset int) int {
	data := p.buffer.Bytes()
	if offset+4 > len(data) {
		panic(fmt.Sprintf("GetInt: offset %d out of bounds", offset))
	}
	return int(binary.BigEndian.Uint32(data[offset:]))
}

func (p *Page) SetInt(offset int, n int) {
	data := p.buffer.Bytes()
	if offset+4 > len(data) {
		panic(fmt.Sprintf("SetInt: offset %d out of bounds", offset))
	}
	binary.BigEndian.PutUint32(data[offset:], uint32(n))
}

func (p *Page) GetBytes(offset int) []byte {
	data := p.buffer.Bytes()
	if offset+4 > len(data) {
		panic(fmt.Sprintf("GetBytes: offset %d out of bounds", offset))
	}

	length := int(binary.BigEndian.Uint32(data[offset:]))
	if offset+4+length > len(data) {
		panic(fmt.Sprintf("GetBytes: data out of bounds at offset %d", offset))
	}

	return data[offset+4 : offset+4+length]
}

func (p *Page) SetBytes(offset int, b []byte) {
	data := p.buffer.Bytes()
	if offset+4+len(b) > len(data) {
		panic(fmt.Sprintf("SetBytes: offset %d out of bounds", offset))
	}

	binary.BigEndian.PutUint32(data[offset:], uint32(len(b)))
	copy(data[offset+4:], b)
}

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset int, s string) {
	p.SetBytes(offset, []byte(s))
}

func MaxLength(strlen int) int {
	return 4 + strlen
}

func (p *Page) Contents() *bytes.Buffer {
	return p.buffer
}

func (p *Page) SetContents(buf *bytes.Buffer) {
	p.buffer = buf
}
