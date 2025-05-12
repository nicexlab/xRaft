package conn

import (
	"encoding/binary"
	"io"
)

type Trans interface {
	io.Writer
	io.Reader
}

func BufWrite(w Trans, bs []byte) {
	var lenb [4]byte
	b := lenb[:4]
	binary.LittleEndian.PutUint32(b, uint32(len(bs)))
	w.Write(b)
	w.Write(bs)
}

func BufRead(r Trans) ([]byte, error) {
	var lenb [4]byte
	b := lenb[:4]
	n, e := io.ReadFull(r, b)
	if n != 4 || e != nil {
		return nil, e
	}
	nlen := binary.LittleEndian.Uint32(b)
	bs := make([]byte, nlen)
	if _, err := io.ReadAtLeast(r, bs, int(nlen)); err != nil {
		return nil, err
	}
	return bs, nil
}

type Error *string
