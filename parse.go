
package pgoutput

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgx/pgtype"
)

type decoder struct {
	order binary.ByteOrder
	buf   *bytes.Buffer
}

func (d *decoder) bool() bool {
	x := d.buf.Next(1)[0]
	return x != 0

}

func (d *decoder) uint8() uint8 {
	x := d.buf.Next(1)[0]
	return x

}

func (d *decoder) uint16() uint16 {
	x := d.order.Uint16(d.buf.Next(2))
	return x
}

func (d *decoder) string() string {
	s, err := d.buf.ReadBytes(0)
	if err != nil {
		// TODO: Return an error
		panic(err)
	}
	return string(s[:len(s)-1])
}

func (d *decoder) uint32() uint32 {
	x := d.order.Uint32(d.buf.Next(4))
	return x

}

func (d *decoder) uint64() uint64 {
	x := d.order.Uint64(d.buf.Next(8))
	return x
}

func (d *decoder) int8() int8   { return int8(d.uint8()) }
func (d *decoder) int16() int16 { return int16(d.uint16()) }
func (d *decoder) int32() int32 { return int32(d.uint32()) }
func (d *decoder) int64() int64 { return int64(d.uint64()) }

func (d *decoder) timestamp() time.Time {
	micro := int(d.uint64())
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	return ts.Add(time.Duration(micro) * time.Microsecond)
}

func (d *decoder) rowinfo(char byte) bool {
	if d.buf.Next(1)[0] == char {
		return true
	} else {
		d.buf.UnreadByte()
		return false
	}
}

func (d *decoder) tupledata() []Tuple {
	size := int(d.uint16())
	data := make([]Tuple, size)
	for i := 0; i < size; i++ {
		switch d.buf.Next(1)[0] {
		case 'n':
		case 'u':
		case 't':
			vsize := int(d.order.Uint32(d.buf.Next(4)))
			data[i] = Tuple{Flag: 't', Value: d.buf.Next(vsize)}
		}
	}
	return data
}

func (d *decoder) columns() []Column {
	size := int(d.uint16())
	data := make([]Column, size)
	for i := 0; i < size; i++ {
		data[i] = Column{
			Key:  d.bool(),
			Name: d.string(),
			Type: d.uint32(),
			Mode: d.uint32(),
		}
	}
	return data
}

type Begin struct {
	// The final LSN of the transaction.
	LSN uint64
	// Commit timestamp of the transaction. The value is in number of
	// microseconds since PostgreSQL epoch (2000-01-01).
	Timestamp time.Time
	// 	Xid of the transaction.
	XID int32
}

type Commit struct {
	Flags uint8
	// The final LSN of the transaction.
	LSN uint64
	// The final LSN of the transaction.
	TransactionLSN uint64
	Timestamp      time.Time
}

type Relation struct {
	// ID of the relation.
	ID uint32
	// Namespace (empty string for pg_catalog).
	Namespace string
	Name      string
	Replica   uint8
	Columns   []Column
}

func (r Relation) IsEmpty() bool {
	return r.ID == 0 && r.Name == "" && r.Replica == 0 && len(r.Columns) == 0
}

type Type struct {
	// ID of the data type
	ID        uint32
	Namespace string
	Name      string
}

type Insert struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	New bool
	Row []Tuple
}

type Update struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Old    bool
	Key    bool
	New    bool
	OldRow []Tuple
	Row    []Tuple
}

type Delete struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Key bool // TODO
	Old bool // TODO
	Row []Tuple
}

type Origin struct {
	LSN  uint64
	Name string
}

type DecoderValue interface {
	pgtype.TextDecoder
	pgtype.Value
}

type Column struct {
	Key  bool
	Name string
	Type uint32
	Mode uint32
}
type Tuple struct {
	Flag  int8
	Value []byte
}

type Message interface {
	msg()
}

func (Begin) msg()    {}
func (Relation) msg() {}
func (Update) msg()   {}
func (Insert) msg()   {}
func (Delete) msg()   {}
func (Commit) msg()   {}