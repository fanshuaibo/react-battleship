
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