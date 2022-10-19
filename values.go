package pgoutput

import (
	"fmt"

	"github.com/jackc/pgx/pgtype"
)

type RelationSet struct {
	// Mutex probably will be redundant as receiving
	// a replication stream is currently strictly single-threaded
	relations map[uint32]Relation
	connInfo  *pgtype.ConnInfo
}

// NewRelationSet creates a new relation set.
// Optionally ConnInfo can be provided, however currently we need some changes to pgx to get it out
// from ReplicationConn.
func NewRelationSet(ci *pgtype.ConnInfo) *RelationSet {
	return &RelationSet{map[uint32]Relation{}, ci}
}

func (rs *RelationSet) Add(r Relation) {
	rs.relations[r.ID] = r
}

func (rs *RelationSet) Get(ID uint32) (r Relation, ok bool) {
	r, ok = rs.relations[ID]
	return
}

func (rs *RelationSet) Values(id uint32, row []Tuple) (map[string]pgtype.Value, error) {
	values := map[string]pgtype.Value{}
	rel, ok := rs.Get(id)
	if !ok {
		return values, fmt.Errorf("no relation for %d", id)
	}

	// assert same number of row and columns
	for i, tuple := range row {
		col := rel.Columns[i]
		decoder := col.Decoder()

		if err := decoder.DecodeText(rs.connInfo, tuple.Value); err != nil {
			return nil, fmt.Errorf("error decoding tuple %d: %s", i, err)
		}

		values[col.Name] = decoder
	}

	return values, nil
}

func (c Column) Decoder() DecoderValue {
	switch c.Type {
	case pgtype.ACLItemArrayOID:
		return &pgtype.ACLItemArray{}
	case pgtype.ACLItemOID:
		return &pgtype.ACLItem{}
	case pgtype.BoolArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.BoolOID:
		return &pgtype.Bool{}
	case pgtype.ByteaArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.ByteaOID:
		return &pgtype.Bytea{}
	case pgtype.CIDOID:
		return &pgtype.CID{}
	case pgtype.CIDRArrayOID:
		return &pgtype.CIDRArray{}
	case pgtype.CIDROID:
		return &pgtype.CIDR{}
	case pgtype.CharOID:
		// Not all possible values of QChar are representable in the text format
		return &pgtype.Unknown{}
	case pgtype.DateArrayOID:
		return &pgtype.DateArray{}
	case pgtype.DateOID:
		return &pgtype.Date{}
	case pgtype.Float4ArrayOID:
		return &pgtype.Float4Array{}
	case pgtype.Float4OID:
		return &pgtype.Float4{}
	case pgtype.Float8ArrayOID:
		return &pgtype.Float8Array{}
	case pgtype.Float8OID:
		return &pgtype.Float8{}
	case pgtype.InetArrayOID:
		return &pgtype.InetArray{}
	case pgtype.InetOID:
		return &pgtype.Inet{}
	case pgtype.Int2ArrayOID:
		return &pgtype.Int2Array{}
	case pgtype.Int2OID:
		return &pgtype.Int2{}
