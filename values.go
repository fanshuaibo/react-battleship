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
	