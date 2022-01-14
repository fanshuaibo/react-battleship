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
// Optionally ConnInfo can be provided, however currently we need some changes to pgx to get i