package pgoutput

import (
	"fmt"

	"github.com/jackc/pgx/pgtype"
)

type RelationSet struct {
	// Mutex probably will be redundant as receiving
	// a replication stream is currently stric