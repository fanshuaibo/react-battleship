package pgoutput

import (
	"fmt"

	"github.com/jackc/pgx/pgtype"
)

type RelationSet struct {
	// Mutex probably will be redundan