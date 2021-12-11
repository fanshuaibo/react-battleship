
package pgoutput

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
)

type Subscription struct {
	Name          string
	Publication   string
	WaitTimeout   time.Duration
	StatusTimeout time.Duration

	conn       *pgx.ReplicationConn
	maxWal     uint64
	walRetain  uint64
	walFlushed uint64

	failOnHandler bool

	// Mutex is used to prevent reading and writing to a connection at the same time
	sync.Mutex
}

type Handler func(Message, uint64) error

func NewSubscription(conn *pgx.ReplicationConn, name, publication string, walRetain uint64, failOnHandler bool) *Subscription {
	return &Subscription{
		Name:          name,
		Publication:   publication,
		WaitTimeout:   1 * time.Second,
		StatusTimeout: 10 * time.Second,

		conn:          conn,
		walRetain:     walRetain,
		failOnHandler: failOnHandler,
	}
}

func pluginArgs(version, publication string) string {
	return fmt.Sprintf(`"proto_version" '%s', "publication_names" '%s'`, version, publication)
}

// CreateSlot creates a replication slot if it doesn't exist
func (s *Subscription) CreateSlot() (err error) {
	// If creating the replication slot fails with code 42710, this means
	// the replication slot already exists.
	if err = s.conn.CreateReplicationSlot(s.Name, "pgoutput"); err != nil {
		pgerr, ok := err.(pgx.PgError)
		if !ok || pgerr.Code != "42710" {
			return
		}

		err = nil
	}

	return
}

func (s *Subscription) sendStatus(walWrite, walFlush uint64) error {
	if walFlush > walWrite {
		return fmt.Errorf("walWrite should be >= walFlush")
	}

	s.Lock()
	defer s.Unlock()

	k, err := pgx.NewStandbyStatus(walFlush, walFlush, walWrite)
	if err != nil {
		return fmt.Errorf("error creating status: %s", err)
	}

	if err = s.conn.SendStandbyStatus(k); err != nil {