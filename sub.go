
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
		return err
	}

	return nil
}

// Flush sends the status message to server indicating that we've fully applied all of the events until maxWal.
// This allows PostgreSQL to purge it's WAL logs
func (s *Subscription) Flush() error {
	wp := atomic.LoadUint64(&s.maxWal)
	err := s.sendStatus(wp, wp)
	if err == nil {
		atomic.StoreUint64(&s.walFlushed, wp)
	}

	return err
}

// Start replication and block until error or ctx is canceled
func (s *Subscription) Start(ctx context.Context, startLSN uint64, h Handler) (err error) {
	err = s.conn.StartReplication(s.Name, startLSN, -1, pluginArgs("1", s.Publication))
	if err != nil {
		return fmt.Errorf("failed to start replication: %s", err)