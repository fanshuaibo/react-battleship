
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
