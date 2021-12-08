
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