
package pgoutput

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx"
)

func GenerateLogicalReplicationFiles(t *testing.T) {
	config := pgx.ConnConfig{
		Database: "opsdash",
		User:     "replicant",
	}

	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.StartReplication("sub1", 0, -1, `("proto_version" '1', "publication_names" 'pub1')`)
	if err != nil {
		log.Fatalf("Failed to start replication: %v", err)
	}

	ctx := context.Background()
	count := 0

	for {
		var message *pgx.ReplicationMessage

		message, err = conn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Fatalf("Replication failed: %v %s", message, err)
		}

		if message.WalMessage != nil {
			ioutil.WriteFile(fmt.Sprintf("%03d.waldata", count), message.WalMessage.WalData, 0644)
			count += 1
		}
		if message.ServerHeartbeat != nil {
			log.Printf("Got heartbeat: %s", message.ServerHeartbeat)
		}
	}
}

func TestParseWalData(t *testing.T) {