# pgoutput

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx"
	"github.com/kyleconroy/pgoutput"
)

func main() {
	ctx := context.Background()
	config := pgx.ConnConfig{Database: "opsdash", User: "replicant"}
	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		log.Fatal(err)
	}

  // Create a slot if it doesn't already exist
	// if err := conn.CreateReplicationSlot("sub2", "pgoutput"); err != nil {
	// 	log.Fatalf("Failed to create replication slot: %v", err)
	// }

	set := pgoutput.NewRelationSet()

	dump := func(relati