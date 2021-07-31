package main

import (
	"context"
	"fmt"
	"log"

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

	set := pgoutput.NewRelationSet(nil)

	dump := func(relation uint32, row []pgoutput.Tuple) error {
		values, err := set.Values(relation, row)
		if err != nil {
			return fmt.Errorf("error parsing values: %s", err)
		}
	