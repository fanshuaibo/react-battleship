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
	config := pg