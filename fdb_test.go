package fdb

import (
	"database/sql"
	_ "github.com/bmizerany/pq.go"
	_ "github.com/ziutek/mymysql/godrv"
	"testing"
)

func TestMysqlTableCreation(t *testing.T) {
	conn, err := sql.Open("mymysql", "tcp:localhost:3306*fdbtest/testuser/foo")
	if err != nil {
		t.Fatal(err)
	}
	Register(conn)

	if err := initTables(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTableCreation(t *testing.T) {
	conn, err := sql.Open("postgres", "postgres://okco:@localhost:5432/okcoerrors")
	if err != nil {
		t.Fatal(err)
	}
	Register(conn)

	if err := initTables(); err != nil {
		t.Fatal(err)
	}
}
