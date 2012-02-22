package fdb

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq.go"
	_ "github.com/ziutek/mymysql/godrv"
	"math/rand"
	"testing"
	"time"
)

func _TestMysqlLoad(t *testing.T) {
	conn, err := sql.Open("mymysql", "tcp:localhost:3306*haha/testuser/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	w, err := New(conn, false)
	if err != nil {
		t.Fatal(err)
	}

	var u User
	if err := w.Load(&u, 1); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", u)
}

func _TestPostgresUpdateAndLoad(t *testing.T) {
	conn, err := sql.Open("postgres", "postgres://okco:@localhost:5432/okcoerrors")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	w, err := New(conn, false)
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(time.Now().UnixNano())

	f := Feed{
		URL:   "foobar",
		Title: fmt.Sprint(rand.Int63()),
	}
	if err := w.Update(&f); err != nil {
		t.Fatal(err)
	}
}

func _TestMysqlUpdateAndLoad(t *testing.T) {
	conn, err := sql.Open("mymysql", "tcp:localhost:3306*fdbtest/testuser/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	w, err := New(conn, false)
	if err != nil {
		t.Fatal(err)
	}

	u := User{
		Email:    "foo@bar.com",
		Password: "bad_pass",
		Salt:     "salty!",
	}
	if err := w.Update(&u); err != nil {
		t.Fatal(err)
	}

	var v User
	if err := w.Load(&v, u.ID); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", u)
	t.Logf("%+v", v)
}

type User struct {
	ID       int
	Email    string
	Password string
	Salt     string
}

type Feed struct {
	ID    int
	URL   string
	Title string
}

func TestPostgresLinks(t *testing.T) {
	conn, err := sql.Open("postgres", "postgres://okco:@localhost:5432/okcoerrors")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	w, err := New(conn, false)
	if err != nil {
		t.Fatal(err)
	}

	var u User
	if err := w.Load(&u, 24); err != nil {
		t.Fatal(err)
	}

	if err := w.Link(u, Feed{ID: 33}); err != nil {
		t.Fatal(err)
	}
	if err := w.Link(u, Feed{ID: 34}); err != nil {
		t.Fatal(err)
	}
	if err := w.Link(u, Feed{ID: 35}); err != nil {
		t.Fatal(err)
	}

	var feeds []Feed
	if err := w.Links(u, &feeds); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", feeds)

	//relink
	if err := w.Link(u, Feed{ID: 34}); err != nil {
		t.Fatal(err)
	}

	if err := w.Links(u, &feeds); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", feeds)

	//unlink
	if err := w.Unlink(u, Feed{ID: 33}); err != nil {
		t.Fatal(err)
	}
	if err := w.Unlink(u, Feed{ID: 34}); err != nil {
		t.Fatal(err)
	}
	if err := w.Unlink(u, Feed{ID: 35}); err != nil {
		t.Fatal(err)
	}

	if err := w.Links(u, &feeds); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", feeds)
}

func _TestMysqlLinks(t *testing.T) {
	conn, err := sql.Open("mymysql", "tcp:localhost:3306*haha/testuser/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	w, err := New(conn, false)
	if err != nil {
		t.Fatal(err)
	}

	var u User
	if err := w.Load(&u, 1); err != nil {
		t.Fatal(err)
	}
	var feeds []Feed
	if err := w.Links(u, &feeds); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", feeds)
}

func _TestPostgresUpdate(t *testing.T) {
	conn, err := sql.Open("postgres", "postgres://okco:@localhost:5432/okcoerrors")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	w, err := New(conn, false)
	if err != nil {
		t.Fatal(err)
	}

	var u User
	if err := w.Load(&u, 24); err != nil {
		t.Fatal(err)
	}

	u.Email = fmt.Sprint(rand.Int63())
	if err := w.Update(u); err != nil {
		t.Fatal(err)
	}
}
