package fdb

import (
	"database/sql"
	_ "github.com/zeebo/pq.go"
	_ "github.com/ziutek/mymysql/godrv"
	"reflect"
	"testing"
)

type serialTest bool

func (s *serialTest) Serialize() (p []byte)          { *s = true; return }
func (s *serialTest) Unserialize([]byte) (err error) { *s = true; return }

type MyThing struct {
	ID int
	S  serialTest
}

func TestSerializer(t *testing.T) {
	w, err := newPostgres()
	if err != nil {
		t.Fatal(err)
	}
	defer w.connection.Close()

	var x MyThing
	if err := w.Update(&x); err != nil {
		t.Fatal(err)
	}
	if !bool(x.S) {
		t.Fatal("Didn't serialize")
	}
	var y MyThing
	if err := w.Load(&y, x.ID); err != nil {
		t.Fatal(err)
	}
	if !bool(y.S) {
		t.Fatal("Didn't unserialize")
	}
}

type User struct {
	ID       int
	Email    string
	Password string
	Salt     string
}

func TestSimpleUser(t *testing.T) {
	w, err := newPostgres()
	if err != nil {
		t.Fatal(err)
	}
	defer w.connection.Close()

	x := User{
		Email:    "foo@bar.com",
		Password: "barrzz",
		Salt:     "fasdfasdf",
	}
	if err := w.Update(&x); err != nil {
		t.Fatal(err)
	}
	var y User
	if err := w.Load(&y, x.ID); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(x, y) {
		t.Fatalf("Expected %+v got %+v", x, y)
	}
}

func newMysql() (*Wrapper, error) {
	conn, err := sql.Open("mymysql", "tcp:localhost:3306*haha/testuser/foo")
	if err != nil {
		return nil, err
	}
	return New(conn, false)
}

func newPostgres() (*Wrapper, error) {
	conn, err := sql.Open("postgres", "postgres://okco:@localhost:5432/okcoerrors")
	if err != nil {
		return nil, err
	}
	return New(conn, false)
}

func doLoadBenchmark(b *testing.B, c connector, id int) {
	b.StopTimer()
	w, err := c()
	if err != nil {
		b.Fatal(err)
	}
	defer w.connection.Close()

	var u User
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Load(&u, id); err != nil {
			b.Fatal(err)
		}
	}
}

func doUpdateBenchmark(b *testing.B, c connector, id int) {
	b.StopTimer()
	w, err := c()
	if err != nil {
		b.Fatal(err)
	}
	defer w.connection.Close()

	var u User
	if err := w.Load(&u, id); err != nil {
		b.Fatal(err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Update(&u); err != nil {
			b.Fatal(err)
		}
	}

}

func BenchmarkMysqlLoad(b *testing.B) {
	doLoadBenchmark(b, newMysql, 1)
}

func BenchmarkMysqlUpdate(b *testing.B) {
	doUpdateBenchmark(b, newMysql, 1)
}

func BenchmarkPostgresLoad(b *testing.B) {
	doLoadBenchmark(b, newPostgres, 24)
}

func BenchmarkPostgresUpdate(b *testing.B) {
	doUpdateBenchmark(b, newPostgres, 24)
}

type connector func() (*Wrapper, error)
