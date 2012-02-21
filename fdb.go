package fdb

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
)

var (
	connection *sql.DB
	connLock   sync.RWMutex
	doer       sync.Once
)

type tableSpec struct {
	Name string
	SQL  string
}

func (t *tableSpec) check(conn *sql.DB) (err error) {
	log.Println(t.Name)
	res, err := connection.Query(fmt.Sprintf("SELECT null FROM %s", t.Name))
	if res != nil {
		defer res.Close()
	}
	if err == nil {
		return
	}
	_, err = conn.Exec(t.SQL)
	return
}

var tables = []tableSpec{
	{"objects", objects_sql},
	{"attributes", attributes_sql},
	{"object_caches", object_caches_sql},
	{"object_links", object_links_sql},
}

func Register(conn *sql.DB) {
	connLock.Lock()
	defer connLock.Unlock()

	connection = conn
}

func Load(obj interface{}, id int) (err error) {
	connLock.RLock()
	defer connLock.RUnlock()

	return
}

func Update(obj interface{}) (err error) {
	connLock.RLock()
	defer connLock.RUnlock()

	return
}

func Bootstrap(conn *sql.DB) (err error) {
	doer.Do(func() {
		Register(conn)
		err = initTables()
	})
	return
}

func initTables() (err error) {
	connLock.RLock()
	defer connLock.RUnlock()

	//check for table existence and create when missing
	for _, table := range tables {
		if err = table.check(connection); err != nil {
			return
		}
	}

	return
}
