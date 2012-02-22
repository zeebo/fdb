package fdb

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

var (
	NotFound        = fmt.Errorf("Object not found")
	UnknownDatabase = fmt.Errorf("Don't know how to prepare statments for this database")
)

var null interface{}

type dbType int

const (
	dbUnknown dbType = iota
	dbPostgres
	dbMysql
)

type Wrapper struct {
	connection *sql.DB
	connLock   sync.RWMutex
	doer       sync.Once
	preview    bool

	prepareType dbType
}

var postgresRegex = regexp.MustCompile(`\$\d+`)

//New returns a new Wrapper and calls Bootstrap for the connection returning
//any errors.
func New(conn *sql.DB, preview bool) (w *Wrapper, err error) {
	w = &Wrapper{
		preview:     preview,
		prepareType: examineDB(conn),
	}

	if w.prepareType == dbUnknown {
		return nil, UnknownDatabase
	}

	err = w.Bootstrap(conn)
	return
}

func examineDB(conn *sql.DB) dbType {
	//attempt postgres
	if _, err := conn.Prepare(`INSERT INTO objects (object_type) VALUES ($1)`); err == nil {
		return dbPostgres
	}

	//attempt mysql
	if _, err := conn.Prepare(`INSERT INTO objects (object_type) VALUES (?)`); err == nil {
		return dbMysql
	}

	return dbUnknown
}

func (w *Wrapper) prepare(query string) string {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	log.Printf("preparing for %d: %q", w.prepareType, query)

	switch w.prepareType {
	case dbUnknown:
		w.prepareType = examineDB(w.connection)
		if w.prepareType == dbUnknown {
			panic(UnknownDatabase)
		}
		return w.prepare(query)
	case dbPostgres:
		return query
	case dbMysql:
		return postgresRegex.ReplaceAllString(query, "?")
	}
	panic("unreachable")
}

func (w *Wrapper) begin() (tx, error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	stx, err := w.connection.Begin()
	return tx{stx, w}, err
}

func (w *Wrapper) exec(query string, args ...interface{}) (sql.Result, error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	return w.connection.Exec(w.prepare(query), args...)
}

func (w *Wrapper) query(query string, args ...interface{}) (*sql.Rows, error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	return w.connection.Query(w.prepare(query), args...)

}

func (w *Wrapper) queryRow(query string, args ...interface{}) *sql.Row {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	return w.connection.QueryRow(w.prepare(query), args...)
}

//SetPreview sets if we are Loading/Updating in preview mode or not.
func (w *Wrapper) SetPreview(val bool) {
	w.connLock.Lock()
	defer w.connLock.Unlock()

	w.preview = val
}

//Load takes an object and an id and loads the attributes for that object into
//it using the same method as the database/sql package.
func (w *Wrapper) Load(obj interface{}, id int) (err error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	prv := reflect.ValueOf(obj)
	if prv.Kind() != reflect.Ptr {
		return fmt.Errorf("%T is not a pointer type", obj)
	}
	rv := prv.Elem()

	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("%T is not a struct type", obj)
	}

	var idFld reflect.Value
	if idFld = rv.FieldByName("ID"); !idFld.IsValid() || idFld.Kind() != reflect.Int {
		return fmt.Errorf("%T is missing a field `ID int`", obj)
	}

	name := rv.Type().Name()

	row := w.queryRow(`SELECT NULL FROM objects WHERE
		    object_id = $1
		AND object_type = $2
		AND object_deleted = false`, id, name)

	if row.Scan(&null) == sql.ErrNoRows {
		return NotFound
	}

	//we found it so set the id
	idFld.SetInt(int64(id))

	rows, err := w.query(`SELECT attribute_key, attribute_value
		FROM attributes WHERE
		    object_id = $1
		AND attribute_archived = false
		AND attribute_preview = $2`, id, w.preview)
	if err != nil {
		return
	}
	defer rows.Close()

	var key string
	var value []byte
	for rows.Next() {
		if err = rows.Scan(&key, &value); err != nil {
			return
		}

		if fld := rv.FieldByName(key); fld.IsValid() {
			if err = convertAssign(fld.Addr().Interface(), value); err != nil {
				return
			}
		}
	}

	err = rows.Err()

	return
}

//getId is a helper method to get the ID field from an object
func getId(val reflect.Value) (id int, err error) {
	fld := val.FieldByName("ID")
	if !fld.IsValid() || fld.Kind() != reflect.Int {
		return 0, fmt.Errorf("%T is missing a field `ID int`", val.Type())
	}
	id = int(fld.Int())
	return
}

//Update takes an object and stores it in the database.
func (w *Wrapper) Update(obj interface{}) (err error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	prv := reflect.ValueOf(obj)
	rv := reflect.Indirect(prv)

	if !rv.IsValid() {
		return fmt.Errorf("Invalid object passed in to Update")
	}

	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("%T is not a struct or pointer to struct", obj)
	}

	tv := rv.Type()

	id, err := getId(rv)
	if err != nil {
		return
	}

	if id == 0 && prv.Kind() != reflect.Ptr {
		return fmt.Errorf("Must pass in a pointer to the object if it is new")
	}

	//begin a transaction
	tx, err := w.begin()
	if err != nil {
		return
	}

	//set up our transaction logic
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			//dont care about the error rolling back
			tx.Rollback()
		}
	}()

	var newObject bool

	//insert the object. by the end we should have a nonzero id.
	if id != 0 {
		//lets check if the id exists
		row := tx.QueryRow(`SELECT object_type FROM objects WHERE
			    object_id = $1
			AND object_deleted = false`, id)

		var resultType string
		if err = row.Scan(&resultType); err != nil {
			return
		}

		switch resultType {
		case tv.Name():
		case "":
			return fmt.Errorf("No object with id %d exists.", id)
		default:
			return fmt.Errorf("Cannot update an object of a different type: %s into %s", tv.Name(), resultType)
		}
	} else {
		//do an insert and update the id
		log.Println("Inserting")

		var result sql.Result
		result, err = tx.Exec(`INSERT INTO objects (object_type) VALUES ($1)`, tv.Name())
		if err != nil {
			return
		}

		log.Println("Getting insert id")

		var tid int64
		tid, err = result.LastInsertId()
		if err != nil {
			//lets try a little hack for postgres
			row := tx.QueryRow(`SELECT currval('objects_object_id_seq')`)

			if err = row.Scan(&tid); err != nil {
				return
			}
		}
		id = int(tid)

		//update the object's id
		rv.FieldByName("ID").SetInt(tid)
		newObject = true
	}

	if id == 0 {
		return fmt.Errorf("Unable to get the ID of the last inserted object. Aborted.")
	}

	//if we have a new object just insert all the fields (fast path)
	if newObject {
		//time to insert
		for i := 0; i < rv.NumField(); i++ {
			fld, name := rv.Field(i), tv.Field(i).Name
			if name == "ID" {
				continue
			}

			_, err = tx.Exec(`INSERT INTO attributes
				(object_id, attribute_key, attribute_value)
				VALUES ($1, $2, $3)`, id, name, fld.Interface())
			if err != nil {
				return
			}
		}

		return
	}

	//we now have an object in the database and an id, so lets grab any attributes
	rows, err := tx.Query(`SELECT attribute_key, attribute_value
		FROM attributes WHERE
		    object_id = $1
		AND attribute_archived = false
		AND attribute_preview = $2`, id, w.preview)
	if err != nil {
		return
	}
	defer rows.Close()

	var (
		attrs = map[string][]byte{}
		key   string
		value []byte
	)

	for rows.Next() {
		if err = rows.Scan(&key, &value); err != nil {
			return
		}
		vcopy := make([]byte, len(value))
		copy(vcopy, value)
		attrs[key] = vcopy
	}

	if err = rows.Err(); err != nil {
		return
	}

	//now lets look at our object and update the attributes in the database
	archive := []string{}
	for i := 0; i < rv.NumField(); i++ {
		fld, name := rv.Field(i), tv.Field(i).Name
		if name == "ID" {
			continue
		}

		if dbval, ex := attrs[name]; ex && differs(fld, dbval) {
			archive = append(archive, name)
		}
		delete(attrs, name)
	}
	for name := range attrs {
		archive = append(archive, name)
	}

	log.Println("Archiving")

	//archive the old attributes
	_, err = tx.Exec(fmt.Sprintf(`UPDATE attributes
		SET attribute_archived = true WHERE
		attribute_key IN ('%s')`, strings.Join(archive, `','`)))
	if err != nil {
		return
	}

	//time to insert
	for _, name := range archive {
		fld := rv.FieldByName(name)

		_, err = tx.Exec(`INSERT INTO attributes
			(object_id, attribute_key, attribute_value)
			VALUES ($1, $2, $3)`, id, name, fld.Interface())
		if err != nil {
			return
		}
	}

	return
}

func (w *Wrapper) Links(base interface{}, links interface{}) (err error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	plv := reflect.ValueOf(links)
	if plv.Kind() != reflect.Ptr {
		return fmt.Errorf("Second param must be a pointer to a slice of the desired type")
	}
	rlv := reflect.Indirect(plv)
	if rlv.Kind() != reflect.Slice {
		return fmt.Errorf("Second param must be a pointer to a slice of the desired type")
	}

	linktype := rlv.Type().Elem()
	//make sure linktype is a valid type
	if linktype.Kind() != reflect.Struct {
		return fmt.Errorf("%T is not a slice of struct types", links)
	}
	//make sure we can set the id on it
	if _, ex := linktype.FieldByName("ID"); !ex {
		return fmt.Errorf("%T must be a pointer to a slice of structs with an `ID int` field", links)
	}

	id, err := getId(reflect.ValueOf(base))
	if err != nil {
		return err
	}

	rows, err := w.query(`SELECT objects.object_id, attribute_key, attribute_value
		FROM object_links
			LEFT JOIN objects ON(object_links.target_id = objects.object_id)
			LEFT JOIN attributes ON(objects.object_id = attributes.object_id)
		WHERE object_links.origin_id = $1
			AND object_type = $2
			AND object_deleted = false
			AND attribute_archived = false
		ORDER BY objects.object_id`, id, linktype.Name())
	if err != nil {
		return err
	}
	defer rows.Close()

	//reset the slice
	rlv.Set(reflect.Zero(rlv.Type()))

	var (
		key            string
		value          []byte
		linkId, prevId int
		linkObj        reflect.Value
	)

	for rows.Next() {
		prevId = linkId
		if err = rows.Scan(&linkId, &key, &value); err != nil {
			return
		}

		//check for new object boundary
		if linkId != prevId {
			if linkObj.IsValid() {
				rlv.Set(reflect.Append(rlv, linkObj))
			}
			linkObj = reflect.New(linktype).Elem()
			linkObj.FieldByName("ID").SetInt(int64(linkId))
		}

		if fld := linkObj.FieldByName(key); fld.IsValid() {
			if err = convertAssign(fld.Addr().Interface(), value); err != nil {
				return
			}
		}
	}

	if linkObj.IsValid() {
		rlv.Set(reflect.Append(rlv, linkObj))
	}

	err = rows.Err()

	return
}

func (w *Wrapper) Link(first, second interface{}) (err error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	fid, err := getId(reflect.ValueOf(first))
	if err != nil {
		return
	}

	sid, err := getId(reflect.ValueOf(second))
	if err != nil {
		return
	}

	row := w.queryRow(`SELECT NULL FROM object_links WHERE
	    origin_id = $1
	AND target_id = $2`, fid, sid)

	err = row.Scan(&null)

	switch err {
	case sql.ErrNoRows:
	default: //nil or otherwise
		return
	}

	_, err = w.exec(`INSERT INTO object_links (origin_id, target_id)
		VALUES ($1, $2), ($3, $4)`, fid, sid, sid, fid)

	return
}

func (w *Wrapper) Unlink(first, second interface{}) (err error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	fid, err := getId(reflect.ValueOf(first))
	if err != nil {
		return
	}

	sid, err := getId(reflect.ValueOf(second))
	if err != nil {
		return
	}

	_, err = w.exec(`DELETE FROM object_links WHERE
		   (origin_id = $1 AND target_id = $2)
		OR (origin_id = $3 AND target_id = $4)`, fid, sid, sid, fid)

	return
}

func differs(data reflect.Value, dbval []byte) bool {
	objval := []byte(fmt.Sprintf("%v", data.Interface()))
	if len(objval) != len(dbval) {
		return true
	}
	for i := 0; i < len(objval); i++ {
		if objval[i] != dbval[i] {
			return true
		}
	}
	return false
}

//Bootstrap sets the Wrappers underyling connection and checks it for the
//existance of tables, creating them if necessary. Subsequent calls with the
//same connection pointer do not recheck for tables.
func (w *Wrapper) Bootstrap(conn *sql.DB) (err error) {
	w.connLock.Lock()
	defer w.connLock.Unlock()

	//if we have a new connection recheck the tables
	if conn != w.connection {
		w.doer = sync.Once{}
	}

	w.doer.Do(func() {
		w.connection = conn
		for _, table := range tables {
			if err = w.check(table); err != nil {
				return
			}
		}
	})
	return
}

//helper for Bootstrap. Assumes the lock has already been taken
func (w *Wrapper) check(spec *tableSpec) (err error) {
	res, err := w.connection.Query(fmt.Sprintf("SELECT null FROM %s", spec.Name))
	if res != nil {
		defer res.Close()
	}
	if err == nil {
		return
	}
	_, err = w.connection.Exec(spec.SQL)
	return
}
