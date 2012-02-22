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

//Errors that can be returned
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

//Wrapper wraps a *sql.DB and provides useful methods for loading and unloading
//structured data.
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

//examineDB attempts to prepare a bunch of statements and returns which database
//type the database is.
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

//prepare takes a query and transforms it into a query suitable for the connection's
//database type.
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

//begin returns a transaction that wraps commands in prepare statements
func (w *Wrapper) begin() (tx, error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	stx, err := w.connection.Begin()
	return tx{stx, w}, err
}

//exec wraps the query in a prepare and runs Exec on the connection.
func (w *Wrapper) exec(query string, args ...interface{}) (sql.Result, error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	return w.connection.Exec(w.prepare(query), args...)
}

//query wraps the query in a prepare and runs Query on the connection.
func (w *Wrapper) query(query string, args ...interface{}) (*sql.Rows, error) {
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	return w.connection.Query(w.prepare(query), args...)
}

//queryRow wraps the query in a prepare and runs QueryRow on the connection
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

//getId is a helper method to get the ID field from an object
func getId(val reflect.Value) (id int, err error) {
	fld := val.FieldByName("ID")
	if !fld.IsValid() || fld.Kind() != reflect.Int {
		return 0, fmt.Errorf("%T is missing a field `ID int`", val.Type())
	}
	id = int(fld.Int())
	return
}

//Load takes an object and an id and loads the attributes for that object into
//it, performing conversions as necessary.
func (w *Wrapper) Load(obj interface{}, id int) (err error) {
	//grab our read lock
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	//ensure we have a connection
	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	//make sure we have a pointer to a value
	prv := reflect.ValueOf(obj)
	if prv.Kind() != reflect.Ptr {
		return fmt.Errorf("%T is not a pointer type", obj)
	}
	rv := prv.Elem()

	//make sure our value is a struct
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("%T is not a struct type", obj)
	}

	//make sure we have an id and store the value for the field so we can set
	//it later
	idFld := rv.FieldByName("ID")
	if !idFld.IsValid() || idFld.Kind() != reflect.Int {
		return fmt.Errorf("%T is missing a field `ID int`", rv.Type())
	}

	//get the name of our type
	name := rv.Type().Name()

	//check to see if the object exists
	row := w.queryRow(`SELECT NULL FROM objects WHERE
		    object_id = $1
		AND object_type = $2
		AND object_deleted = false`, id, name)
	err = row.Scan(&null)

	switch err {
	case nil:
	case sql.ErrNoRows:
		return NotFound
	default:
		return
	}

	//we found it so set the id
	idFld.SetInt(int64(id))

	//grab the attributes
	rows, err := w.query(`SELECT attribute_key, attribute_value
		FROM attributes WHERE
		    object_id = $1
		AND attribute_archived = false
		AND attribute_preview = $2`, id, w.preview)
	if err != nil {
		return
	}
	defer rows.Close()

	//loop over them
	var key string
	var value []byte
	for rows.Next() {
		if err = rows.Scan(&key, &value); err != nil {
			return
		}

		//assign the attribute to the field
		if fld := rv.FieldByName(key); fld.IsValid() {
			if err = convertAssign(fld.Addr().Interface(), value); err != nil {
				return
			}
		}
	}

	//return any errors
	err = rows.Err()

	return
}

//Update takes an object and stores it in the database, performing conversions
//as necessary.
func (w *Wrapper) Update(obj interface{}) (err error) {
	//grab a read lock
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	//make sure we have a connection
	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	//if we have a pointer, just grab the actual value
	prv := reflect.ValueOf(obj)
	rv := reflect.Indirect(prv)

	//make sure it wasn't a nil pointer
	if !rv.IsValid() {
		return fmt.Errorf("Invalid object passed in to Update")
	}

	//make sure we were passed a struct
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("%T is not a struct or pointer to struct", obj)
	}

	//get the type
	tv := rv.Type()

	//grab the id of the object
	id, err := getId(rv)
	if err != nil {
		return
	}

	//if we're making a new object, we have to have a pointer
	//so that we can set the value later after the insert.
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

	//record if we made a new object
	var newObject bool

	//insert the object. by the end we should have a nonzero id.
	if id != 0 {
		//lets check if the id exists and has the correct type
		row := tx.QueryRow(`SELECT object_type FROM objects WHERE
			    object_id = $1
			AND object_deleted = false`, id)

		//grab the type
		var resultType string
		if err = row.Scan(&resultType); err != nil {
			return
		}

		//make sure the types match
		switch resultType {
		case tv.Name():
		case "":
			return fmt.Errorf("No object with id %d exists.", id)
		default:
			return fmt.Errorf("Cannot update an object of a different type: %s into %s", tv.Name(), resultType)
		}
		//we now have a valid object
	} else {
		//do an insert and update the id
		var result sql.Result
		result, err = tx.Exec(`INSERT INTO objects (object_type) VALUES ($1)`, tv.Name())
		if err != nil {
			return
		}

		//try to grab the last insert id
		var tid int64
		tid, err = result.LastInsertId()
		if err != nil {
			//lets try a little hack for postgres (not all drivers support LastInsertId)
			row := tx.QueryRow(`SELECT currval('objects_object_id_seq')`)

			if err = row.Scan(&tid); err != nil {
				return
			}
		}

		//set our id
		id = int(tid)

		//update the object's id
		rv.FieldByName("ID").SetInt(tid)

		//record that we had a new object
		newObject = true
	}

	//if we don't have an id by now, theres a problem
	if id == 0 {
		return fmt.Errorf("Unable to get the ID of the last inserted object. Aborted.")
	}

	//if we have a new object just insert all the fields (fast path)
	if newObject {

		//loop over the fields
		for i := 0; i < rv.NumField(); i++ {
			fld, name := rv.Field(i), tv.Field(i).Name
			if name == "ID" {
				continue
			}

			//check to see if the field is a Serializer
			//little wacky but you should be able to figure it out
			data := fld.Interface()
			if ser, ok := data.(Serializer); ok {
				data = ser.Serialize()
			}

			//insert the data
			_, err = tx.Exec(`INSERT INTO attributes
				(object_id, attribute_key, attribute_value)
				VALUES ($1, $2, $3)`, id, name, data)

			//check the error
			if err != nil {
				return
			}
		}

		//object has been inserted
		return
	}

	//now we have an actual update and the id of the object, so lets grab the
	//attributes
	rows, err := tx.Query(`SELECT attribute_key, attribute_value
		FROM attributes WHERE
		    object_id = $1
		AND attribute_archived = false
		AND attribute_preview = $2`, id, w.preview)
	if err != nil {
		return
	}
	defer rows.Close()

	//loop over the results
	var (
		attrs = map[string][]byte{}
		key   string
		value []byte
	)
	for rows.Next() {
		//scan in the attribute
		if err = rows.Scan(&key, &value); err != nil {
			return
		}

		//store a copy of the data
		vcopy := make([]byte, len(value))
		copy(vcopy, value)
		attrs[key] = vcopy
	}

	//return any errors
	if err = rows.Err(); err != nil {
		return
	}

	//now lets look at our object and update the attributes in the database
	//a list of things to be archived
	archive := []string{}
	for i := 0; i < rv.NumField(); i++ {
		fld, name := rv.Field(i), tv.Field(i).Name
		if name == "ID" {
			continue
		}

		//if we have a value in the database and it differs
		if dbval, ex := attrs[name]; ex && differs(fld, dbval) {
			//then we need to archive
			archive = append(archive, name)
		}

		//remove the value from the database
		//this way after the loop we have a set of values that are no longer
		//active for the type, so we can archive them.
		delete(attrs, name)
	}

	//append the values that didn't get looked at from the struct to the list
	//to be archived
	for name := range attrs {
		archive = append(archive, name)
	}

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

		//check to see if the field is a Serializer
		//little wacky but you should be able to figure it out
		data := fld.Interface()
		if ser, ok := data.(Serializer); ok {
			data = ser.Serialize()
		}

		//insert it in
		_, err = tx.Exec(`INSERT INTO attributes
			(object_id, attribute_key, attribute_value)
			VALUES ($1, $2, $3)`, id, name, data)

		//return any errors
		if err != nil {
			return
		}
	}

	//we're done!
	return
}

//Links takes an object and a pointer to a slice of objects, and sets the slice
//pointer to a new slice containing the populated objects of the type of an
//element of the slice that are linked to the base object.
func (w *Wrapper) Links(base interface{}, links interface{}) (err error) {
	//grab the read lock
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	//make sure we have a connection
	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	//make sure the second value is a pointer
	plv := reflect.ValueOf(links)
	if plv.Kind() != reflect.Ptr {
		return fmt.Errorf("Second param must be a pointer to a slice of the desired type")
	}
	//make sure it points to a slice
	rlv := reflect.Indirect(plv)
	if rlv.Kind() != reflect.Slice {
		return fmt.Errorf("Second param must be a pointer to a slice of the desired type")
	}

	//get the type of the object in the slice
	linktype := rlv.Type().Elem()

	//make sure linktype is a struct
	if linktype.Kind() != reflect.Struct {
		return fmt.Errorf("%T is not a slice of struct types", links)
	}

	//make sure we can set the id on it
	if _, ex := linktype.FieldByName("ID"); !ex {
		return fmt.Errorf("%T must be a pointer to a slice of structs with an `ID int` field", links)
	}

	//grab the id of the base object
	id, err := getId(reflect.ValueOf(base))
	if err != nil {
		return err
	}

	//select all of the attributes of all of the objects linked to the base object
	//of the correct type.
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

	//loop over the results
	var (
		key            string
		value          []byte
		linkId, prevId int
		linkObj        reflect.Value
	)
	for rows.Next() {
		//if the id changes, we have a new object so record the previous id
		prevId = linkId
		if err = rows.Scan(&linkId, &key, &value); err != nil {
			return
		}

		//check for new object boundary
		if linkId != prevId {
			//if we have a valid object, append it
			if linkObj.IsValid() {
				rlv.Set(reflect.Append(rlv, linkObj))
			}

			//create a new object of the type and set the ID
			linkObj = reflect.New(linktype).Elem()
			linkObj.FieldByName("ID").SetInt(int64(linkId))
		}

		//assign the attribute to the correct field
		if fld := linkObj.FieldByName(key); fld.IsValid() {
			if err = convertAssign(fld.Addr().Interface(), value); err != nil {
				return
			}
		}
	}

	//if we end the loop with a valid object, append it
	if linkObj.IsValid() {
		rlv.Set(reflect.Append(rlv, linkObj))
	}

	//return any errors
	err = rows.Err()

	return
}

//Link attaches two objects for the Links function.
func (w *Wrapper) Link(first, second interface{}) (err error) {
	//grab the read lock
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	//make sure we have a connection
	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	//grab the id of the first object
	fid, err := getId(reflect.ValueOf(first))
	if err != nil {
		return
	}

	//grab the id of the second object
	sid, err := getId(reflect.ValueOf(second))
	if err != nil {
		return
	}

	//check to see if the link already exists
	row := w.queryRow(`SELECT NULL FROM object_links WHERE
	    origin_id = $1
	AND target_id = $2`, fid, sid)

	err = row.Scan(&null)

	switch err {
	case sql.ErrNoRows: //we need to do an inser
	default: //nil or otherwise
		return
	}

	//insert the new link
	_, err = w.exec(`INSERT INTO object_links (origin_id, target_id)
		VALUES ($1, $2), ($3, $4)`, fid, sid, sid, fid)

	//done
	return
}

//Unlink removes an attachment for the Links function.
func (w *Wrapper) Unlink(first, second interface{}) (err error) {
	//grab the read lock
	w.connLock.RLock()
	defer w.connLock.RUnlock()

	//make sure we have a connection
	if w.connection == nil {
		return fmt.Errorf("No connection. Bootstrap a connection first.")
	}

	//grab the id of the first object
	fid, err := getId(reflect.ValueOf(first))
	if err != nil {
		return
	}

	//grab the id of the second object
	sid, err := getId(reflect.ValueOf(second))
	if err != nil {
		return
	}

	//delete the pair
	_, err = w.exec(`DELETE FROM object_links WHERE
		   (origin_id = $1 AND target_id = $2)
		OR (origin_id = $3 AND target_id = $4)`, fid, sid, sid, fid)

	return
}

//differs takes a reflect value and a []byte and determines if they represent
//the same data.
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
