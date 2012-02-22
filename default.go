package fdb

import "database/sql"

var defaultWrapper = new(Wrapper)

//SetPreview calls SetPreview on the defaultWrapper
func SetPreview(preview bool) { defaultWrapper.SetPreview(preview) }

//Load calls Load on the defaultWrapper
func Load(obj interface{}, id int) error { return defaultWrapper.Load(obj, id) }

//Update calls Update on the defaultWrapper
func Update(obj interface{}) error { return defaultWrapper.Update(obj) }

//Links calls Links on the defaultWrapper
func Links(base interface{}, links interface{}) error { return defaultWrapper.Links(base, links) }

//Link calls Link on the defaultWrapper
func Link(first, second interface{}) error { return defaultWrapper.Link(first, second) }

//Unlink calls Unlink on the defaultWrapper
func Unlink(first, second interface{}) error { return defaultWrapper.Unlink(first, second) }

//Bootstrap calls Bootstrap on the defaultWrapper. Must be called before any other function, and only can be run once.
//To change the underlying connection for the defaultWrapper, call Reset.
func Bootstrap(conn *sql.DB) error { return defaultWrapper.Bootstrap(conn) }

//Reset resets the defaultWrapper to an uninitialized state. Must call Bootstrap again.
func Reset() { defaultWrapper = new(Wrapper) }
