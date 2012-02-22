package fdb

import "database/sql"

//DefaultWrapper is used by the package level functions. Must call Bootstrap before
//it can be used.
var DefaultWrapper = new(Wrapper)

//SetPreview calls SetPreview on the default Wrapper
func SetPreview(preview bool) { DefaultWrapper.SetPreview(preview) }

//Load calls Load on the default Wrapper
func Load(obj interface{}, id int) error { return DefaultWrapper.Load(obj, id) }

//Update calls Update on the default Wrapper
func Update(obj interface{}) error { return DefaultWrapper.Update(obj) }

//Links calls Links on the default Wrapper
func Links(base interface{}, links interface{}) error { return DefaultWrapper.Links(base, links) }

//Link calls Link on the default Wrapper
func Link(first, second interface{}) error { return DefaultWrapper.Link(first, second) }

//Unlink calls Unlink on the default Wrapper
func Unlink(first, second interface{}) error { return DefaultWrapper.Unlink(first, second) }

//Bootstrap calls Bootstrap on the default Wrapper. Must be called before any other function, and only can be run once.
//To change the underlying connection for the default Wrapper, call Reset.
func Bootstrap(conn *sql.DB) error { return DefaultWrapper.Bootstrap(conn) }

//Reset resets the default Wrapper to an uninitialized state. Must call Bootstrap again.
func Reset() { DefaultWrapper = new(Wrapper) }
