package fdb

import "database/sql"

var defaultWrapper = new(Wrapper)

func SetPreview(preview bool)                         { defaultWrapper.SetPreview(preview) }
func Load(obj interface{}, id int) error              { return defaultWrapper.Load(obj, id) }
func Update(obj interface{}) error                    { return defaultWrapper.Update(obj) }
func Links(base interface{}, links interface{}) error { return defaultWrapper.Links(base, links) }
func Bootstrap(conn *sql.DB) error                    { return defaultWrapper.Bootstrap(conn) }
