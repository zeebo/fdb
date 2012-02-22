package fdb

import "database/sql"

type tx struct {
	tx *sql.Tx
	w  *Wrapper
}

func (tx *tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.tx.Exec(tx.w.prepare(query), args...)
}
func (tx *tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.tx.Query(tx.w.prepare(query), args...)
}
func (tx *tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return tx.tx.QueryRow(tx.w.prepare(query), args...)
}
func (tx *tx) Commit() error   { return tx.tx.Commit() }
func (tx *tx) Rollback() error { return tx.tx.Rollback() }
