package fdb

var attributes_sql string = `CREATE TABLE attributes (
	object_id          bigint      NOT NULL,
	attribute_key      varchar(64) NOT NULL,
	attribute_value    text,
	attribute_added    timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP,
	attribute_archived bool        NOT NULL DEFAULT false,
	attribute_preview  bool        NOT NULL DEFAULT false
)`

var object_caches_sql string = `CREATE TABLE object_caches (
	object_id bigint NOT NULL,
	cache     text,

	UNIQUE (object_id)
)`

var object_links_sql string = `CREATE TABLE object_links (
	origin_id bigint    DEFAULT NULL,
	target_id bigint    DEFAULT NULL,
	added     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,

	UNIQUE (origin_id, target_id)
)`

var objects_sql string = `CREATE TABLE objects (
	object_id      serial,
	object_type    varchar(64) NOT NULL,
	object_added   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP,
	object_deleted bool        DEFAULT false,

	PRIMARY KEY (object_id)
)`
