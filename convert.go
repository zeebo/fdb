package fdb

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

//Serializer is a type that knows how to serialize and unserialize itself to a
//table column. This way arbitrary types can be stored as a field in the database.
type Serializer interface {
	Serialize() []byte
	Unserialize([]byte) error
}

// convertAssign copies to dest the value in src, converting it if possible.
func convertAssign(dest interface{}, src []byte) error {
	// Common cases
	switch d := dest.(type) {
	case *string:
		*d = string(src)
		return nil
	case *interface{}:
		bcopy := make([]byte, len(src))
		copy(bcopy, src)
		*d = bcopy
		return nil
	case *[]byte:
		*d = src
		return nil
	case *bool:
		bv, err := driver.Bool.ConvertValue(src)
		if err == nil {
			*d = bv.(bool)
		}
		return err
	case *time.Time:
		t, err := time.Parse("2006-01-02 15:04:05.000000 -0700 MST", string(src))
		if err == nil {
			*d = t
		}
		return err
	case Serializer:
		return d.Unserialize(src)
	}

	dpv := reflect.ValueOf(dest)
	if dpv.Kind() != reflect.Ptr {
		return errors.New("destination not a pointer")
	}
	dv := reflect.Indirect(dpv)

	switch dv.Kind() {
	case reflect.Ptr:
		if src == nil {
			dv.Set(reflect.Zero(dv.Type()))
			return nil
		} else {
			dv.Set(reflect.New(dv.Type().Elem()))
			return convertAssign(dv.Interface(), src)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := string(src)
		i64, err := strconv.ParseInt(s, 10, dv.Type().Bits())
		if err != nil {
			return fmt.Errorf("converting string %q to a %s: %v", s, dv.Kind(), err)
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := string(src)
		u64, err := strconv.ParseUint(s, 10, dv.Type().Bits())
		if err != nil {
			return fmt.Errorf("converting string %q to a %s: %v", s, dv.Kind(), err)
		}
		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		s := string(src)
		f64, err := strconv.ParseFloat(s, dv.Type().Bits())
		if err != nil {
			return fmt.Errorf("converting string %q to a %s: %v", s, dv.Kind(), err)
		}
		dv.SetFloat(f64)
		return nil
	}

	return fmt.Errorf("unsupported driver -> Scan pair: %T -> %T", src, dest)
}
