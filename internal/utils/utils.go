package utils

import (
	"reflect"
	"strings"
)

// GetStructFieldNames returns a sclice of strings containing the struct's specified tag name (i.e. json or sql)
func GetStructFieldNames(s interface{}, tagName string) []string {
	t := reflect.TypeOf(s)

	// If s is a pointer, get the type it points to
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Ensure we're dealing with a struct
	if t.Kind() != reflect.Struct {
		return nil
	}

	fieldNames := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Check for the specified tag (e.g., "json" or "sql")
		tag := field.Tag.Get(tagName)
		if tag != "" {
			// Split the tag value and use the first part (before any comma)
			tagParts := strings.SplitN(tag, ",", 2)
			if tagParts[0] != "-" {
				fieldNames[i] = tagParts[0]
				continue
			}
		}

		// If no valid tag is found, use the struct field name
		fieldNames[i] = field.Name
	}

	return fieldNames
}

// GetStructFields returns a slice of interface{} containing the field values
func GetStructFields(s interface{}) []interface{} {
	val := reflect.ValueOf(s).Elem()
	numFields := val.NumField()
	row := make([]interface{}, numFields)

	for i := 0; i < numFields; i++ {
		row[i] = val.Field(i).Interface()
	}
	return row
}
