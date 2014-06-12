package bqschema

import (
	"code.google.com/p/google-api-go-client/bigquery/v2"

	"reflect"
	"strings"
)

func ToStructs(result bigquery.QueryResponse, dst interface{}) error {
	var err error
	value := reflect.Indirect(reflect.ValueOf(dst))

	itemType := value.Type().Elem()
	rowCount := len(result.Rows)

	nameMap := map[string]string{}

	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		nameMap[strings.ToLower(field.Name)] = field.Name
	}

	items := reflect.MakeSlice(value.Type(), rowCount, rowCount)
	for i := 0; i < rowCount; i++ {
		item := reflect.Indirect(reflect.New(itemType))
		row := result.Rows[i]
		for j, cell := range row.F {
			schemaField := result.Schema.Fields[j]

			if name, ok := nameMap[strings.ToLower(schemaField.Name)]; ok {
				field := item.FieldByName(name)
				if field.IsValid() {
					field.Set(reflect.ValueOf(cell.V))
				}
			}
		}
		items.Index(i).Set(item)
	}
	value.Set(items)
	return err
}
