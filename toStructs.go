package bqschema

import (
	"reflect"
	"strconv"
	"strings"

	"google.golang.org/api/bigquery/v2"
)

func ToStructs(result *bigquery.QueryResponse, dst interface{}) error {
	var err error
	value := reflect.Indirect(reflect.ValueOf(dst))

	itemType := value.Type().Elem()
	rowCount := len(result.Rows)

	nameMap := map[string]string{}

	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		jsonTag := field.Tag.Get("json")
		switch jsonTag {
		case "-":
			continue
		case "":
			nameMap[strings.ToLower(field.Name)] = field.Name
		default:
			jsonName := strings.Split(jsonTag, ",")[0]
			nameMap[jsonName] = field.Name
		}

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
					switch field.Kind() {
					case reflect.Float64, reflect.Float32:
						if cell.V == nil {
							field.SetFloat(0)
							continue
						}
						f, err := strconv.ParseFloat(cell.V.(string), 64)
						if err == nil {
							field.SetFloat(f)
						} else {
							return err
						}
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						if cell.V == nil {
							field.SetInt(0)
							continue
						}
						i, err := strconv.ParseInt(cell.V.(string), 10, 64)
						if err == nil {
							field.SetInt(i)
						} else {
							return err
						}
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						if cell.V == nil {
							field.SetUint(0)
							continue
						}
						i, err := strconv.ParseUint(cell.V.(string), 10, 64)
						if err == nil {
							field.SetUint(i)
						} else {
							return err
						}

					case reflect.Bool:
						if cell.V == nil {
							field.SetBool(false)
							continue
						}
						b, err := strconv.ParseBool(cell.V.(string))
						if err == nil {
							field.SetBool(b)
						} else {
							return err
						}
					case reflect.String:
						if cell.V == nil {
							field.SetString("")
							continue
						}
						field.Set(reflect.ValueOf(cell.V))
					}

				}
			}
		}
		items.Index(i).Set(item)
	}
	value.Set(items)
	return err
}
