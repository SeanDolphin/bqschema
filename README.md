# BQSchema [![wercker status](https://app.wercker.com/status/c3ce047415c3b4ba6ac9bc5ad26d1747/s "wercker status")](https://app.wercker.com/project/bykey/c3ce047415c3b4ba6ac9bc5ad26d1747)

BQSchema is a package used to created BigQuery schema directly from Go structs and import BigQuery QueryResponse into arrays of Go structs.

## Usage

~~~ go
// main.go
package main

import (
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"github.com/SeanDolphin/bqschema"
)

type person struct{
	Name  string
	Email string
	Age   int
}

func main() {
  	// authorize the bigquery service
  	// create a query
	result, err := bq.Jobs.Query("projectID", query).Do()
	if err == nil {
		var people []person
		err := bqschema.ToStructs(result, &people)
		// do something with people
	}
}

~~~