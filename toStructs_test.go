package bqschema_test

import (
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"github.com/SeanDolphin/bqschema"

	"reflect"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ToStructs", func() {
	Context("when converting result rows to array of structs", func() {
		It("will fill an array of structs of simple types whos names match", func() {
			response := &bigquery.QueryResponse{
				Schema: &bigquery.TableSchema{
					Fields: []*bigquery.TableFieldSchema{
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "A",
							Type: "integer",
						},
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "B",
							Type: "float",
						},
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "C",
							Type: "string",
						},
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "D",
							Type: "boolean",
						},
					},
				},
				Rows: []*bigquery.TableRow{
					&bigquery.TableRow{
						F: []*bigquery.TableCell{
							&bigquery.TableCell{
								V: "1",
							},
							&bigquery.TableCell{
								V: "2.0",
							},
							&bigquery.TableCell{
								V: "some",
							},
							&bigquery.TableCell{
								V: "false",
							},
						},
					},
				},
			}

			type test1 struct {
				A int
				B float64
				C string
				D bool
			}

			expectedResult := []test1{
				test1{
					A: 1,
					B: 2.0,
					C: "some",
					D: false,
				},
			}

			var dst []test1

			err := bqschema.ToStructs(response, &dst)
			Expect(err).To(BeNil())
			Expect(reflect.DeepEqual(expectedResult, dst)).To(BeTrue())
		})

		It("will fill an array of structs of simple types whos names no matter the casing", func() {
			response := &bigquery.QueryResponse{
				Schema: &bigquery.TableSchema{
					Fields: []*bigquery.TableFieldSchema{
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "lower",
							Type: "integer",
						},
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "UPPER",
							Type: "float",
						},
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "Title",
							Type: "string",
						},
						&bigquery.TableFieldSchema{
							Mode: "required",
							Name: "camelCase",
							Type: "boolean",
						},
					},
				},
				Rows: []*bigquery.TableRow{
					&bigquery.TableRow{
						F: []*bigquery.TableCell{
							&bigquery.TableCell{
								V: "1",
							},
							&bigquery.TableCell{
								V: "2.0",
							},
							&bigquery.TableCell{
								V: "some",
							},
							&bigquery.TableCell{
								V: "false",
							},
						},
					},
				},
			}

			type test2 struct {
				Lower     int
				UPPER     float64
				Title     string
				CamelCase bool
			}

			expectedResult := []test2{
				test2{
					Lower:     1,
					UPPER:     2.0,
					Title:     "some",
					CamelCase: false,
				},
			}

			var dst []test2

			err := bqschema.ToStructs(response, &dst)
			Expect(err).To(BeNil())
			Expect(reflect.DeepEqual(expectedResult, dst)).To(BeTrue())
		})

	})
})
