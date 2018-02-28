// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// concat_table is an example client of the bigquery client library.
// It concatenates two BigQuery tables and writes the result to another table.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
)

var (
	project = flag.String("project", "", "The ID of a Google Cloud Platform project")
	dataset = flag.String("dataset", "", "The ID of a BigQuery dataset")
	src1    = flag.String("src1", "", "The ID of the first BigQuery table to concatenate")
	src2    = flag.String("src2", "", "The ID of the second BigQuery table to concatenate")
	dest    = flag.String("dest", "", "The ID of the BigQuery table to write the result to")
	pollint = flag.Duration("pollint", 10*time.Second, "Polling interval for checking job status")
)

func main() {
	flag.Parse()

	flagsOk := true
	for _, f := range []string{"project", "dataset", "src1", "src2", "dest"} {
		if flag.Lookup(f).Value.String() == "" {
			fmt.Fprintf(os.Stderr, "Flag --%s is required\n", f)
			flagsOk = false
		}
	}
	if !flagsOk {
		os.Exit(1)
	}
	if *src1 == *src2 || *src1 == *dest || *src2 == *dest {
		log.Fatalf("Different values must be supplied for each of --src1, --src2 and --dest")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project)
	if err != nil {
		log.Fatalf("Creating bigquery client: %v", err)
	}

	s1 := client.Dataset(*dataset).Table(*src1)
	s2 := client.Dataset(*dataset).Table(*src2)
	d := client.Dataset(*dataset).Table(*dest)

	// Concatenate data.
	copier := d.CopierFrom(s1, s2)
	copier.WriteDisposition = bigquery.WriteTruncate
	job, err := copier.Run(ctx)
	if err != nil {
		log.Fatalf("Concatenating: %v", err)
	}

	fmt.Printf("Job for concatenation operation: %+v\n", job)
	fmt.Printf("Waiting for job to complete.\n")

	for range time.Tick(*pollint) {
		status, err := job.Status(ctx)
		if err != nil {
			fmt.Printf("Failure determining status: %v", err)
			break
		}
		if !status.Done() {
			continue
		}
		if err := status.Err(); err == nil {
			fmt.Printf("Success\n")
		} else {
			fmt.Printf("Failure: %+v\n", err)
		}
		break
	}
}
