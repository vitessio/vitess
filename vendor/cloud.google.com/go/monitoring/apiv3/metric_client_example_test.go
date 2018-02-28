// Copyright 2016, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// AUTO-GENERATED CODE. DO NOT EDIT.

package monitoring_test

import (
	"cloud.google.com/go/monitoring/apiv3"
	"golang.org/x/net/context"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

func ExampleNewMetricClient() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleMetricClient_ListMonitoredResourceDescriptors() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListMonitoredResourceDescriptorsRequest{
	// TODO: Fill request struct fields.
	}
	it := c.ListMonitoredResourceDescriptors(ctx, req)
	for {
		resp, err := it.Next()
		if err != nil {
			// TODO: Handle error.
			break
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleMetricClient_GetMonitoredResourceDescriptor() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.GetMonitoredResourceDescriptorRequest{
	// TODO: Fill request struct fields.
	}
	resp, err := c.GetMonitoredResourceDescriptor(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleMetricClient_ListMetricDescriptors() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListMetricDescriptorsRequest{
	// TODO: Fill request struct fields.
	}
	it := c.ListMetricDescriptors(ctx, req)
	for {
		resp, err := it.Next()
		if err != nil {
			// TODO: Handle error.
			break
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleMetricClient_GetMetricDescriptor() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.GetMetricDescriptorRequest{
	// TODO: Fill request struct fields.
	}
	resp, err := c.GetMetricDescriptor(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleMetricClient_CreateMetricDescriptor() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.CreateMetricDescriptorRequest{
	// TODO: Fill request struct fields.
	}
	resp, err := c.CreateMetricDescriptor(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleMetricClient_DeleteMetricDescriptor() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.DeleteMetricDescriptorRequest{
	// TODO: Fill request struct fields.
	}
	err = c.DeleteMetricDescriptor(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleMetricClient_ListTimeSeries() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListTimeSeriesRequest{
	// TODO: Fill request struct fields.
	}
	it := c.ListTimeSeries(ctx, req)
	for {
		resp, err := it.Next()
		if err != nil {
			// TODO: Handle error.
			break
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleMetricClient_CreateTimeSeries() {
	ctx := context.Background()
	c, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.CreateTimeSeriesRequest{
	// TODO: Fill request struct fields.
	}
	err = c.CreateTimeSeries(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}
