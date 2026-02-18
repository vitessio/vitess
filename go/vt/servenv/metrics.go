/*
Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

func getCpuUsage() float64 {
	if value, err := getCgroupCpu(); err == nil {
		return value
	}
	if value, err := getHostCpuUsage(); err == nil {
		return value
	}
	return -1
}

func getMemoryUsage() float64 {
	if value, err := getCgroupMemory(); err == nil {
		return value
	}
	if value, err := getHostMemoryUsage(); err == nil {
		return value
	}
	return -1
}
