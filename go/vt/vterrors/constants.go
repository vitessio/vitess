/*
Copyright 2021 The Vitess Authors.

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

package vterrors

import "regexp"

// Operation not allowed error
const (
	NotServing   = "operation not allowed in state NOT_SERVING"
	ShuttingDown = "operation not allowed in state SHUTTING_DOWN"
)

// RxOp regex for operation not allowed error
var RxOp = regexp.MustCompile("operation not allowed in state (NOT_SERVING|SHUTTING_DOWN)")

// TxEngineClosed for transaction engine closed error
const TxEngineClosed = "tx engine can't accept new connections in state %v"

// WrongTablet for invalid tablet type error
const WrongTablet = "wrong tablet type"

// RxWrongTablet regex for invalid tablet type error
var RxWrongTablet = regexp.MustCompile("(wrong|invalid) tablet type")

// Constants for error messages
const (
	// PrimaryVindexNotSet is the error message to be used when there is no primary vindex found on a table
	PrimaryVindexNotSet = "table '%s' does not have a primary vindex"
)

// TxKillerRollback purpose when acquire lock on connection for rolling back transaction.
const TxKillerRollback = "in use: for tx killer rollback"

// TxClosed regex for connection closed
var TxClosed = regexp.MustCompile("transaction ([a-z0-9:]+) (?:ended|not found|in use: for tx killer rollback)")
