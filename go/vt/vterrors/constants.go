package vterrors

import "regexp"

// Operation not allowed error
const (
	NotServing   = "operation not allowed in state NOT_SERVING"
	ShuttingDown = "operation not allowed in state SHUTTING_DOWN"
)

// RxOp regex for operation not allowed error
var RxOp = regexp.MustCompile("operation not allowed in state (NOT_SERVING|SHUTTING_DOWN)")

// WrongTablet for invalid tablet type error
const WrongTablet = "wrong tablet type"

// RxWrongTablet regex for invalid tablet type error
var RxWrongTablet = regexp.MustCompile("(wrong|invalid) tablet type")

// Constants for error messages
const (
	// PrimaryVindexNotSet is the error message to be used when there is no primary vindex found on a table
	PrimaryVindexNotSet = "table '%s' does not have a primary vindex"
)
