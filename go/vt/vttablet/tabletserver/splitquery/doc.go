// Package splitquery contains the logic needed for implementing the tabletserver's SplitQuery RPC.
//
// It defines the Splitter type that drives the query splitting procedure. It cooperates with the
// SplitParams type and splitAlgorithmInterface interface. See example_test.go for a usage example.
//
// General guidelines for contributing to this package:
// 1) Error messages should not contain the "splitquery:" prefix. It will be added by the calling
// code in 'tabletserver'.
package splitquery
