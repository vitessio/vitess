// Package splitquery contains the logic needed for implementing the tabletserver's SplitQuery RPC.
//
// It defines the Splitter type that drives the query splitting procedure. It cooperates with the
// SplitParams type and splitAlgorithmInterface interface.
//
// Usage Example:
//
// 1. Create a SplitParams object.
//
//   splitParams, err := NewSplitParams("SELECT * FROM table WHERE id > :id", // SQL query
//                                      map[string]interface{}{id: int64(5)}  // Bind Variables
//                                      ["id", "user_id"],                    // SplitColumns
//                                      schema)                               // See below.
//   // Schema should be a map[string]schema.Table that maps a table name to its schema.Table
//   // object. It's mostly used for error-checking the split columns.
//   if (err != nil) {
//     panic("SplitParams creation failed with: " + err)
//   }
//
// 2. Create the SplitAlgorithmInterface object used for splitting.
// SplitQuery supports multiple algorithms for splitting the query. These are encapsulated as
// types implementing the SplitAlgorithmInterface. Currently two algorithms are supported
// represented by the FullScanAlgorithm and EqualSplitsAlgorithm types. See the documentation
// of these types for more details on each algorithm.
// To do the split we'll need to create an object of one of these types and pass it to the
// Splitter (see below). Here we use the FullScan algorithm.
// We also pass a type implementing the MySQLExecuter interface that the algorithm will
// use to send statements to MySQL.
//
//   algorithm, err := NewFullScanAlgorithm(splitParams, mySqlExecuter)
//   if (err != nil) {
//     panic("FullScnaAlgorithm creation failed with: " + err)
//   }
//
// 3. Create a splitter object. Always succeeds.
//
//   splitter := NewSplitter(splitParams, algorithm)
//
// 4. Call splitter.Split() to Split the query.
// The result is a slice of querytypes.QuerySplit objects.
//
//   var queryParts []querytypes.QuerySplit
//   queryParts, err = splitter.Split()
//   if (err != nil) {
//     panic("splitter.Split() failed with: " + err);
//   }
package splitquery
