// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

const (
	defaultSourceReaderCount = 10
	// defaultDestinationPackCount is the number of rows which will be aggreated
	// into one transaction. Note that higher values will increase memory
	// consumption in vtworker, vttablet and mysql.
	defaultDestinationPackCount   = 1000
	defaultMinTableSizeForSplit   = 1024 * 1024
	defaultDestinationWriterCount = 20
)
