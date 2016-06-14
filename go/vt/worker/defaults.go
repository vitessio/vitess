// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import "github.com/youtube/vitess/go/vt/throttler"

const (
	defaultOnline            = true
	defaultOffline           = true
	defaultSourceReaderCount = 10
	// defaultDestinationPackCount is the number of StreamExecute responses which
	// will be aggreated into one transaction. See the vttablet flag
	// "-queryserver-config-stream-buffer-size" for the size (in bytes) of a
	// StreamExecute response. As of 06/2015, the default for it was 32 kB.
	// Note that higher values for this flag --destination_pack_count will
	// increase memory consumption in vtworker, vttablet and mysql.
	defaultDestinationPackCount    = 10
	defaultMinTableSizeForSplit    = 1024 * 1024
	defaultDestinationWriterCount  = 20
	defaultMinHealthyRdonlyTablets = 2
	defaultMaxTPS                  = throttler.MaxRateModuleDisabled
)
