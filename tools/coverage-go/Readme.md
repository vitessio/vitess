#### Purpose

- To get the coverage when we run the end to end testcase,we need instrumented binaries.
- To get such binaries, we have to put a test file under go/cmd/<each_binary>, so that if we execute `go test ... -c .. -o ..` command, it will produce such files.
- This directory contains the test files which will copied to go/cmd/<respective_dir> via some script and then it instrumented binaries will be produced.
- The end to end test can be configured to run in coverage mode, which will utilize the binaries to produce coverage report.